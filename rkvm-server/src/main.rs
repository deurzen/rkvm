mod config;
mod server;
mod tls;

use clap::Parser;
use config::{Config, DeviceMatch, SwitchKey};
use rkvm_input::monitor::list_devices;
use std::collections::HashSet;
use std::future;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use tokio::{fs, signal, time};
use tracing::subscriber;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[structopt(name = "rkvm-server", about = "The rkvm server application")]
struct Args {
    #[structopt(help = "Path to configuration file")]
    config_path: PathBuf,
    #[structopt(help = "Shutdown after N seconds", long, short)]
    shutdown_after: Option<u64>,
    #[structopt(help = "List input devices and exit", long)]
    list_devices: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().without_time());

    subscriber::set_global_default(registry).unwrap();

    let args = Args::parse();
    let config = match fs::read_to_string(&args.config_path).await {
        Ok(config) => config,
        Err(err) => {
            tracing::error!("Error reading config: {}", err);
            return ExitCode::FAILURE;
        }
    };

    let config = match toml::from_str::<Config>(&config) {
        Ok(config) => config,
        Err(err) => {
            tracing::error!("Error parsing config: {}", err);
            return ExitCode::FAILURE;
        }
    };

    if config
        .device_whitelist
        .as_ref()
        .map_or(false, |items| items.iter().any(|item| item.is_empty()))
    {
        tracing::error!(
            "Error parsing config: device-whitelist entries must contain at least one match field"
        );
        return ExitCode::FAILURE;
    }

    let client_queue_size = config
        .client_queue_size
        .unwrap_or(server::DEFAULT_CLIENT_QUEUE_SIZE);
    if client_queue_size == 0 {
        tracing::error!("Error parsing config: client-queue-size must be greater than zero");
        return ExitCode::FAILURE;
    }
    warn_on_broad_device_whitelist(&config.device_whitelist);

    if args.list_devices {
        match print_devices(config.device_whitelist.as_deref()).await {
            Ok(()) => return ExitCode::SUCCESS,
            Err(err) => {
                tracing::error!("Error listing input devices: {}", err);
                return ExitCode::FAILURE;
            }
        }
    }

    let switch_bindings = match build_switch_bindings(&config) {
        Ok(switch_bindings) => switch_bindings,
        Err(err) => {
            tracing::error!("Error parsing config: {}", err);
            return ExitCode::FAILURE;
        }
    };

    let acceptor = match tls::configure(&config.certificate, &config.key).await {
        Ok(acceptor) => acceptor,
        Err(err) => {
            tracing::error!("Error configuring TLS: {}", err);
            return ExitCode::FAILURE;
        }
    };

    let shutdown = async {
        match args.shutdown_after {
            Some(shutdown_after) => time::sleep(Duration::from_secs(shutdown_after)).await,
            None => future::pending().await,
        }
    };

    let propagate_switch_keys = config.propagate_switch_keys.unwrap_or(true);
    let device_whitelist = config.device_whitelist;

    tokio::select! {
        result = server::run(
            config.listen,
            acceptor,
            &config.password,
            &switch_bindings,
            propagate_switch_keys,
            device_whitelist,
            client_queue_size,
        ) => {
            if let Err(err) = result {
                tracing::error!("Error: {}", err);
                return ExitCode::FAILURE;
            }
        }
        // This is needed to properly clean libevdev stuff up.
        result = signal::ctrl_c() => {
            if let Err(err) = result {
                tracing::error!("Error setting up signal handler: {}", err);
                return ExitCode::FAILURE;
            }

            tracing::info!("Exiting on signal");
        }
        _ = shutdown => {
            tracing::info!("Shutting down as requested");
        }
    }

    ExitCode::SUCCESS
}

fn build_switch_bindings(config: &Config) -> Result<Vec<server::SwitchBinding>, &'static str> {
    let mut bindings = Vec::new();

    if let Some(switch_keys) = &config.switch_keys {
        bindings.push(convert_switch_binding(switch_keys, "switch-keys")?);
    }

    if let Some(switch_bindings) = &config.switch_bindings {
        for binding in switch_bindings {
            bindings.push(convert_switch_binding(binding, "switch-bindings entries")?);
        }
    }

    if bindings.is_empty() {
        return Err("either switch-keys or switch-bindings must be configured");
    }

    Ok(bindings)
}

fn convert_switch_binding(
    binding: &[SwitchKey],
    name: &'static str,
) -> Result<server::SwitchBinding, &'static str> {
    let Some(trigger) = binding.last().copied() else {
        return Err(match name {
            "switch-keys" => "switch-keys must contain at least one key",
            _ => "switch-bindings entries must contain at least one key",
        });
    };

    let mut seen = HashSet::new();
    if !binding.iter().all(|key| seen.insert(*key)) {
        return Err(match name {
            "switch-keys" => "switch-keys must not contain duplicate keys",
            _ => "switch-bindings entries must not contain duplicate keys",
        });
    }

    Ok(server::SwitchBinding::new(
        binding.iter().copied().map(Into::into).collect(),
        trigger.into(),
    ))
}

fn warn_on_broad_device_whitelist(device_whitelist: &Option<Vec<DeviceMatch>>) {
    let Some(device_whitelist) = device_whitelist else {
        return;
    };

    if device_whitelist.iter().any(|item| item.path.is_none()) {
        tracing::warn!(
            "device-whitelist entries without path may match multiple event nodes; prefer /dev/input/by-id/*-event-kbd or /dev/input/by-path/*-event-kbd for keyboard-only forwarding"
        );
    }
}

async fn print_devices(device_whitelist: Option<&[DeviceMatch]>) -> Result<(), std::io::Error> {
    for device in list_devices().await? {
        let info = device.info();
        let whitelist = match device_whitelist {
            Some(items) if items.iter().any(|item| item.matches(info)) => "yes",
            Some(_) => "no",
            None => "disabled",
        };

        println!("path = {}", info.path().display());
        println!("name = {:?}", info.name());
        println!("vendor = 0x{:04x}", info.vendor());
        println!("product = 0x{:04x}", info.product());
        println!("version = 0x{:04x}", info.version());
        println!("whitelisted = {}", whitelist);

        if device.aliases().is_empty() {
            println!("aliases = []");
        } else {
            println!("aliases = [");
            for alias in device.aliases() {
                println!("    {},", toml_string(&alias.display().to_string()));
            }
            println!("]");
        }

        println!();
    }

    Ok(())
}

fn toml_string(value: &str) -> String {
    let mut escaped = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch => escaped.push(ch),
        }
    }
    escaped.push('"');
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(data: &str) -> Config {
        toml::from_str(data).unwrap()
    }

    #[test]
    fn switch_keys_reject_duplicate_keys() {
        let config = config(
            r#"
listen = "127.0.0.1:5258"
switch-keys = ["left-ctrl", "left-ctrl"]
certificate = "/etc/rkvm/certificate.pem"
key = "/etc/rkvm/key.pem"
password = "123456789"
"#,
        );

        assert_eq!(
            build_switch_bindings(&config).unwrap_err(),
            "switch-keys must not contain duplicate keys"
        );
    }

    #[test]
    fn switch_bindings_reject_empty_entries() {
        let config = config(
            r#"
listen = "127.0.0.1:5258"
switch-bindings = [[]]
certificate = "/etc/rkvm/certificate.pem"
key = "/etc/rkvm/key.pem"
password = "123456789"
"#,
        );

        assert_eq!(
            build_switch_bindings(&config).unwrap_err(),
            "switch-bindings entries must contain at least one key"
        );
    }

    #[test]
    fn switch_bindings_reject_duplicate_keys() {
        let config = config(
            r#"
listen = "127.0.0.1:5258"
switch-bindings = [["left-ctrl", "space", "left-ctrl"]]
certificate = "/etc/rkvm/certificate.pem"
key = "/etc/rkvm/key.pem"
password = "123456789"
"#,
        );

        assert_eq!(
            build_switch_bindings(&config).unwrap_err(),
            "switch-bindings entries must not contain duplicate keys"
        );
    }
}
