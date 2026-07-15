mod config;
mod server;
mod tls;

use clap::Parser;
use config::{Config, DeviceGroup, DeviceMatch, SwitchKey};
use rkvm_input::interceptor::{DeviceCapabilities, DeviceOrigin};
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

    if let Err(err) = validate_device_policy(&config) {
        tracing::error!("Error parsing config: {}", err);
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
        match print_devices(
            config.device_whitelist.as_deref(),
            config.device_groups.as_deref(),
        )
        .await
        {
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
    let device_groups = config.device_groups;

    tokio::select! {
        result = server::run(
            config.listen,
            acceptor,
            &config.password,
            &switch_bindings,
            propagate_switch_keys,
            device_whitelist,
            device_groups,
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

fn validate_device_policy(config: &Config) -> Result<(), String> {
    if config.device_whitelist.is_some() && config.device_groups.is_some() {
        return Err("device-whitelist and device-groups are mutually exclusive".into());
    }
    if config
        .device_whitelist
        .as_ref()
        .map_or(false, |items| items.iter().any(DeviceMatch::is_empty))
    {
        return Err("device-whitelist entries must contain at least one match field".into());
    }

    if let Some(groups) = &config.device_groups {
        if groups.is_empty() {
            return Err("device-groups must contain at least one group".into());
        }
        let mut names = HashSet::new();
        for group in groups {
            if group.name.trim().is_empty() {
                return Err("device-group names must not be empty".into());
            }
            if !names.insert(&group.name) {
                return Err(format!("duplicate device-group name {:?}", group.name));
            }
            if group.candidates.is_empty() {
                return Err(format!(
                    "device-group {:?} must contain at least one candidate",
                    group.name
                ));
            }
            if group
                .candidates
                .iter()
                .any(|candidate| candidate.matcher.is_empty())
            {
                return Err(format!(
                    "device-group {:?} candidates must contain at least one match field",
                    group.name
                ));
            }
        }
    }

    Ok(())
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

async fn print_devices(
    device_whitelist: Option<&[DeviceMatch]>,
    device_groups: Option<&[DeviceGroup]>,
) -> Result<(), std::io::Error> {
    for device in list_devices().await? {
        let info = device.info();
        let whitelist = match device_whitelist {
            Some(items) if items.iter().any(|item| item.matches(info)) => "yes",
            Some(_) => "no",
            None => "disabled",
        };
        let group_match = device_groups.and_then(|groups| {
            groups.iter().enumerate().find_map(|(group_index, group)| {
                group
                    .candidates
                    .iter()
                    .position(|candidate| candidate.matcher.matches(info))
                    .map(|candidate_index| (group_index, group, candidate_index))
            })
        });

        println!("path = {}", info.path().display());
        println!("sysfs-path = {}", info.sysfs_path().display());
        println!("origin = {}", origin_name(info.origin()));
        println!("name = {:?}", info.name());
        print!("bustype = 0x{:04x}", info.bustype());
        if let Some(name) = bustype_name(info.bustype()) {
            print!(" # {name}");
        }
        println!();
        println!("vendor = 0x{:04x}", info.vendor());
        println!("product = 0x{:04x}", info.product());
        println!("version = 0x{:04x}", info.version());
        println!("capabilities = {}", capability_summary(info.capabilities()));
        println!("whitelisted = {}", whitelist);
        match (device_groups, group_match) {
            (Some(_), Some((group_index, group, candidate_index))) => {
                let candidate = &group.candidates[candidate_index];
                println!("matching-group = {}", toml_string(&group.name));
                println!("matching-group-index = {}", group_index + 1);
                println!("matching-candidate-index = {}", candidate_index + 1);
                println!(
                    "configured-grab-delay-ms = {}",
                    candidate.grab_delay_ms.unwrap_or_default()
                );
            }
            (Some(_), None) => println!("matching-group = none"),
            (None, _) => println!("matching-group = disabled"),
        }

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

fn origin_name(origin: DeviceOrigin) -> &'static str {
    match origin {
        DeviceOrigin::Physical => "physical",
        DeviceOrigin::Virtual => "virtual",
    }
}

fn bustype_name(bustype: u16) -> Option<&'static str> {
    Some(match bustype {
        0x0001 => "BUS_PCI",
        0x0002 => "BUS_ISAPNP",
        0x0003 => "BUS_USB",
        0x0004 => "BUS_HIL",
        0x0005 => "BUS_BLUETOOTH",
        0x0006 => "BUS_VIRTUAL",
        0x0010 => "BUS_ISA",
        0x0011 => "BUS_I8042",
        0x0012 => "BUS_XTKBD",
        0x0013 => "BUS_RS232",
        0x0014 => "BUS_GAMEPORT",
        0x0015 => "BUS_PARPORT",
        0x0016 => "BUS_AMIGA",
        0x0017 => "BUS_ADB",
        0x0018 => "BUS_I2C",
        0x0019 => "BUS_HOST",
        0x001a => "BUS_GSC",
        0x001b => "BUS_ATARI",
        0x001c => "BUS_SPI",
        0x001d => "BUS_RMI",
        0x001e => "BUS_CEC",
        0x001f => "BUS_INTEL_ISHTP",
        0x0020 => "BUS_AMD_SFH",
        _ => return None,
    })
}

fn capability_summary(capabilities: DeviceCapabilities) -> String {
    let mut names = Vec::new();
    if capabilities.key {
        names.push("key");
    }
    if capabilities.relative {
        names.push("relative");
    }
    if capabilities.absolute {
        names.push("absolute");
    }

    format!("[{}]", names.join(", "))
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
    fn input_bus_names_match_linux_ids() {
        assert_eq!(bustype_name(0x0003), Some("BUS_USB"));
        assert_eq!(bustype_name(0x0006), Some("BUS_VIRTUAL"));
        assert_eq!(bustype_name(0x0018), Some("BUS_I2C"));
        assert_eq!(bustype_name(0x0019), Some("BUS_HOST"));
        assert_eq!(bustype_name(0xffff), None);
    }

    #[test]
    fn device_policy_rejects_legacy_and_group_configuration_together() {
        let config = config(
            r#"
listen = "127.0.0.1:5258"
switch-keys = ["left-ctrl"]
certificate = "/etc/rkvm/certificate.pem"
key = "/etc/rkvm/key.pem"
password = "123456789"
device-whitelist = [{ name = "keyboard" }]

[[device-groups]]
name = "keyboard"
[[device-groups.candidates]]
name = "keyboard"
"#,
        );

        assert_eq!(
            validate_device_policy(&config).unwrap_err(),
            "device-whitelist and device-groups are mutually exclusive"
        );
    }

    #[test]
    fn device_policy_rejects_duplicate_and_empty_groups() {
        let duplicate = config(
            r#"
listen = "127.0.0.1:5258"
switch-keys = ["left-ctrl"]
certificate = "/etc/rkvm/certificate.pem"
key = "/etc/rkvm/key.pem"
password = "123456789"

[[device-groups]]
name = "mouse"
[[device-groups.candidates]]
name = "mouse"

[[device-groups]]
name = "mouse"
[[device-groups.candidates]]
name = "other mouse"
"#,
        );
        assert_eq!(
            validate_device_policy(&duplicate).unwrap_err(),
            "duplicate device-group name \"mouse\""
        );

        let empty = config(
            r#"
listen = "127.0.0.1:5258"
switch-keys = ["left-ctrl"]
certificate = "/etc/rkvm/certificate.pem"
key = "/etc/rkvm/key.pem"
password = "123456789"

[[device-groups]]
name = "keyboard"
"#,
        );
        assert_eq!(
            validate_device_policy(&empty).unwrap_err(),
            "device-group \"keyboard\" must contain at least one candidate"
        );
    }

    #[test]
    fn device_policy_rejects_empty_candidate_match() {
        let config = config(
            r#"
listen = "127.0.0.1:5258"
switch-keys = ["left-ctrl"]
certificate = "/etc/rkvm/certificate.pem"
key = "/etc/rkvm/key.pem"
password = "123456789"

[[device-groups]]
name = "keyboard"
[[device-groups.candidates]]
grab-delay-ms = 1000
"#,
        );
        assert_eq!(
            validate_device_policy(&config).unwrap_err(),
            "device-group \"keyboard\" candidates must contain at least one match field"
        );
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
