use crate::interceptor::{DeviceInfo, Interceptor, OpenError};
use crate::registry::Registry;

use futures::StreamExt;
use inotify::{Inotify, WatchMask};
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc::{self, Receiver, Sender};

const EVENT_PATHS: &[&str] = &["/dev/input", "/dev/input/by-id", "/dev/input/by-path"];

pub struct CandidateDevice {
    info: DeviceInfo,
    aliases: Vec<PathBuf>,
}

impl CandidateDevice {
    pub fn info(&self) -> &DeviceInfo {
        &self.info
    }

    pub fn aliases(&self) -> &[PathBuf] {
        &self.aliases
    }
}

pub struct Monitor {
    receiver: Receiver<Result<Interceptor, Error>>,
}

impl Monitor {
    pub fn new() -> Self {
        Self::with_filter(|_| true)
    }

    pub fn with_filter<F>(device_filter: F) -> Self
    where
        F: Fn(&DeviceInfo) -> bool + Send + Sync + 'static,
    {
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(monitor(sender, Arc::new(device_filter)));

        Self { receiver }
    }

    pub async fn read(&mut self) -> Result<Interceptor, Error> {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "Monitor task exited"))?
    }
}

pub async fn list_devices() -> Result<Vec<CandidateDevice>, Error> {
    let mut devices = Vec::new();
    let mut by_canonical_path = HashMap::new();

    let mut read_dir = fs::read_dir("/dev/input").await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        if !is_event_path(&path).await {
            continue;
        }

        let canonical_path = fs::canonicalize(&path).await?;
        let info = DeviceInfo::open(&path).await?;
        by_canonical_path.insert(canonical_path, devices.len());
        devices.push(CandidateDevice {
            info,
            aliases: Vec::new(),
        });
    }

    for event_path in ["/dev/input/by-id", "/dev/input/by-path"] {
        let mut read_dir = match fs::read_dir(event_path).await {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };

        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if !is_event_path(&path).await {
                continue;
            }

            let canonical_path = fs::canonicalize(&path).await?;
            if let Some(index) = by_canonical_path.get(&canonical_path) {
                devices[*index].aliases.push(path);
            }
        }
    }

    devices.sort_by(|a, b| a.info.path().cmp(b.info.path()));
    for device in &mut devices {
        device.aliases.sort();
    }

    Ok(devices)
}

async fn is_event_path(path: &Path) -> bool {
    let is_event_file = |path: &Path| {
        path.file_name()
            .and_then(OsStr::to_str)
            .map_or(false, |name| name.starts_with("event"))
    };

    is_event_file(path)
        || fs::canonicalize(path)
            .await
            .map(|path| is_event_file(&path))
            .unwrap_or(false)
}

async fn monitor(
    sender: Sender<Result<Interceptor, Error>>,
    device_filter: Arc<dyn Fn(&DeviceInfo) -> bool + Send + Sync>,
) {
    let run = async {
        let registry = Registry::new();
        let inotify = Inotify::init()?;
        let mut watches = HashMap::new();
        let mut pending = VecDeque::new();

        for event_path in EVENT_PATHS {
            let mut read_dir = match fs::read_dir(event_path).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == ErrorKind::NotFound && *event_path != "/dev/input" => {
                    continue
                }
                Err(err) => return Err(err),
            };

            while let Some(entry) = read_dir.next_entry().await? {
                pending.push_back(entry.path());
            }

            let watch = inotify
                .watches()
                .add(event_path, WatchMask::CREATE | WatchMask::MOVED_TO)?;
            watches.insert(watch, PathBuf::from(event_path));
        }

        // This buffer size should be OK, since we don't expect a lot of devices
        // to be plugged in frequently.
        let mut stream = inotify.into_event_stream([0; 512])?;

        loop {
            let path = match pending.pop_front() {
                Some(path) => path,
                None => match stream.next().await {
                    Some(event) => {
                        let event = event?;
                        let name = match event.name {
                            Some(name) => name,
                            None => continue,
                        };
                        let directory = match watches.get(&event.wd) {
                            Some(directory) => directory,
                            None => continue,
                        };

                        directory.join(&name)
                    }
                    None => break,
                },
            };

            if !is_event_path(&path).await {
                tracing::debug!("Skipping non event file {:?}", path);
                continue;
            }

            let interceptor = match Interceptor::open(&path, &registry, &*device_filter).await {
                Ok(interceptor) => interceptor,
                Err(OpenError::Io(err)) => return Err(err),
                Err(OpenError::NotAppliable) => continue,
            };

            if sender.send(Ok(interceptor)).await.is_err() {
                return Ok(());
            }
        }

        Ok(())
    };

    tokio::select! {
        result = run => match result {
            Ok(_) => {},
            Err(err) => {
                let _ = sender.send(Err(err)).await;
            }
        },
        _ = sender.closed() => {}
    }
}
