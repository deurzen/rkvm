use crate::interceptor::{ClaimError, DeviceInfo, Interceptor};
use crate::registry::{Entry, Registry};

use futures::StreamExt;
use inotify::{EventMask, Inotify, WatchMask};
use std::collections::{BTreeSet, HashMap};
use std::ffi::OsStr;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{self, Instant};

const INPUT_PATH: &str = "/dev/input";
const ALIAS_PATHS: &[&str] = &["/dev/input/by-id", "/dev/input/by-path"];
const RETRY_BACKOFF: &[Duration] = &[
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
];
const BLOCKED_RETRY: Duration = Duration::from_secs(5);
const WATCH_MASK: WatchMask = WatchMask::CREATE
    .union(WatchMask::MOVED_TO)
    .union(WatchMask::DELETE)
    .union(WatchMask::MOVED_FROM)
    .union(WatchMask::DELETE_SELF)
    .union(WatchMask::MOVE_SELF);

pub type ActivationId = u64;
type DeviceFilter = dyn Fn(&DeviceInfo) -> bool + Send + Sync;

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

pub struct ActivatedDevice {
    id: ActivationId,
    interceptor: Interceptor,
}

impl ActivatedDevice {
    pub fn id(&self) -> ActivationId {
        self.id
    }

    pub fn into_interceptor(self) -> Interceptor {
        self.interceptor
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseCause {
    Disconnected,
    Failed,
}

pub enum MonitorEvent {
    Activated(ActivatedDevice),
    Remove { activation_id: ActivationId },
}

struct Release {
    activation_id: ActivationId,
    cause: ReleaseCause,
}

pub struct Monitor {
    receiver: Receiver<Result<MonitorEvent, Error>>,
    release_sender: Sender<Release>,
}

impl Monitor {
    pub fn new() -> Self {
        Self::with_filter(|_| true)
    }

    pub fn with_filter<F>(device_filter: F) -> Self
    where
        F: Fn(&DeviceInfo) -> bool + Send + Sync + 'static,
    {
        let (sender, receiver) = mpsc::channel(32);
        let (release_sender, release_receiver) = mpsc::channel(32);
        tokio::spawn(monitor(sender, release_receiver, Arc::new(device_filter)));

        Self {
            receiver,
            release_sender,
        }
    }

    pub async fn read(&mut self) -> Result<MonitorEvent, Error> {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "Monitor task exited"))?
    }

    pub async fn release(
        &self,
        activation_id: ActivationId,
        cause: ReleaseCause,
    ) -> Result<(), Error> {
        self.release_sender
            .send(Release {
                activation_id,
                cause,
            })
            .await
            .map_err(|_| Error::new(ErrorKind::BrokenPipe, "Monitor task exited"))
    }
}

pub async fn list_devices() -> Result<Vec<CandidateDevice>, Error> {
    let registry = Registry::new();
    let snapshots = scan_devices(&registry).await?;
    let mut devices = snapshots
        .into_values()
        .filter_map(|snapshot| match snapshot {
            Snapshot::Candidate { info, aliases } => Some(CandidateDevice {
                info,
                aliases: aliases.into_iter().collect(),
            }),
            Snapshot::Owned { .. } => None,
        })
        .collect::<Vec<_>>();

    devices.sort_by(|a, b| a.info.path().cmp(b.info.path()));
    Ok(devices)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetryBackoff {
    failures: usize,
}

impl RetryBackoff {
    fn new() -> Self {
        Self { failures: 0 }
    }

    fn next(&mut self) -> Duration {
        let delay = RETRY_BACKOFF[self.failures.min(RETRY_BACKOFF.len() - 1)];
        self.failures = self.failures.saturating_add(1);
        delay
    }

    fn reset(&mut self) {
        self.failures = 0;
    }
}

#[derive(Debug)]
enum CandidateState {
    Ready,
    Waiting {
        until: Instant,
    },
    Blocked {
        until: Instant,
    },
    Unsupported,
    Active {
        activation_id: ActivationId,
        removal_sent: bool,
    },
    Rejected,
}

struct Candidate {
    info: DeviceInfo,
    aliases: BTreeSet<PathBuf>,
    state: CandidateState,
    backoff: RetryBackoff,
}

impl Candidate {
    fn new(info: DeviceInfo, aliases: BTreeSet<PathBuf>, device_filter: &DeviceFilter) -> Self {
        let state = if device_filter(&info) {
            tracing::info!(
                path = ?info.path(),
                name = ?info.name(),
                origin = ?info.origin(),
                bustype = format_args!("0x{:04x}", info.bustype()),
                "Discovered eligible input candidate"
            );
            CandidateState::Ready
        } else {
            tracing::info!(
                path = ?info.path(),
                name = ?info.name(),
                vendor = %info.vendor(),
                product = %info.product(),
                version = %info.version(),
                origin = ?info.origin(),
                bustype = format_args!("0x{:04x}", info.bustype()),
                "Rejected input candidate: no matching allow rule"
            );
            CandidateState::Rejected
        };

        Self {
            info,
            aliases,
            state,
            backoff: RetryBackoff::new(),
        }
    }

    fn retry_at(&self) -> Option<Instant> {
        match self.state {
            CandidateState::Waiting { until } | CandidateState::Blocked { until } => Some(until),
            _ => None,
        }
    }
}

enum Snapshot {
    Candidate {
        info: DeviceInfo,
        aliases: BTreeSet<PathBuf>,
    },
    Owned {
        path: PathBuf,
        aliases: BTreeSet<PathBuf>,
    },
}

impl Snapshot {
    fn aliases_mut(&mut self) -> &mut BTreeSet<PathBuf> {
        match self {
            Self::Candidate { aliases, .. } | Self::Owned { aliases, .. } => aliases,
        }
    }

    fn path(&self) -> &Path {
        match self {
            Self::Candidate { info, .. } => info.path(),
            Self::Owned { path, .. } => path,
        }
    }
}

async fn monitor(
    sender: Sender<Result<MonitorEvent, Error>>,
    mut release_receiver: Receiver<Release>,
    device_filter: Arc<DeviceFilter>,
) {
    let run = async {
        let registry = Registry::new();
        let inotify = Inotify::init()?;
        let mut watched_paths = BTreeSet::new();

        // Watch the parent before inspecting alias directories. If by-id/by-path
        // appears concurrently, the parent event causes it to be watched before
        // the next scan.
        inotify.watches().add(INPUT_PATH, WATCH_MASK)?;
        watched_paths.insert(PathBuf::from(INPUT_PATH));
        for alias_path in ALIAS_PATHS {
            add_watch_if_present(&inotify, &mut watched_paths, Path::new(alias_path)).await?;
        }

        let mut stream = inotify.into_event_stream([0; 4096])?;
        let mut candidates = HashMap::<Entry, Candidate>::new();
        let mut next_activation_id = 1;
        let mut inventory_interval = time::interval(Duration::from_secs(2));
        inventory_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        inventory_interval.tick().await;

        refresh_inventory(&registry, &mut candidates, &*device_filter, &sender).await?;
        reconcile(
            &registry,
            &mut candidates,
            &*device_filter,
            &sender,
            &mut next_activation_id,
        )
        .await?;

        loop {
            let deadline = candidates.values().filter_map(Candidate::retry_at).min();
            let retry = async {
                match deadline {
                    Some(deadline) => time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                event = stream.next() => {
                    let Some(event) = event else {
                        return Err(Error::new(ErrorKind::BrokenPipe, "inotify event stream ended"));
                    };
                    let event = event?;

                    if event.mask.contains(EventMask::Q_OVERFLOW) {
                        tracing::warn!("Input inotify queue overflowed; rebuilding candidate inventory");
                    }
                    if event.mask.intersects(EventMask::DELETE | EventMask::MOVED_FROM) {
                        if let Some(name) = event.name.as_deref() {
                            if name == OsStr::new("by-id") || name == OsStr::new("by-path") {
                                watched_paths.remove(&Path::new(INPUT_PATH).join(name));
                            }
                        }
                    }

                    // Adding an existing watch is harmlessly avoided by path. Do
                    // this before scanning so alias creation cannot fall into a
                    // scan/watch gap.
                    for alias_path in ALIAS_PATHS {
                        add_stream_watch_if_present(
                            &mut stream,
                            &mut watched_paths,
                            Path::new(alias_path),
                        )
                        .await?;
                    }

                    refresh_inventory(
                        &registry,
                        &mut candidates,
                        &*device_filter,
                        &sender,
                    )
                    .await?;
                }
                release = release_receiver.recv() => {
                    let Some(release) = release else {
                        return Ok(());
                    };
                    handle_release(&mut candidates, release);
                    refresh_inventory(
                        &registry,
                        &mut candidates,
                        &*device_filter,
                        &sender,
                    )
                    .await?;
                }
                _ = inventory_interval.tick() => {
                    refresh_inventory(
                        &registry,
                        &mut candidates,
                        &*device_filter,
                        &sender,
                    )
                    .await?;
                }
                _ = retry => {
                    for candidate in candidates.values_mut() {
                        let expired = candidate.retry_at().map_or(false, |until| until <= Instant::now());
                        if expired {
                            candidate.state = CandidateState::Ready;
                        }
                    }
                }
                _ = sender.closed() => return Ok(()),
            }

            reconcile(
                &registry,
                &mut candidates,
                &*device_filter,
                &sender,
                &mut next_activation_id,
            )
            .await?;
        }
    };

    if let Err(err) = run.await {
        let _ = sender.send(Err(err)).await;
    }
}

async fn add_watch_if_present(
    inotify: &Inotify,
    watched_paths: &mut BTreeSet<PathBuf>,
    path: &Path,
) -> Result<(), Error> {
    if watched_paths.contains(path) {
        return Ok(());
    }
    match fs::metadata(path).await {
        Ok(_) => {
            inotify.watches().add(path, WATCH_MASK)?;
            watched_paths.insert(path.to_owned());
        }
        Err(err) if is_disappearance(&err) => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

async fn add_stream_watch_if_present(
    stream: &mut inotify::EventStream<[u8; 4096]>,
    watched_paths: &mut BTreeSet<PathBuf>,
    path: &Path,
) -> Result<(), Error> {
    if watched_paths.contains(path) {
        return Ok(());
    }
    match fs::metadata(path).await {
        Ok(_) => {
            stream.watches().add(path, WATCH_MASK)?;
            watched_paths.insert(path.to_owned());
        }
        Err(err) if is_disappearance(&err) => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

async fn refresh_inventory(
    registry: &Registry,
    candidates: &mut HashMap<Entry, Candidate>,
    device_filter: &DeviceFilter,
    sender: &Sender<Result<MonitorEvent, Error>>,
) -> Result<(), Error> {
    let mut snapshots = scan_devices(registry).await?;
    let existing_keys = candidates.keys().copied().collect::<Vec<_>>();

    for key in existing_keys {
        let active = matches!(
            candidates.get(&key).map(|candidate| &candidate.state),
            Some(CandidateState::Active { .. })
        );

        match snapshots.remove(&key) {
            Some(Snapshot::Candidate { info, aliases }) => {
                let candidate = candidates.get_mut(&key).unwrap();
                candidate.info = info;
                candidate.aliases = aliases;
                if !device_filter(&candidate.info) && active {
                    request_removal(candidate, sender).await?;
                } else if !device_filter(&candidate.info) {
                    candidate.state = CandidateState::Rejected;
                }
            }
            Some(Snapshot::Owned { aliases, .. }) if active => {
                candidates.get_mut(&key).unwrap().aliases = aliases;
            }
            Some(Snapshot::Owned { .. }) => {
                candidates.remove(&key);
            }
            None if active => {
                request_removal(candidates.get_mut(&key).unwrap(), sender).await?;
            }
            None => {
                candidates.remove(&key);
            }
        }
    }

    for (key, snapshot) in snapshots {
        if let Snapshot::Candidate { info, aliases } = snapshot {
            candidates.insert(key, Candidate::new(info, aliases, device_filter));
        }
    }

    Ok(())
}

async fn request_removal(
    candidate: &mut Candidate,
    sender: &Sender<Result<MonitorEvent, Error>>,
) -> Result<(), Error> {
    let CandidateState::Active {
        activation_id,
        removal_sent,
    } = &mut candidate.state
    else {
        return Ok(());
    };
    if *removal_sent {
        return Ok(());
    }

    *removal_sent = true;
    sender
        .send(Ok(MonitorEvent::Remove {
            activation_id: *activation_id,
        }))
        .await
        .map_err(|_| Error::new(ErrorKind::BrokenPipe, "Monitor receiver closed"))
}

fn handle_release(candidates: &mut HashMap<Entry, Candidate>, release: Release) {
    let Some(candidate) = candidates.values_mut().find(|candidate| {
        matches!(
            candidate.state,
            CandidateState::Active { activation_id, .. } if activation_id == release.activation_id
        )
    }) else {
        tracing::debug!(
            activation_id = release.activation_id,
            "Ignoring stale input activation release"
        );
        return;
    };

    candidate.backoff.reset();
    candidate.state = match release.cause {
        ReleaseCause::Disconnected => CandidateState::Ready,
        ReleaseCause::Failed => CandidateState::Blocked {
            until: Instant::now() + BLOCKED_RETRY,
        },
    };
    tracing::info!(
        activation_id = release.activation_id,
        path = ?candidate.info.path(),
        cause = ?release.cause,
        "Released input activation"
    );
}

async fn reconcile(
    registry: &Registry,
    candidates: &mut HashMap<Entry, Candidate>,
    device_filter: &DeviceFilter,
    sender: &Sender<Result<MonitorEvent, Error>>,
    next_activation_id: &mut ActivationId,
) -> Result<(), Error> {
    let mut keys = candidates
        .iter()
        .filter_map(|(key, candidate)| {
            matches!(candidate.state, CandidateState::Ready).then_some(*key)
        })
        .collect::<Vec<_>>();
    keys.sort_by(|a, b| candidates[a].info.path().cmp(candidates[b].info.path()));

    for key in keys {
        let path = candidates[&key].info.path().to_owned();
        let mut result = Interceptor::claim(&path, key, registry, device_filter).await;
        if matches!(result, Err(ClaimError::Interrupted(_))) {
            result = Interceptor::claim(&path, key, registry, device_filter).await;
        }

        match result {
            Ok(interceptor) => {
                let activation_id = *next_activation_id;
                *next_activation_id = next_activation_id
                    .checked_add(1)
                    .ok_or_else(|| Error::new(ErrorKind::Other, "Input activation ID exhausted"))?;

                let candidate = candidates.get_mut(&key).unwrap();
                candidate.backoff.reset();
                candidate.state = CandidateState::Active {
                    activation_id,
                    removal_sent: false,
                };
                tracing::info!(
                    activation_id,
                    path = ?candidate.info.path(),
                    aliases = ?candidate.aliases,
                    name = ?candidate.info.name(),
                    origin = ?candidate.info.origin(),
                    bustype = format_args!("0x{:04x}", candidate.info.bustype()),
                    "Activated input candidate"
                );

                sender
                    .send(Ok(MonitorEvent::Activated(ActivatedDevice {
                        id: activation_id,
                        interceptor,
                    })))
                    .await
                    .map_err(|_| Error::new(ErrorKind::BrokenPipe, "Monitor receiver closed"))?;
            }
            Err(ClaimError::Gone(err)) => {
                tracing::debug!(path = ?path, error = %err, "Input candidate disappeared before claim");
                candidates.remove(&key);
            }
            Err(ClaimError::Stale) => {
                tracing::debug!(path = ?path, "Input candidate node instance changed before claim");
                candidates.remove(&key);
            }
            Err(ClaimError::Owned) => {
                tracing::debug!(path = ?path, "Removing rkvm-owned input candidate from inventory");
                candidates.remove(&key);
            }
            Err(ClaimError::NotApplicable) => {
                candidates.get_mut(&key).unwrap().state = CandidateState::Rejected;
            }
            Err(ClaimError::Unsupported(err)) => {
                tracing::info!(path = ?path, error = %err, "Input candidate is unsupported");
                candidates.get_mut(&key).unwrap().state = CandidateState::Unsupported;
            }
            Err(ClaimError::Busy) => schedule_backoff(candidates.get_mut(&key).unwrap(), "busy"),
            Err(ClaimError::Interrupted(err)) => {
                tracing::debug!(path = ?path, error = %err, "Input claim repeatedly interrupted");
                schedule_backoff(candidates.get_mut(&key).unwrap(), "interrupted");
            }
            Err(ClaimError::Permission(err)) => {
                tracing::error!(path = ?path, error = %err, "Input candidate access blocked; retrying in 5 seconds");
                candidates.get_mut(&key).unwrap().state = CandidateState::Blocked {
                    until: Instant::now() + BLOCKED_RETRY,
                };
            }
            Err(ClaimError::Output(err))
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ENOENT) | Some(libc::ENODEV) | Some(libc::EIO)
                ) =>
            {
                tracing::debug!(path = ?path, error = %err, "Input candidate vanished during output creation");
                candidates.remove(&key);
            }
            Err(ClaimError::Output(err)) if err.raw_os_error() == Some(libc::EINTR) => {
                schedule_backoff(
                    candidates.get_mut(&key).unwrap(),
                    "output creation interrupted",
                );
            }
            Err(ClaimError::Output(err) | ClaimError::Other(err)) => {
                tracing::error!(path = ?path, error = %err, "Input candidate failed; retrying in 5 seconds");
                candidates.get_mut(&key).unwrap().state = CandidateState::Blocked {
                    until: Instant::now() + BLOCKED_RETRY,
                };
            }
            Err(ClaimError::Fatal(err)) => return Err(err),
        }
    }

    Ok(())
}

fn schedule_backoff(candidate: &mut Candidate, reason: &str) {
    let delay = candidate.backoff.next();
    let retry = candidate.backoff.failures;
    candidate.state = CandidateState::Waiting {
        until: Instant::now() + delay,
    };
    tracing::info!(
        path = ?candidate.info.path(),
        reason,
        retry,
        delay_ms = delay.as_millis(),
        "Input candidate unavailable; retry scheduled"
    );
}

async fn scan_devices(registry: &Registry) -> Result<HashMap<Entry, Snapshot>, Error> {
    let mut snapshots = HashMap::new();
    let mut read_dir = fs::read_dir(INPUT_PATH).await?;

    while let Some(entry) = next_entry_resilient(&mut read_dir).await? {
        let path = entry.path();
        if !is_event_filename(&path) {
            continue;
        }

        let canonical_path = match fs::canonicalize(&path).await {
            Ok(path) => path,
            Err(err) if is_disappearance(&err) => continue,
            Err(err) => return Err(err),
        };
        let metadata = match fs::metadata(&canonical_path).await {
            Ok(metadata) => metadata,
            Err(err) if is_disappearance(&err) => continue,
            Err(err) => return Err(err),
        };
        let key = Entry::from_metadata(&metadata);

        if registry.contains(key) {
            snapshots.entry(key).or_insert_with(|| Snapshot::Owned {
                path: canonical_path,
                aliases: BTreeSet::new(),
            });
            continue;
        }

        let info = match open_info_resilient(&canonical_path).await {
            Ok(info) => info,
            Err(err) if is_disappearance(&err) => continue,
            Err(err) => {
                tracing::error!(path = ?canonical_path, error = %err, "Unable to inspect input candidate; retrying on the next reconciliation");
                continue;
            }
        };
        snapshots.insert(
            key,
            Snapshot::Candidate {
                info,
                aliases: BTreeSet::new(),
            },
        );
    }

    for alias_path in ALIAS_PATHS {
        let mut read_dir = match fs::read_dir(alias_path).await {
            Ok(read_dir) => read_dir,
            Err(err) if is_disappearance(&err) => continue,
            Err(err) => return Err(err),
        };

        while let Some(entry) = next_entry_resilient(&mut read_dir).await? {
            let alias = entry.path();
            let canonical_path = match fs::canonicalize(&alias).await {
                Ok(path) if is_event_filename(&path) => path,
                Ok(_) => continue,
                Err(err) if is_disappearance(&err) => continue,
                Err(err) => return Err(err),
            };
            let metadata = match fs::metadata(&canonical_path).await {
                Ok(metadata) => metadata,
                Err(err) if is_disappearance(&err) => continue,
                Err(err) => return Err(err),
            };
            let key = Entry::from_metadata(&metadata);
            if let Some(snapshot) = snapshots.get_mut(&key) {
                tracing::debug!(alias = ?alias, canonical = ?snapshot.path(), "Associated input alias with canonical candidate");
                snapshot.aliases_mut().insert(alias);
            }
        }
    }

    Ok(snapshots)
}

async fn next_entry_resilient(read_dir: &mut fs::ReadDir) -> Result<Option<fs::DirEntry>, Error> {
    loop {
        match read_dir.next_entry().await {
            Err(err) if err.raw_os_error() == Some(libc::EINTR) => continue,
            result => return result,
        }
    }
}

async fn open_info_resilient(path: &Path) -> Result<DeviceInfo, Error> {
    match DeviceInfo::open(path).await {
        Err(err) if err.raw_os_error() == Some(libc::EINTR) => DeviceInfo::open(path).await,
        result => result,
    }
}

fn is_event_filename(path: &Path) -> bool {
    path.file_name()
        .and_then(OsStr::to_str)
        .map_or(false, |name| name.starts_with("event"))
}

fn is_disappearance(err: &Error) -> bool {
    err.kind() == ErrorKind::NotFound || err.raw_os_error() == Some(libc::ENODEV)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_follows_bounded_schedule() {
        let mut backoff = RetryBackoff::new();
        assert_eq!(backoff.next(), Duration::from_millis(100));
        assert_eq!(backoff.next(), Duration::from_millis(250));
        assert_eq!(backoff.next(), Duration::from_millis(500));
        assert_eq!(backoff.next(), Duration::from_secs(1));
        assert_eq!(backoff.next(), Duration::from_secs(2));
        assert_eq!(backoff.next(), Duration::from_secs(2));
        assert_eq!(backoff.next(), Duration::from_secs(2));
        backoff.reset();
        assert_eq!(backoff.next(), Duration::from_millis(100));
    }

    #[test]
    fn stale_release_cannot_clear_current_activation() {
        let key = Entry {
            device: 1,
            inode: 2,
        };
        let mut candidates = HashMap::from([(
            key,
            Candidate {
                info: DeviceInfo::test("/dev/input/event1"),
                aliases: BTreeSet::new(),
                state: CandidateState::Active {
                    activation_id: 9,
                    removal_sent: false,
                },
                backoff: RetryBackoff::new(),
            },
        )]);

        handle_release(
            &mut candidates,
            Release {
                activation_id: 8,
                cause: ReleaseCause::Disconnected,
            },
        );
        assert!(matches!(
            candidates[&key].state,
            CandidateState::Active {
                activation_id: 9,
                ..
            }
        ));

        handle_release(
            &mut candidates,
            Release {
                activation_id: 9,
                cause: ReleaseCause::Failed,
            },
        );
        assert!(matches!(
            candidates[&key].state,
            CandidateState::Blocked { .. }
        ));
    }

    #[test]
    fn event_path_detection_does_not_accept_prefix_directories() {
        assert!(is_event_filename(Path::new("/dev/input/event12")));
        assert!(!is_event_filename(Path::new("/dev/input/by-id")));
        assert!(!is_event_filename(Path::new("/dev/input/not-an-event")));
    }
}
