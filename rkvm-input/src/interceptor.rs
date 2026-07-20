mod caps;

pub use caps::{AbsCaps, KeyCaps, RelCaps, Repeat};

use crate::abs::{AbsAxis, AbsEvent, ToolType};
use crate::convert::Convert;
use crate::evdev::Evdev;
use crate::event::Event;
use crate::glue;
use crate::key::{Key, KeyEvent};
use crate::registry::{Entry, Handle, Registry};
use crate::rel::{RelAxis, RelEvent};
use crate::sync::SyncEvent;
use crate::uinput::CreateError;
use crate::writer::Writer;

use serde::Deserialize;
use std::collections::{HashSet, VecDeque};
use std::ffi::{CStr, CString};
use std::fs;
use std::io::{Error, ErrorKind};
use std::mem::MaybeUninit;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs as async_fs;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DeviceOrigin {
    Physical,
    Virtual,
}

impl DeviceOrigin {
    fn from_sysfs_path(path: &Path) -> Self {
        if path.starts_with("/sys/devices/virtual/input") {
            Self::Virtual
        } else {
            Self::Physical
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeviceCapabilities {
    pub key: bool,
    pub relative: bool,
    pub absolute: bool,
}

#[derive(Clone)]
pub struct DeviceInfo {
    path: PathBuf,
    sysfs_path: PathBuf,
    origin: DeviceOrigin,
    name: CString,
    bustype: u16,
    vendor: u16,
    product: u16,
    version: u16,
    capabilities: DeviceCapabilities,
}

impl DeviceInfo {
    pub async fn open(path: &Path) -> Result<Self, Error> {
        let evdev = Evdev::open(path).await?;
        Self::from_evdev(path, &evdev).await
    }

    async fn from_evdev(path: &Path, evdev: &Evdev) -> Result<Self, Error> {
        let name = unsafe { glue::libevdev_get_name(evdev.as_ptr()) };
        let name = unsafe { CStr::from_ptr(name) }.to_owned();
        let metadata = evdev.file().unwrap().get_ref().metadata()?;
        let sysfs_path = source_sysfs_path(&metadata).await?;

        Ok(Self {
            path: path.to_owned(),
            origin: DeviceOrigin::from_sysfs_path(&sysfs_path),
            sysfs_path,
            name,
            bustype: unsafe { glue::libevdev_get_id_bustype(evdev.as_ptr()) as _ },
            vendor: unsafe { glue::libevdev_get_id_vendor(evdev.as_ptr()) as _ },
            product: unsafe { glue::libevdev_get_id_product(evdev.as_ptr()) as _ },
            version: unsafe { glue::libevdev_get_id_version(evdev.as_ptr()) as _ },
            capabilities: DeviceCapabilities {
                key: unsafe { glue::libevdev_has_event_type(evdev.as_ptr(), glue::EV_KEY) == 1 },
                relative: unsafe {
                    glue::libevdev_has_event_type(evdev.as_ptr(), glue::EV_REL) == 1
                },
                absolute: unsafe {
                    glue::libevdev_has_event_type(evdev.as_ptr(), glue::EV_ABS) == 1
                },
            },
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sysfs_path(&self) -> &Path {
        &self.sysfs_path
    }

    pub fn origin(&self) -> DeviceOrigin {
        self.origin
    }

    pub fn name(&self) -> &CStr {
        &self.name
    }

    pub fn bustype(&self) -> u16 {
        self.bustype
    }

    pub fn vendor(&self) -> u16 {
        self.vendor
    }

    pub fn product(&self) -> u16 {
        self.product
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn capabilities(&self) -> DeviceCapabilities {
        self.capabilities
    }

    #[cfg(test)]
    pub(crate) fn test(path: &str) -> Self {
        Self {
            path: PathBuf::from(path),
            sysfs_path: PathBuf::from("/sys/devices/virtual/input/input0/event0"),
            origin: DeviceOrigin::Virtual,
            name: CString::new("test device").unwrap(),
            bustype: 0x0006,
            vendor: 1,
            product: 2,
            version: 3,
            capabilities: DeviceCapabilities {
                key: true,
                relative: false,
                absolute: false,
            },
        }
    }
}

async fn source_sysfs_path(metadata: &fs::Metadata) -> Result<PathBuf, Error> {
    let device = metadata.rdev();
    let major = unsafe { libc::major(device) };
    let minor = unsafe { libc::minor(device) };
    async_fs::canonicalize(format!("/sys/dev/char/{major}:{minor}/device")).await
}

fn input_error(ret: i32) -> Error {
    // ENODEV means that the device got disconnected. However, ErrorKind doesn't
    // have support for it yet, so translate to BrokenPipe here to not introduce
    // platform specific code to rkvm-server.
    if ret == -libc::ENODEV {
        Error::new(ErrorKind::BrokenPipe, "Device disconnected")
    } else {
        Error::from_raw_os_error(-ret)
    }
}

fn recovery_error(err: Error) -> Error {
    Error::new(
        ErrorKind::BrokenPipe,
        format!("Input recovery failed: {err}"),
    )
}

pub enum Frame {
    Events(Vec<Event>),
    InputLost { pressed_keys: HashSet<Key> },
}

enum Read {
    Event(Event),
    InputLost { pressed_keys: HashSet<Key> },
}

enum RawRead {
    Event(u16, u16, i32),
    InputLost,
}

enum NextEvent {
    Success(u16, u16, i32),
    Sync,
}

pub struct Interceptor {
    evdev: Evdev,
    registry: Registry,
    writer: Option<Writer>,
    // The state of `read` is stored here to make it cancel safe.
    events: VecDeque<Event>,
    writing: Option<(u16, u16, i32)>,

    _reader_handle: Handle,
    writer_handle: Option<Handle>,
}

impl Interceptor {
    async fn read(&mut self) -> Result<Read, Error> {
        if let Some((r#type, code, value)) = self.writing {
            tracing::trace!("Resuming interrupted write");

            self.writer_mut()?.write_raw(r#type, code, value).await?;
            self.writing = None;
        }

        while !matches!(self.events.back(), Some(Event::Sync(SyncEvent::All))) {
            let (r#type, code, value) = match self.read_raw().await? {
                RawRead::Event(r#type, code, value) => (r#type, code, value),
                RawRead::InputLost => {
                    self.recover_input_loss().await?;
                    return Ok(Read::InputLost {
                        pressed_keys: self.pressed_keys(),
                    });
                }
            };

            let event = match r#type as _ {
                glue::EV_REL => {
                    RelAxis::from_raw(code).map(|axis| Event::Rel(RelEvent { axis, value }))
                }
                glue::EV_ABS => match code as _ {
                    glue::ABS_MT_TOOL_TYPE => {
                        ToolType::from_raw(value).map(|value| AbsEvent::MtToolType { value })
                    }
                    _ => AbsAxis::from_raw(code).map(|axis| AbsEvent::Axis { axis, value }),
                }
                .map(Event::Abs),
                glue::EV_KEY if value == 0 || value == 1 => Key::from_raw(code).map(|key| {
                    Event::Key(KeyEvent {
                        key,
                        down: value == 1,
                    })
                }),
                // The cloned uinput device generates repeats from EV_REP itself.
                // Echoing source repeat events would also bypass server routing.
                glue::EV_KEY if value == 2 => continue,
                glue::EV_SYN => match code as _ {
                    glue::SYN_REPORT => Some(Event::Sync(SyncEvent::All)),
                    glue::SYN_DROPPED => {
                        self.recover_input_loss().await?;
                        return Ok(Read::InputLost {
                            pressed_keys: self.pressed_keys(),
                        });
                    }
                    glue::SYN_MT_REPORT => Some(Event::Sync(SyncEvent::Mt)),
                    _ => continue,
                },
                _ => None,
            };

            if let Some(event) = event {
                self.events.push_back(event);
                continue;
            }

            self.writing = Some((r#type, code, value));
            self.writer_mut()?.write_raw(r#type, code, value).await?;
            self.writing = None;
        }

        Ok(Read::Event(self.events.pop_front().unwrap()))
    }

    pub async fn read_frame(&mut self) -> Result<Frame, Error> {
        let mut events = Vec::new();

        loop {
            let event = match self.read().await? {
                Read::Event(event) => event,
                Read::InputLost { pressed_keys } => return Ok(Frame::InputLost { pressed_keys }),
            };
            let is_frame_end = matches!(&event, Event::Sync(SyncEvent::All));
            events.push(event);

            if is_frame_end {
                return Ok(Frame::Events(events));
            }
        }
    }

    pub async fn write(&mut self, event: &Event) -> Result<(), Error> {
        self.writer_mut()?.write(event).await
    }

    pub async fn write_frame(&mut self, events: &[Event]) -> Result<(), Error> {
        self.writer_mut()?.write_frame(events).await
    }

    pub async fn set_key_state(&mut self, pressed_keys: &HashSet<Key>) -> Result<(), Error> {
        self.writer_mut()?.set_key_state(pressed_keys).await
    }

    pub fn name(&self) -> &CStr {
        let name = unsafe { glue::libevdev_get_name(self.evdev.as_ptr()) };
        let name = unsafe { CStr::from_ptr(name) };

        name
    }

    pub fn vendor(&self) -> u16 {
        unsafe { glue::libevdev_get_id_vendor(self.evdev.as_ptr()) as _ }
    }

    pub fn product(&self) -> u16 {
        unsafe { glue::libevdev_get_id_product(self.evdev.as_ptr()) as _ }
    }

    pub fn version(&self) -> u16 {
        unsafe { glue::libevdev_get_id_version(self.evdev.as_ptr()) as _ }
    }

    pub fn rel(&self) -> RelCaps {
        RelCaps::new(self)
    }

    pub fn abs(&self) -> AbsCaps {
        AbsCaps::new(self)
    }

    pub fn key(&self) -> KeyCaps {
        KeyCaps::new(self)
    }

    pub fn pressed_keys(&self) -> HashSet<Key> {
        self.key()
            .filter(|key| {
                let Some(code) = key.to_raw() else {
                    return false;
                };
                unsafe {
                    glue::libevdev_get_event_value(self.evdev.as_ptr(), glue::EV_KEY, code as _)
                        != 0
                }
            })
            .collect()
    }

    pub fn repeat(&self) -> Repeat {
        Repeat::new(self)
    }

    fn writer_mut(&mut self) -> Result<&mut Writer, Error> {
        self.writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "Local writer unavailable"))
    }

    async fn recover_input_loss(&mut self) -> Result<(), Error> {
        tracing::warn!(
            "Dropped {} event{}; resetting input state",
            self.events.len(),
            if self.events.len() == 1 { "" } else { "s" }
        );

        self.events.clear();
        self.writing = None;
        self.drain_sync_events().map_err(recovery_error)
    }

    fn drain_sync_events(&self) -> Result<(), Error> {
        loop {
            match self.try_read_event(glue::libevdev_read_flag_LIBEVDEV_READ_FLAG_SYNC) {
                Ok(NextEvent::Success(..) | NextEvent::Sync) => {}
                Err(err) if err.kind() == ErrorKind::WouldBlock => return Ok(()),
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn reset_writer(&mut self) -> Result<(), Error> {
        self.writer.take();
        self.writer_handle.take();

        let _gate = self.registry.lock().await;
        let writer = Writer::from_evdev(&self.evdev)
            .await
            .map_err(create_error_into_io)?;
        let path = writer
            .path()
            .ok_or_else(|| Error::new(ErrorKind::Other, "No syspath for writer"))?;
        let metadata = fs::metadata(path)?;
        let writer_handle = self
            .registry
            .register(Entry::from_metadata(&metadata))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Writer already registered"))?;

        self.writer = Some(writer);
        self.writer_handle = Some(writer_handle);
        Ok(())
    }

    async fn read_raw(&mut self) -> Result<RawRead, Error> {
        let file = self.evdev.file().unwrap();

        loop {
            if self.has_pending_event()? {
                match self.try_read_raw() {
                    Ok(event) => return Ok(event),
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                    Err(err) if err.raw_os_error() == Some(libc::EINTR) => continue,
                    Err(err) => return Err(err),
                }
            }

            let mut readable = match file.readable().await {
                Ok(readable) => readable,
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => continue,
                Err(err) => return Err(err),
            };
            let result = readable.try_io(|_| self.try_read_raw());

            match result {
                Ok(Err(err)) if err.raw_os_error() == Some(libc::EINTR) => continue,
                Ok(result) => return result,
                Err(_) => continue, // This means it would block.
            }
        }
    }

    fn has_pending_event(&self) -> Result<bool, Error> {
        let ret = unsafe { glue::libevdev_has_event_pending(self.evdev.as_ptr()) };
        if ret < 0 {
            return Err(input_error(ret));
        }

        Ok(ret != 0)
    }

    fn try_read_raw(&self) -> Result<RawRead, Error> {
        match self.try_read_event(glue::libevdev_read_flag_LIBEVDEV_READ_FLAG_NORMAL)? {
            NextEvent::Success(r#type, code, value) => Ok(RawRead::Event(r#type, code, value)),
            NextEvent::Sync => Ok(RawRead::InputLost),
        }
    }

    fn try_read_event(&self, flags: u32) -> Result<NextEvent, Error> {
        let mut event = MaybeUninit::uninit();
        let ret =
            unsafe { glue::libevdev_next_event(self.evdev.as_ptr(), flags, event.as_mut_ptr()) };

        if ret < 0 {
            return Err(input_error(ret));
        }

        let event = unsafe { event.assume_init() };
        match ret as _ {
            glue::libevdev_read_status_LIBEVDEV_READ_STATUS_SUCCESS => {
                Ok(NextEvent::Success(event.type_, event.code, event.value))
            }
            glue::libevdev_read_status_LIBEVDEV_READ_STATUS_SYNC => Ok(NextEvent::Sync),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid libevdev read status",
            )),
        }
    }

    #[tracing::instrument(skip(registry, device_filter))]
    pub(crate) async fn claim<F>(
        path: &Path,
        expected: Entry,
        registry: &Registry,
        device_filter: &F,
    ) -> Result<Self, ClaimError>
    where
        F: Fn(&DeviceInfo) -> bool + ?Sized,
    {
        let _gate = registry.lock().await;
        if registry.contains(expected) {
            return Err(ClaimError::Owned);
        }

        let evdev = Evdev::open(path).await.map_err(ClaimError::source)?;
        let metadata = evdev
            .file()
            .unwrap()
            .get_ref()
            .metadata()
            .map_err(ClaimError::source)?;
        if Entry::from_metadata(&metadata) != expected {
            return Err(ClaimError::Stale);
        }

        let info = DeviceInfo::from_evdev(path, &evdev)
            .await
            .map_err(ClaimError::source)?;
        if !device_filter(&info) {
            return Err(ClaimError::NotApplicable);
        }

        let reader_handle = registry.register(expected).ok_or(ClaimError::Owned)?;

        // "Upon binding to a device or resuming from suspend, a driver must report
        // the current switch state. This ensures that the device, kernel, and userspace
        // state is in sync."
        // We have no way of knowing that.
        let sw = unsafe { glue::libevdev_has_event_type(evdev.as_ptr(), glue::EV_SW) };
        if sw == 1 {
            return Err(ClaimError::Unsupported(Error::new(
                ErrorKind::Unsupported,
                "switch devices cannot be reproduced safely",
            )));
        }

        // Some buggy kernels can report nonsense abs info, so check for it and disable the axes.
        for i in 0..glue::ABS_CNT {
            let abs_info = unsafe { glue::libevdev_get_abs_info(evdev.as_ptr(), i).as_ref() };
            let abs_info = match abs_info {
                Some(abs_info) => abs_info,
                None => continue,
            };

            // See Linux source at drivers/input/misc/uinput.c#L408 commit 93f5de5f648d2b1ce3540a4ac71756d4a852dc23.

            let min = abs_info.minimum;
            let max = abs_info.maximum;

            if (min != 0 || max != 0) && max < min {
                tracing::warn!(
                    min = %min,
                    max = max,
                    axis = i,
                    "Detected nonsense min and max values for absolute axis, disabling it",
                );

                let ret =
                    unsafe { glue::libevdev_disable_event_code(evdev.as_ptr(), glue::EV_ABS, i) };

                if ret < 0 {
                    return Err(ClaimError::source(Error::from_raw_os_error(-ret)));
                }
            }
        }

        unsafe {
            glue::libevdev_set_id_bustype(evdev.as_ptr(), glue::BUS_VIRTUAL as _);
        }

        let ret =
            unsafe { glue::libevdev_grab(evdev.as_ptr(), glue::libevdev_grab_mode_LIBEVDEV_GRAB) };

        if ret < 0 {
            let err = Error::from_raw_os_error(-ret);
            return Err(if ret == -libc::EBUSY {
                ClaimError::Busy
            } else {
                ClaimError::source(err)
            });
        }

        let writer = Writer::from_evdev(&evdev).await.map_err(|err| match err {
            CreateError::Open(err) => ClaimError::Fatal(err),
            CreateError::Create(err) => ClaimError::Output(err),
        })?;
        let path = writer.path().ok_or_else(|| {
            ClaimError::Fatal(Error::new(ErrorKind::Other, "No devnode for writer"))
        })?;

        let metadata = fs::metadata(path).map_err(ClaimError::Fatal)?;
        let writer_handle = registry
            .register(Entry::from_metadata(&metadata))
            .ok_or_else(|| {
                ClaimError::Fatal(Error::new(ErrorKind::Other, "Writer already registered"))
            })?;

        Ok(Self {
            evdev,
            registry: registry.clone(),
            writer: Some(writer),
            events: VecDeque::new(),
            writing: None,

            _reader_handle: reader_handle,
            writer_handle: Some(writer_handle),
        })
    }
}

unsafe impl Send for Interceptor {}

fn create_error_into_io(err: CreateError) -> Error {
    match err {
        CreateError::Open(err) | CreateError::Create(err) => err,
    }
}

#[derive(Error, Debug)]
pub(crate) enum ClaimError {
    #[error("candidate is already owned by rkvm")]
    Owned,
    #[error("candidate node instance changed")]
    Stale,
    #[error("candidate no longer matches policy")]
    NotApplicable,
    #[error("candidate is busy")]
    Busy,
    #[error("candidate disappeared: {0}")]
    Gone(Error),
    #[error("candidate operation was interrupted: {0}")]
    Interrupted(Error),
    #[error("candidate access is blocked: {0}")]
    Permission(Error),
    #[error("candidate is unsupported: {0}")]
    Unsupported(Error),
    #[error("failed to create candidate output: {0}")]
    Output(Error),
    #[error("input infrastructure failed: {0}")]
    Fatal(Error),
    #[error("candidate operation failed: {0}")]
    Other(Error),
}

impl ClaimError {
    fn source(err: Error) -> Self {
        match err.raw_os_error() {
            Some(libc::ENOENT) | Some(libc::ENODEV) => Self::Gone(err),
            Some(libc::EINTR) => Self::Interrupted(err),
            Some(libc::EACCES) | Some(libc::EPERM) => Self::Permission(err),
            Some(libc::EINVAL) | Some(libc::ENOTTY) => Self::Unsupported(err),
            _ => Self::Other(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sysfs_origin_detects_only_virtual_input_subtree() {
        assert_eq!(
            DeviceOrigin::from_sysfs_path(Path::new("/sys/devices/virtual/input/input12/event7")),
            DeviceOrigin::Virtual
        );
        assert_eq!(
            DeviceOrigin::from_sysfs_path(Path::new(
                "/sys/devices/pci0000:00/usb1/1-1/input/input4/event3"
            )),
            DeviceOrigin::Physical
        );
        assert_eq!(
            DeviceOrigin::from_sysfs_path(Path::new("/sys/devices/virtual/misc/uinput")),
            DeviceOrigin::Physical
        );
    }

    #[test]
    fn source_errors_have_retryable_dispositions() {
        assert!(matches!(
            ClaimError::source(Error::from_raw_os_error(libc::ENOENT)),
            ClaimError::Gone(_)
        ));
        assert!(matches!(
            ClaimError::source(Error::from_raw_os_error(libc::EINTR)),
            ClaimError::Interrupted(_)
        ));
        assert!(matches!(
            ClaimError::source(Error::from_raw_os_error(libc::EBUSY)),
            ClaimError::Other(_)
        ));
        assert!(matches!(
            ClaimError::source(Error::from_raw_os_error(libc::EACCES)),
            ClaimError::Permission(_)
        ));
        assert!(matches!(
            ClaimError::source(Error::from_raw_os_error(libc::ENOTTY)),
            ClaimError::Unsupported(_)
        ));
    }
}
