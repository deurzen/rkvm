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
use crate::writer::Writer;

use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::fs;
use std::io::{Error, ErrorKind};
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone)]
pub struct DeviceInfo {
    path: PathBuf,
    name: CString,
    vendor: u16,
    product: u16,
    version: u16,
}

impl DeviceInfo {
    pub async fn open(path: &Path) -> Result<Self, Error> {
        let evdev = Evdev::open(path).await?;
        Ok(Self::from_evdev(path, &evdev))
    }

    fn from_evdev(path: &Path, evdev: &Evdev) -> Self {
        let name = unsafe { glue::libevdev_get_name(evdev.as_ptr()) };
        let name = unsafe { CStr::from_ptr(name) }.to_owned();

        Self {
            path: path.to_owned(),
            name,
            vendor: unsafe { glue::libevdev_get_id_vendor(evdev.as_ptr()) as _ },
            product: unsafe { glue::libevdev_get_id_product(evdev.as_ptr()) as _ },
            version: unsafe { glue::libevdev_get_id_version(evdev.as_ptr()) as _ },
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn name(&self) -> &CStr {
        &self.name
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
    InputLost,
}

enum Read {
    Event(Event),
    InputLost,
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
                    return Ok(Read::InputLost);
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
                glue::EV_SYN => match code as _ {
                    glue::SYN_REPORT => Some(Event::Sync(SyncEvent::All)),
                    glue::SYN_DROPPED => {
                        self.recover_input_loss().await?;
                        return Ok(Read::InputLost);
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
                Read::InputLost => return Ok(Frame::InputLost),
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

        let writer = Writer::from_evdev(&self.evdev).await?;
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
                    Err(err) => return Err(err),
                }
            }

            let result = file.readable().await?.try_io(|_| self.try_read_raw());

            match result {
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
    pub(crate) async fn open<F>(
        path: &Path,
        registry: &Registry,
        device_filter: &F,
    ) -> Result<Self, OpenError>
    where
        F: Fn(&DeviceInfo) -> bool + ?Sized,
    {
        let evdev = Evdev::open(path).await?;
        let info = DeviceInfo::from_evdev(path, &evdev);

        if !device_filter(&info) {
            tracing::info!(
                path = ?info.path(),
                name = ?info.name(),
                vendor = %info.vendor(),
                product = %info.product(),
                version = %info.version(),
                "Ignored device because it is not whitelisted",
            );
            return Err(OpenError::NotAppliable);
        }

        let metadata = evdev.file().unwrap().get_ref().metadata()?;

        let reader_handle = registry
            .register(Entry::from_metadata(&metadata))
            .ok_or(OpenError::NotAppliable)?;

        // "Upon binding to a device or resuming from suspend, a driver must report
        // the current switch state. This ensures that the device, kernel, and userspace
        // state is in sync."
        // We have no way of knowing that.
        let sw = unsafe { glue::libevdev_has_event_type(evdev.as_ptr(), glue::EV_SW) };
        if sw == 1 {
            return Err(OpenError::NotAppliable);
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
                    return Err(Error::from_raw_os_error(-ret).into());
                }
            }
        }

        unsafe {
            glue::libevdev_set_id_bustype(evdev.as_ptr(), glue::BUS_VIRTUAL as _);
        }

        let ret =
            unsafe { glue::libevdev_grab(evdev.as_ptr(), glue::libevdev_grab_mode_LIBEVDEV_GRAB) };

        if ret < 0 {
            // We do not use ErrorKind::ResourceBusy because it is a nightly-only API.
            let err = if ret == -libc::EBUSY {
                tracing::info!(
                    "Ignored {:?} because it is busy and can not be grabbed",
                    path
                );
                OpenError::NotAppliable
            } else {
                Error::from_raw_os_error(-ret).into()
            };

            return Err(err);
        }

        let writer = Writer::from_evdev(&evdev).await?;
        let path = writer
            .path()
            .ok_or_else(|| Error::new(ErrorKind::Other, "No syspath for writer"))?;

        let metadata = fs::metadata(path)?;
        let writer_handle = registry
            .register(Entry::from_metadata(&metadata))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Writer already registered"))?;

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

#[derive(Error, Debug)]
pub(crate) enum OpenError {
    #[error("Not appliable")]
    NotAppliable,
    #[error(transparent)]
    Io(#[from] Error),
}
