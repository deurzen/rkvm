use libc::c_int;

use crate::abs::{AbsAxis, AbsEvent, AbsInfo};
use crate::convert::Convert;
use crate::evdev::Evdev;
use crate::event::Event;
use crate::glue::{self, input_absinfo};
use crate::key::{Key, KeyEvent};
use crate::rel::{RelAxis, RelEvent};
use crate::uinput::{CreateError, Uinput};

use std::collections::HashSet;
use std::ffi::{CStr, OsStr};
use std::io::Error;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;

pub struct Writer {
    uinput: Uinput,
    pressed_keys: HashSet<Key>,
}

fn raw_event(event: &Event) -> Option<(u16, u16, i32)> {
    let (r#type, code, value) = match event {
        Event::Rel(RelEvent { axis, value }) => (glue::EV_REL, axis.to_raw(), Some(*value)),
        Event::Abs(event) => match event {
            AbsEvent::Axis { axis, value } => (glue::EV_ABS, axis.to_raw(), Some(*value)),
            AbsEvent::MtToolType { value } => (
                glue::EV_ABS,
                Some(glue::ABS_MT_TOOL_TYPE as _),
                value.to_raw(),
            ),
        },
        Event::Key(KeyEvent { down, key }) => (glue::EV_KEY, key.to_raw(), Some(*down as _)),
        Event::Sync(event) => (glue::EV_SYN, event.to_raw(), Some(0)),
    };

    Some((r#type as _, code?, value?))
}

impl Writer {
    pub fn builder() -> Result<WriterBuilder, Error> {
        WriterBuilder::new()
    }

    pub async fn write(&mut self, event: &Event) -> Result<(), Error> {
        if let Some((r#type, code, value)) = raw_event(event) {
            self.write_raw(r#type, code, value).await?;
            self.update_key_state(event);
        }

        Ok(())
    }

    pub async fn write_frame(&mut self, events: &[Event]) -> Result<(), Error> {
        let raw_events = events.iter().filter_map(raw_event).collect::<Vec<_>>();
        let mut cursor = 0;

        while cursor < raw_events.len() {
            let result = self.uinput.file().writable().await?.try_io(|_| {
                while let Some((r#type, code, value)) = raw_events.get(cursor).copied() {
                    let ret = unsafe {
                        glue::libevdev_uinput_write_event(
                            self.uinput.as_ptr(),
                            r#type as _,
                            code as _,
                            value,
                        )
                    };

                    if ret < 0 {
                        return Err(Error::from_raw_os_error(-ret).into());
                    }

                    cursor += 1;
                }

                Ok(())
            });

            match result {
                Ok(result) => result?,
                Err(_) => continue, // This means it would block.
            }
        }

        for event in events {
            self.update_key_state(event);
        }
        Ok(())
    }

    pub async fn set_key_state(&mut self, pressed_keys: &HashSet<Key>) -> Result<(), Error> {
        let events = key_state_events(&self.pressed_keys, pressed_keys);
        if events.is_empty() {
            return Ok(());
        }

        self.write_frame(&events).await
    }

    pub fn pressed_keys(&self) -> &HashSet<Key> {
        &self.pressed_keys
    }

    fn update_key_state(&mut self, event: &Event) {
        let Event::Key(KeyEvent { key, down }) = event else {
            return;
        };

        if *down {
            self.pressed_keys.insert(*key);
        } else {
            self.pressed_keys.remove(key);
        }
    }

    pub fn path(&self) -> Option<&Path> {
        let path = unsafe { glue::libevdev_uinput_get_devnode(self.uinput.as_ptr()) };
        if path.is_null() {
            return None;
        }

        let path = unsafe { CStr::from_ptr(path) };
        let path = OsStr::from_bytes(path.to_bytes());
        let path = Path::new(path);

        Some(path)
    }

    pub(crate) async fn from_evdev(evdev: &Evdev) -> Result<Self, CreateError> {
        Ok(Self {
            uinput: Uinput::from_evdev(evdev).await?,
            pressed_keys: HashSet::new(),
        })
    }

    pub(crate) async fn write_raw(
        &mut self,
        r#type: u16,
        code: u16,
        value: i32,
    ) -> Result<(), Error> {
        loop {
            let result = self.uinput.file().writable().await?.try_io(|_| {
                let ret = unsafe {
                    glue::libevdev_uinput_write_event(
                        self.uinput.as_ptr(),
                        r#type as _,
                        code as _,
                        value,
                    )
                };

                if ret < 0 {
                    return Err(Error::from_raw_os_error(-ret).into());
                }

                Ok(())
            });

            match result {
                Ok(result) => return result,
                Err(_) => continue, // This means it would block.
            }
        }
    }
}

fn key_state_events(current: &HashSet<Key>, desired: &HashSet<Key>) -> Vec<Event> {
    let mut released = current.difference(desired).copied().collect::<Vec<_>>();
    released.sort_by_key(|key| (key.is_modifier(), key.to_raw()));

    let mut pressed = desired.difference(current).copied().collect::<Vec<_>>();
    pressed.sort_by_key(|key| (!key.is_modifier(), key.to_raw()));

    let mut events = released
        .into_iter()
        .map(|key| Event::Key(KeyEvent { key, down: false }))
        .chain(
            pressed
                .into_iter()
                .map(|key| Event::Key(KeyEvent { key, down: true })),
        )
        .collect::<Vec<_>>();
    if !events.is_empty() {
        events.push(Event::Sync(crate::sync::SyncEvent::All));
    }
    events
}

pub struct WriterBuilder {
    evdev: Evdev,
}

impl WriterBuilder {
    pub fn new() -> Result<Self, Error> {
        let evdev = Evdev::new()?;

        unsafe {
            glue::libevdev_set_id_bustype(evdev.as_ptr(), glue::BUS_VIRTUAL as _);
        }

        Ok(Self { evdev })
    }

    pub fn name(self, name: &CStr) -> Self {
        unsafe {
            glue::libevdev_set_name(self.evdev.as_ptr(), name.as_ptr());
        }

        self
    }

    pub fn vendor(self, value: u16) -> Self {
        unsafe {
            glue::libevdev_set_id_vendor(self.evdev.as_ptr(), value as _);
        }

        self
    }

    pub fn product(self, value: u16) -> Self {
        unsafe {
            glue::libevdev_set_id_product(self.evdev.as_ptr(), value as _);
        }

        self
    }

    pub fn version(self, value: u16) -> Self {
        unsafe {
            glue::libevdev_set_id_version(self.evdev.as_ptr(), value as _);
        }

        self
    }

    pub fn rel<T: IntoIterator<Item = RelAxis>>(self, items: T) -> Result<Self, Error> {
        for axis in items {
            let axis = match axis.to_raw() {
                Some(axis) => axis,
                None => continue,
            };

            let ret = unsafe {
                glue::libevdev_enable_event_code(
                    self.evdev.as_ptr(),
                    glue::EV_REL,
                    axis as _,
                    ptr::null(),
                )
            };

            if ret < 0 {
                return Err(Error::from_raw_os_error(-ret));
            }
        }

        Ok(self)
    }

    pub fn abs<T: IntoIterator<Item = (AbsAxis, AbsInfo)>>(self, items: T) -> Result<Self, Error> {
        let ret = unsafe {
            glue::libevdev_enable_event_code(
                self.evdev.as_ptr(),
                glue::EV_SYN,
                glue::SYN_MT_REPORT,
                ptr::null(),
            )
        };

        if ret < 0 {
            return Err(Error::from_raw_os_error(-ret));
        }

        for (axis, info) in items {
            let code = match axis.to_raw() {
                Some(code) => code,
                None => continue,
            };

            let info = input_absinfo {
                value: info.min,
                minimum: info.min,
                maximum: info.max,
                fuzz: info.fuzz,
                flat: info.flat,
                resolution: info.resolution,
            };

            let ret = unsafe {
                glue::libevdev_enable_event_code(
                    self.evdev.as_ptr(),
                    glue::EV_ABS,
                    code as _,
                    &info as *const _ as *const _,
                )
            };

            if ret < 0 {
                return Err(Error::from_raw_os_error(-ret));
            }
        }

        Ok(self)
    }

    pub fn key<T: IntoIterator<Item = Key>>(self, items: T) -> Result<Self, Error> {
        for key in items {
            let key = match key.to_raw() {
                Some(key) => key,
                None => continue,
            };

            let ret = unsafe {
                glue::libevdev_enable_event_code(
                    self.evdev.as_ptr(),
                    glue::EV_KEY,
                    key as _,
                    ptr::null(),
                )
            };

            if ret < 0 {
                return Err(Error::from_raw_os_error(-ret));
            }
        }

        Ok(self)
    }

    pub fn delay(self, value: Option<i32>) -> Result<Self, Error> {
        let value: c_int = match value {
            Some(value) => value,
            None => return Ok(self),
        };

        let ret = unsafe {
            glue::libevdev_enable_event_code(
                self.evdev.as_ptr(),
                glue::EV_REP,
                glue::REP_DELAY,
                &value as *const _ as *const _,
            )
        };

        if ret < 0 {
            return Err(Error::from_raw_os_error(-ret));
        }

        Ok(self)
    }

    pub fn period(self, value: Option<i32>) -> Result<Self, Error> {
        let value: c_int = match value {
            Some(value) => value,
            None => return Ok(self),
        };

        let ret = unsafe {
            glue::libevdev_enable_event_code(
                self.evdev.as_ptr(),
                glue::EV_REP,
                glue::REP_PERIOD,
                &value as *const _ as *const _,
            )
        };

        if ret < 0 {
            return Err(Error::from_raw_os_error(-ret));
        }

        Ok(self)
    }

    pub async fn build(self) -> Result<Writer, Error> {
        Writer::from_evdev(&self.evdev)
            .await
            .map_err(|err| match err {
                CreateError::Open(err) | CreateError::Create(err) => err,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::Keyboard;
    use crate::sync::SyncEvent;

    fn key(key: Keyboard) -> Key {
        Key::Key(key)
    }

    #[test]
    fn raw_event_preserves_basic_event_order_data() {
        assert_eq!(
            raw_event(&Event::Rel(RelEvent {
                axis: RelAxis::X,
                value: 12,
            })),
            Some((glue::EV_REL as _, glue::REL_X as _, 12))
        );
        assert_eq!(
            raw_event(&Event::Key(KeyEvent {
                key: Key::Key(Keyboard::A),
                down: true,
            })),
            Some((glue::EV_KEY as _, glue::KEY_A as _, 1))
        );
        assert_eq!(
            raw_event(&Event::Sync(SyncEvent::All)),
            Some((glue::EV_SYN as _, glue::SYN_REPORT as _, 0))
        );
    }

    #[test]
    fn key_state_delta_releases_plain_keys_before_modifiers() {
        let current = [
            key(Keyboard::LeftMeta),
            key(Keyboard::LeftShift),
            key(Keyboard::Grave),
        ]
        .into_iter()
        .collect();

        let events = key_state_events(&current, &HashSet::new());

        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    Event::Key(event) => Some(*event),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                KeyEvent {
                    key: key(Keyboard::Grave),
                    down: false,
                },
                KeyEvent {
                    key: key(Keyboard::LeftShift),
                    down: false,
                },
                KeyEvent {
                    key: key(Keyboard::LeftMeta),
                    down: false,
                },
            ]
        );
        assert!(matches!(events.last(), Some(Event::Sync(SyncEvent::All))));
    }

    #[test]
    fn key_state_delta_presses_modifiers_before_plain_keys() {
        let desired = [
            key(Keyboard::A),
            key(Keyboard::LeftMeta),
            key(Keyboard::LeftShift),
        ]
        .into_iter()
        .collect();

        let events = key_state_events(&HashSet::new(), &desired);

        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    Event::Key(event) => Some(*event),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                KeyEvent {
                    key: key(Keyboard::LeftShift),
                    down: true,
                },
                KeyEvent {
                    key: key(Keyboard::LeftMeta),
                    down: true,
                },
                KeyEvent {
                    key: key(Keyboard::A),
                    down: true,
                },
            ]
        );
    }

    #[test]
    fn equal_key_states_need_no_events() {
        let state = [key(Keyboard::LeftCtrl)].into_iter().collect();
        assert!(key_state_events(&state, &state).is_empty());
    }
}
