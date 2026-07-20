mod button;
mod keyboard;

pub use button::Button;
pub use keyboard::Keyboard;

use crate::convert::Convert;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct KeyEvent {
    pub key: Key,
    pub down: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Key {
    Key(Keyboard),
    Button(Button),
}

impl Key {
    pub fn is_modifier(self) -> bool {
        matches!(
            self,
            Self::Key(
                Keyboard::LeftAlt
                    | Keyboard::LeftCtrl
                    | Keyboard::LeftMeta
                    | Keyboard::LeftShift
                    | Keyboard::RightAlt
                    | Keyboard::RightCtrl
                    | Keyboard::RightMeta
                    | Keyboard::RightShift
            )
        )
    }
}

impl Convert for Key {
    type Raw = u16;

    fn from_raw(code: Self::Raw) -> Option<Self> {
        if let Some(key) = Keyboard::from_raw(code) {
            return Some(Self::Key(key));
        }

        if let Some(button) = Button::from_raw(code) {
            return Some(Self::Button(button));
        }

        None
    }

    fn to_raw(&self) -> Option<u16> {
        match self {
            Self::Key(key) => key.to_raw(),
            Self::Button(button) => button.to_raw(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_keyboard_modifiers() {
        for modifier in [
            Keyboard::LeftAlt,
            Keyboard::LeftCtrl,
            Keyboard::LeftMeta,
            Keyboard::LeftShift,
            Keyboard::RightAlt,
            Keyboard::RightCtrl,
            Keyboard::RightMeta,
            Keyboard::RightShift,
        ] {
            assert!(Key::Key(modifier).is_modifier());
        }

        assert!(!Key::Key(Keyboard::CapsLock).is_modifier());
        assert!(!Key::Key(Keyboard::A).is_modifier());
        assert!(!Key::Button(Button::Left).is_modifier());
    }
}
