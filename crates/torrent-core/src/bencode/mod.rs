mod decode;
mod encode;

pub use decode::decode;
pub use encode::encode;

use std::collections::BTreeMap;
use std::fmt;

/// Represents a bencoded value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Byte string (not necessarily valid UTF-8).
    Bytes(Vec<u8>),
    /// Integer.
    Int(i64),
    /// Ordered list of values.
    List(Vec<Value>),
    /// Dictionary with sorted byte-string keys.
    Dict(BTreeMap<Vec<u8>, Value>),
}

impl Value {
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        match self {
            Value::Dict(d) => Some(d),
            _ => None,
        }
    }

    /// Lookup a key in a dictionary value.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_dict().and_then(|d| d.get(key.as_bytes()))
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bytes(b) => match std::str::from_utf8(b) {
                Ok(s) => write!(f, "\"{}\"", s),
                Err(_) => write!(f, "<{} bytes>", b.len()),
            },
            Value::Int(i) => write!(f, "{}", i),
            Value::List(l) => {
                write!(f, "[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Dict(d) => {
                write!(f, "{{")?;
                for (i, (k, v)) in d.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    match std::str::from_utf8(k) {
                        Ok(s) => write!(f, "\"{}\": {}", s, v)?,
                        Err(_) => write!(f, "<{} bytes>: {}", k.len(), v)?,
                    }
                }
                write!(f, "}}")
            }
        }
    }
}
