use super::Value;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid byte at position {0}: {1:#04x}")]
    InvalidByte(usize, u8),
    #[error("invalid integer encoding")]
    InvalidInt,
    #[error("leading zeros in integer")]
    LeadingZeros,
    #[error("negative zero")]
    NegativeZero,
    #[error("invalid string length")]
    InvalidStringLen,
    #[error("trailing data after bencoded value")]
    TrailingData,
}

struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn peek(&self) -> Result<u8, DecodeError> {
        self.data.get(self.pos).copied().ok_or(DecodeError::UnexpectedEof)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn expect(&mut self, byte: u8) -> Result<(), DecodeError> {
        if self.peek()? == byte {
            self.advance();
            Ok(())
        } else {
            Err(DecodeError::InvalidByte(self.pos, self.peek().unwrap()))
        }
    }

    fn decode_value(&mut self) -> Result<Value, DecodeError> {
        match self.peek()? {
            b'i' => self.decode_int(),
            b'l' => self.decode_list(),
            b'd' => self.decode_dict(),
            b'0'..=b'9' => self.decode_bytes(),
            b => Err(DecodeError::InvalidByte(self.pos, b)),
        }
    }

    fn decode_int(&mut self) -> Result<Value, DecodeError> {
        self.expect(b'i')?;
        let start = self.pos;

        // Find the 'e' terminator
        while self.peek()? != b'e' {
            self.advance();
        }

        let num_str = std::str::from_utf8(&self.data[start..self.pos])
            .map_err(|_| DecodeError::InvalidInt)?;

        // Validate: no leading zeros (except "0" itself), no negative zero
        if num_str.len() > 1 && num_str.starts_with('0') {
            return Err(DecodeError::LeadingZeros);
        }
        if num_str.len() > 2 && num_str.starts_with("-0") {
            return Err(DecodeError::LeadingZeros);
        }
        if num_str == "-0" {
            return Err(DecodeError::NegativeZero);
        }

        let value: i64 = num_str.parse().map_err(|_| DecodeError::InvalidInt)?;
        self.expect(b'e')?;
        Ok(Value::Int(value))
    }

    fn decode_bytes(&mut self) -> Result<Value, DecodeError> {
        let start = self.pos;

        while self.peek()? != b':' {
            self.advance();
        }

        let len_str = std::str::from_utf8(&self.data[start..self.pos])
            .map_err(|_| DecodeError::InvalidStringLen)?;
        let len: usize = len_str.parse().map_err(|_| DecodeError::InvalidStringLen)?;

        self.advance(); // skip ':'

        if self.pos + len > self.data.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(Value::Bytes(bytes))
    }

    fn decode_list(&mut self) -> Result<Value, DecodeError> {
        self.expect(b'l')?;
        let mut items = Vec::new();

        while self.peek()? != b'e' {
            items.push(self.decode_value()?);
        }

        self.expect(b'e')?;
        Ok(Value::List(items))
    }

    fn decode_dict(&mut self) -> Result<Value, DecodeError> {
        self.expect(b'd')?;
        let mut map = BTreeMap::new();

        while self.peek()? != b'e' {
            let key = self.decode_bytes()?;
            let key_bytes = match key {
                Value::Bytes(b) => b,
                _ => return Err(DecodeError::InvalidByte(self.pos, 0)),
            };
            let value = self.decode_value()?;
            map.insert(key_bytes, value);
        }

        self.expect(b'e')?;
        Ok(Value::Dict(map))
    }
}

/// Decode bencoded bytes into a Value. Returns the value and the number of bytes consumed.
pub fn decode(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    let mut decoder = Decoder::new(data);
    let value = decoder.decode_value()?;
    Ok((value, decoder.pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_int() {
        let (val, _) = decode(b"i42e").unwrap();
        assert_eq!(val.as_int(), Some(42));
    }

    #[test]
    fn test_decode_negative_int() {
        let (val, _) = decode(b"i-5e").unwrap();
        assert_eq!(val.as_int(), Some(-5));
    }

    #[test]
    fn test_decode_zero() {
        let (val, _) = decode(b"i0e").unwrap();
        assert_eq!(val.as_int(), Some(0));
    }

    #[test]
    fn test_decode_negative_zero_fails() {
        assert!(decode(b"i-0e").is_err());
    }

    #[test]
    fn test_decode_leading_zero_fails() {
        assert!(decode(b"i03e").is_err());
    }

    #[test]
    fn test_decode_string() {
        let (val, _) = decode(b"4:spam").unwrap();
        assert_eq!(val.as_str(), Some("spam"));
    }

    #[test]
    fn test_decode_empty_string() {
        let (val, _) = decode(b"0:").unwrap();
        assert_eq!(val.as_str(), Some(""));
    }

    #[test]
    fn test_decode_list() {
        let (val, _) = decode(b"l4:spam4:eggse").unwrap();
        let list = val.as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].as_str(), Some("spam"));
        assert_eq!(list[1].as_str(), Some("eggs"));
    }

    #[test]
    fn test_decode_dict() {
        let (val, _) = decode(b"d3:cow3:moo4:spam4:eggse").unwrap();
        assert_eq!(val.get("cow").unwrap().as_str(), Some("moo"));
        assert_eq!(val.get("spam").unwrap().as_str(), Some("eggs"));
    }

    #[test]
    fn test_decode_nested() {
        let (val, _) = decode(b"d4:listli1ei2ei3ee4:name4:teste").unwrap();
        let list = val.get("list").unwrap().as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(val.get("name").unwrap().as_str(), Some("test"));
    }
}
