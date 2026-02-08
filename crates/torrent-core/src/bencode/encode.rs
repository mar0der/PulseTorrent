use super::Value;

/// Encode a Value into bencoded bytes.
pub fn encode(value: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_into(value, &mut buf);
    buf
}

fn encode_into(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::Bytes(b) => {
            buf.extend_from_slice(b.len().to_string().as_bytes());
            buf.push(b':');
            buf.extend_from_slice(b);
        }
        Value::Int(i) => {
            buf.push(b'i');
            buf.extend_from_slice(i.to_string().as_bytes());
            buf.push(b'e');
        }
        Value::List(items) => {
            buf.push(b'l');
            for item in items {
                encode_into(item, buf);
            }
            buf.push(b'e');
        }
        Value::Dict(map) => {
            buf.push(b'd');
            // BTreeMap iterates in sorted order, which is required by bencode spec
            for (key, value) in map {
                buf.extend_from_slice(key.len().to_string().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(key);
                encode_into(value, buf);
            }
            buf.push(b'e');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode;
    use std::collections::BTreeMap;

    #[test]
    fn test_encode_int() {
        let encoded = encode(&Value::Int(42));
        assert_eq!(encoded, b"i42e");
    }

    #[test]
    fn test_encode_string() {
        let encoded = encode(&Value::Bytes(b"spam".to_vec()));
        assert_eq!(encoded, b"4:spam");
    }

    #[test]
    fn test_encode_list() {
        let encoded = encode(&Value::List(vec![
            Value::Bytes(b"spam".to_vec()),
            Value::Bytes(b"eggs".to_vec()),
        ]));
        assert_eq!(encoded, b"l4:spam4:eggse");
    }

    #[test]
    fn test_encode_dict() {
        let mut map = BTreeMap::new();
        map.insert(b"cow".to_vec(), Value::Bytes(b"moo".to_vec()));
        map.insert(b"spam".to_vec(), Value::Bytes(b"eggs".to_vec()));
        let encoded = encode(&Value::Dict(map));
        assert_eq!(encoded, b"d3:cow3:moo4:spam4:eggse");
    }

    #[test]
    fn test_roundtrip() {
        let mut map = BTreeMap::new();
        map.insert(b"key".to_vec(), Value::Int(123));
        map.insert(
            b"list".to_vec(),
            Value::List(vec![Value::Bytes(b"hello".to_vec()), Value::Int(-1)]),
        );
        let original = Value::Dict(map);
        let encoded = encode(&original);
        let (decoded, _) = bencode::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }
}
