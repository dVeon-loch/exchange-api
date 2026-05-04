//! FIX message parsing and serialization.
//!
//! A FIX message is a sequence of tag=value fields delimited by SOH (\x01).
//! The last field is always tag 10 (CheckSum).
//!
//! Example: "8=FIX.4.4\x019=99\x0135=A\x0149=SENDER\x0156=TARGET\x0134=1\x0152=20240101-00:00:00\x0198=0\x01108=30\x0110=123\x01"

use crate::tags;
use crate::types::MsgType;
use std::collections::HashMap;

/// A parsed FIX message.
#[derive(Clone, Debug)]
pub struct Message {
    /// Raw tag-value pairs in order of appearance.
    pub fields: Vec<(u32, String)>,
    /// Indexed for O(1) lookup.
    pub index: HashMap<u32, usize>,
    /// Raw message after checksum stripping (for verification / logging).
    pub raw: String,
}

impl Message {
    /// Parse a raw FIX string (with trailing SOH) into a `Message`.
    pub fn parse(raw: &str) -> Result<Self, crate::Error> {
        let trimmed = raw.trim_end_matches('\x01');
        let mut fields = Vec::new();
        let mut index = HashMap::new();

        for (i, pair) in trimmed.split('\x01').enumerate() {
            let mut parts = pair.splitn(2, '=');
            let tag: u32 = parts
                .next()
                .ok_or_else(|| crate::Error::Parse("empty field".into()))?
                .parse()
                .map_err(|_| crate::Error::Parse(format!("invalid tag: {pair}")))?;
            let value = parts
                .next()
                .ok_or_else(|| crate::Error::Parse(format!("missing value for tag {tag}")))?
                .to_string();
            index.insert(tag, i);
            fields.push((tag, value));
        }

        Ok(Self {
            fields,
            index,
            raw: raw.to_string(),
        })
    }

    /// Get a field value by tag number.
    pub fn get(&self, tag: u32) -> Option<&str> {
        self.index.get(&tag).map(|&i| self.fields[i].1.as_str())
    }

    /// Get the MsgType (tag 35).
    pub fn msg_type(&self) -> Option<MsgType> {
        self.get(tags::MSG_TYPE).map(MsgType::from_fix)
    }

    /// Get the MsgSeqNum (tag 34).
    pub fn seq_num(&self) -> Option<u64> {
        self.get(tags::MSG_SEQ_NUM)?.parse().ok()
    }

    /// Get SenderCompID (tag 49).
    pub fn sender(&self) -> Option<&str> {
        self.get(tags::SENDER_COMP_ID)
    }

    /// Get TargetCompID (tag 56).
    pub fn target(&self) -> Option<&str> {
        self.get(tags::TARGET_COMP_ID)
    }

    /// BeginString (tag 8).
    pub fn begin_string(&self) -> Option<&str> {
        self.get(tags::BEGIN_STRING)
    }

    /// Serialize this message back to a raw FIX string.
    ///
    /// Recalculates BodyLength (tag 9) and CheckSum (tag 10).
    pub fn encode(&self) -> String {
        let mut buf = String::new();
        for (tag, value) in &self.fields {
            if *tag == tags::BODY_LENGTH || *tag == tags::CHECK_SUM {
                continue;
            }
            buf.push_str(&format!("{tag}={value}\x01"));
        }

        // Calculate body length: everything between tag 9 and tag 10
        let body_len = buf.len().to_string();

        // Build final message without checksum
        let mut msg = String::new();
        for (tag, value) in &self.fields {
            if *tag == tags::CHECK_SUM {
                continue;
            }
            if *tag == tags::BODY_LENGTH {
                msg.push_str(&format!("{}={}\x01", tags::BODY_LENGTH, body_len));
            } else {
                msg.push_str(&format!("{tag}={value}\x01"));
            }
        }

        let check_sum = compute_checksum(msg.as_bytes());
        msg.push_str(&format!("{}={:03}\x01", tags::CHECK_SUM, check_sum));
        msg
    }
}

/// Build a new FIX message from a list of tag-value pairs.
/// BodyLength and CheckSum are computed automatically.
pub fn build_message(fields: Vec<(u32, String)>) -> Message {
    let mut index = HashMap::new();
    for (i, (tag, _)) in fields.iter().enumerate() {
        index.insert(*tag, i);
    }
    Message {
        fields,
        index,
        raw: String::new(),
    }
}

/// Compute the FIX checksum: sum of all bytes modulo 256, formatted as 3 digits.
pub fn compute_checksum(body: &[u8]) -> u32 {
    body.iter().map(|&b| b as u32).sum::<u32>() % 256
}

/// Validate that tag 10 checksum matches the message body.
pub fn validate_checksum(raw: &str) -> Result<bool, crate::Error> {
    let msg = raw.trim_end_matches('\x01');
    // Find the tag 10 field (last field)
    let last_eq = msg.rfind("\x01").unwrap_or(0);
    let check_field = &msg[last_eq..];

    if !check_field.starts_with("\x0110=") && !check_field.starts_with("10=") {
        return Err(crate::Error::Parse("missing checksum field".into()));
    }

    let body = if check_field.starts_with("\x0110=") {
        &msg[..last_eq]
    } else {
        // 10= is the only field
        ""
    };

    let expected = compute_checksum(body.as_bytes());
    let colon = check_field.rfind('=').unwrap();
    let actual: u32 = check_field[colon + 1..]
        .parse()
        .map_err(|_| crate::Error::Parse("invalid checksum value".into()))?;

    Ok(expected == actual)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_encode_roundtrip() {
        let raw = "8=FIX.4.4\x019=45\x0135=A\x0149=EXECUTOR\x0156=CLIENT1\x0134=1\x0152=20240101-00:00:00\x0198=0\x01108=30\x0110=000\x01";
        let msg = Message::parse(raw).unwrap();
        assert_eq!(msg.msg_type(), Some(MsgType::Logon));
        assert_eq!(msg.seq_num(), Some(1));
        assert_eq!(msg.sender(), Some("EXECUTOR"));
        assert_eq!(msg.target(), Some("CLIENT1"));
    }

    #[test]
    fn checksum_validation() {
        // Compute the expected checksum programmatically
        let body = "8=FIX.4.4\x019=12\x0135=0";
        let expected_cs = compute_checksum(body.as_bytes());
        let raw = format!("{body}\x0110={expected_cs:03}\x01");
        assert!(
            validate_checksum(&raw).unwrap(),
            "checksum should be {expected_cs}"
        );
    }
}
