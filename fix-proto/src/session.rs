//! FIX session state machine.
//!
//! Manages the session lifecycle: logon, sequence numbers, heartbeats,
//! test requests, resend requests, and logout.

use crate::message::Message;

/// Current state of a FIX session.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionState {
    Disconnected,
    LogonSent,
    LogonReceived,
    Active,
    LogoutSent,
    LogoutReceived,
}

/// Configuration for a FIX session.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub begin_string: String,
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub heart_bt_int: u32,
    pub default_appl_ver_id: String,
}

/// Tracks session-level sequence numbers and timing.
#[derive(Clone, Debug)]
pub struct Session {
    pub state: SessionState,
    pub config: SessionConfig,

    /// Outgoing message sequence number (MsgSeqNum).
    pub outgoing_seq: u64,
    /// Incoming message sequence number (expected from peer).
    pub incoming_seq: u64,

    /// True if we initiated the connection.
    pub is_initiator: bool,
}

impl Session {
    pub fn new(config: SessionConfig, is_initiator: bool) -> Self {
        Self {
            state: SessionState::Disconnected,
            config,
            outgoing_seq: 1,
            incoming_seq: 1,
            is_initiator,
        }
    }

    /// Build a Logon message (MsgType "A").
    pub fn build_logon(&mut self) -> Message {
        let fields = vec![
            (crate::tags::BEGIN_STRING, self.config.begin_string.clone()),
            (crate::tags::MSG_TYPE, "A".to_string()),
            (
                crate::tags::SENDER_COMP_ID,
                self.config.sender_comp_id.clone(),
            ),
            (
                crate::tags::TARGET_COMP_ID,
                self.config.target_comp_id.clone(),
            ),
            (crate::tags::MSG_SEQ_NUM, self.outgoing_seq.to_string()),
            (
                crate::tags::SENDING_TIME,
                chrono::Utc::now().format("%Y%m%d-%H:%M:%S").to_string(),
            ),
            (crate::tags::ENCRYPT_METHOD, "0".to_string()),
            (
                crate::tags::HEART_BT_INT,
                self.config.heart_bt_int.to_string(),
            ),
            (
                crate::tags::DEFAULT_APPL_VER_ID,
                self.config.default_appl_ver_id.clone(),
            ),
        ];
        self.outgoing_seq += 1;
        self.state = SessionState::LogonSent;
        crate::message::build_message(fields)
    }

    /// Build a Heartbeat message (MsgType "0").
    pub fn build_heartbeat(&mut self) -> Message {
        let fields = vec![
            (crate::tags::BEGIN_STRING, self.config.begin_string.clone()),
            (crate::tags::MSG_TYPE, "0".to_string()),
            (
                crate::tags::SENDER_COMP_ID,
                self.config.sender_comp_id.clone(),
            ),
            (
                crate::tags::TARGET_COMP_ID,
                self.config.target_comp_id.clone(),
            ),
            (crate::tags::MSG_SEQ_NUM, self.outgoing_seq.to_string()),
            (
                crate::tags::SENDING_TIME,
                chrono::Utc::now().format("%Y%m%d-%H:%M:%S").to_string(),
            ),
        ];
        self.outgoing_seq += 1;
        crate::message::build_message(fields)
    }

    /// Build a TestRequest message (MsgType "1").
    pub fn build_test_request(&mut self, test_req_id: &str) -> Message {
        let fields = vec![
            (crate::tags::BEGIN_STRING, self.config.begin_string.clone()),
            (crate::tags::MSG_TYPE, "1".to_string()),
            (
                crate::tags::SENDER_COMP_ID,
                self.config.sender_comp_id.clone(),
            ),
            (
                crate::tags::TARGET_COMP_ID,
                self.config.target_comp_id.clone(),
            ),
            (crate::tags::MSG_SEQ_NUM, self.outgoing_seq.to_string()),
            (
                crate::tags::SENDING_TIME,
                chrono::Utc::now().format("%Y%m%d-%H:%M:%S").to_string(),
            ),
            (crate::tags::TEST_REQ_ID, test_req_id.to_string()),
        ];
        self.outgoing_seq += 1;
        crate::message::build_message(fields)
    }

    /// Build a Logout message (MsgType "5").
    pub fn build_logout(&mut self) -> Message {
        let fields = vec![
            (crate::tags::BEGIN_STRING, self.config.begin_string.clone()),
            (crate::tags::MSG_TYPE, "5".to_string()),
            (
                crate::tags::SENDER_COMP_ID,
                self.config.sender_comp_id.clone(),
            ),
            (
                crate::tags::TARGET_COMP_ID,
                self.config.target_comp_id.clone(),
            ),
            (crate::tags::MSG_SEQ_NUM, self.outgoing_seq.to_string()),
            (
                crate::tags::SENDING_TIME,
                chrono::Utc::now().format("%Y%m%d-%H:%M:%S").to_string(),
            ),
        ];
        self.outgoing_seq += 1;
        self.state = SessionState::LogoutSent;
        crate::message::build_message(fields)
    }

    /// Process an incoming message and update session state.
    ///
    /// Returns an action the caller should take (e.g. send heartbeat, logon response).
    pub fn receive(&mut self, msg: &Message) -> Result<SessionEvent, crate::Error> {
        self.incoming_seq += 1;

        match msg.msg_type() {
            Some(crate::types::MsgType::Logon) => {
                self.state = SessionState::Active;
                Ok(SessionEvent::LogonReceived)
            }
            Some(crate::types::MsgType::Heartbeat) => Ok(SessionEvent::HeartbeatReceived),
            Some(crate::types::MsgType::TestRequest) => {
                // Must respond with Heartbeat containing the TestReqID
                Ok(SessionEvent::RespondHeartbeat)
            }
            Some(crate::types::MsgType::Logout) => {
                self.state = SessionState::LogoutReceived;
                Ok(SessionEvent::LogoutReceived)
            }
            Some(crate::types::MsgType::SequenceReset) => {
                // Gap fill / sequence reset
                if let Some(new_seq) = msg.get(crate::tags::LAST_MSG_SEQ_NUM) {
                    if let Ok(n) = new_seq.parse::<u64>() {
                        self.incoming_seq = n + 1;
                    }
                }
                Ok(SessionEvent::SequenceReset)
            }
            Some(crate::types::MsgType::ResendRequest) => Ok(SessionEvent::ResendRequested),
            Some(crate::types::MsgType::Reject) => Ok(SessionEvent::MessageRejected),
            // Application messages pass through
            _ => Ok(SessionEvent::ApplicationMessage(msg.clone())),
        }
    }
}

/// Actions the session layer may need to take in response to an incoming message.
#[derive(Clone, Debug)]
pub enum SessionEvent {
    LogonReceived,
    HeartbeatReceived,
    RespondHeartbeat,
    LogoutReceived,
    SequenceReset,
    ResendRequested,
    MessageRejected,
    ApplicationMessage(Message),
}
