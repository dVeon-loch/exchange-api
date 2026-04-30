//! FIX data types and enumerations.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
    SellShort,
}

impl Side {
    pub fn from_fix(val: char) -> Option<Self> {
        match val {
            '1' => Some(Self::Buy),
            '2' => Some(Self::Sell),
            '5' => Some(Self::SellShort),
            _ => None,
        }
    }

    pub fn to_fix(&self) -> char {
        match self {
            Self::Buy => '1',
            Self::Sell => '2',
            Self::SellShort => '5',
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum OrdType {
    Market,
    Limit,
    Stop,
    StopLimit,
    MarketIfTouched,
}

impl OrdType {
    pub fn from_fix(val: char) -> Option<Self> {
        match val {
            '1' => Some(Self::Market),
            '2' => Some(Self::Limit),
            '3' => Some(Self::Stop),
            '4' => Some(Self::StopLimit),
            'K' => Some(Self::MarketIfTouched),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum TimeInForce {
    Day,
    GoodTillCancel,
    ImmediateOrCancel,
    FillOrKill,
}

impl TimeInForce {
    pub fn from_fix(val: char) -> Option<Self> {
        match val {
            '0' => Some(Self::Day),
            '1' => Some(Self::GoodTillCancel),
            '3' => Some(Self::ImmediateOrCancel),
            '4' => Some(Self::FillOrKill),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ExecType {
    New,
    PartialFill,
    Fill,
    DoneForDay,
    Cancelled,
    Replaced,
    PendingCancel,
    Rejected,
    Suspended,
    PendingNew,
    Expired,
    Trade,
    OrderStatus,
}

impl ExecType {
    pub fn from_fix(val: char) -> Option<Self> {
        match val {
            '0' => Some(Self::New),
            '1' => Some(Self::PartialFill),
            '2' => Some(Self::Fill),
            '3' => Some(Self::DoneForDay),
            '4' => Some(Self::Cancelled),
            '5' => Some(Self::Replaced),
            '6' => Some(Self::PendingCancel),
            '8' => Some(Self::Rejected),
            '9' => Some(Self::Suspended),
            'A' => Some(Self::PendingNew),
            'C' => Some(Self::Expired),
            'F' => Some(Self::Trade),
            'I' => Some(Self::OrderStatus),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum OrdStatus {
    New,
    PartiallyFilled,
    Filled,
    DoneForDay,
    Cancelled,
    Replaced,
    PendingCancel,
    Stopped,
    Rejected,
    Suspended,
    PendingNew,
    Expired,
    PendingReplace,
}

impl OrdStatus {
    pub fn from_fix(val: char) -> Option<Self> {
        match val {
            '0' => Some(Self::New),
            '1' => Some(Self::PartiallyFilled),
            '2' => Some(Self::Filled),
            '3' => Some(Self::DoneForDay),
            '4' => Some(Self::Cancelled),
            '5' => Some(Self::Replaced),
            '6' => Some(Self::PendingCancel),
            '7' => Some(Self::Stopped),
            '8' => Some(Self::Rejected),
            '9' => Some(Self::Suspended),
            'A' => Some(Self::PendingNew),
            'C' => Some(Self::Expired),
            'D' => Some(Self::PendingReplace),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum MDEntryType {
    Bid,
    Offer,
    Trade,
    OpeningPrice,
    ClosingPrice,
}

impl MDEntryType {
    pub fn from_fix(val: char) -> Option<Self> {
        match val {
            '0' => Some(Self::Bid),
            '1' => Some(Self::Offer),
            '2' => Some(Self::Trade),
            '4' => Some(Self::OpeningPrice),
            '5' => Some(Self::ClosingPrice),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum MsgType {
    Heartbeat,
    TestRequest,
    ResendRequest,
    Reject,
    SequenceReset,
    Logout,
    Logon,
    NewOrderSingle,
    ExecutionReport,
    OrderCancelRequest,
    OrderCancelReject,
    MarketDataRequest,
    MarketDataSnapshot,
    MarketDataIncremental,
    Unknown(String),
}

impl MsgType {
    pub fn from_fix(val: &str) -> Self {
        match val {
            "0" => Self::Heartbeat,
            "1" => Self::TestRequest,
            "2" => Self::ResendRequest,
            "3" => Self::Reject,
            "4" => Self::SequenceReset,
            "5" => Self::Logout,
            "A" => Self::Logon,
            "D" => Self::NewOrderSingle,
            "8" => Self::ExecutionReport,
            "F" => Self::OrderCancelRequest,
            "9" => Self::OrderCancelReject,
            "V" => Self::MarketDataRequest,
            "W" => Self::MarketDataSnapshot,
            "X" => Self::MarketDataIncremental,
            other => Self::Unknown(other.to_string()),
        }
    }

    pub fn to_fix(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::Heartbeat => "0".into(),
            Self::TestRequest => "1".into(),
            Self::ResendRequest => "2".into(),
            Self::Reject => "3".into(),
            Self::SequenceReset => "4".into(),
            Self::Logout => "5".into(),
            Self::Logon => "A".into(),
            Self::NewOrderSingle => "D".into(),
            Self::ExecutionReport => "8".into(),
            Self::OrderCancelRequest => "F".into(),
            Self::OrderCancelReject => "9".into(),
            Self::MarketDataRequest => "V".into(),
            Self::MarketDataSnapshot => "W".into(),
            Self::MarketDataIncremental => "X".into(),
            Self::Unknown(s) => std::borrow::Cow::Owned(s.clone()),
        }
    }
}
