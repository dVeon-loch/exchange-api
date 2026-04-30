//! FIX tag number constants.
//!
//! These are the standard tag numbers defined by the FIX protocol.
//! Only the most commonly used tags are listed here; exchange-specific
//! custom tags (e.g. for Binance) are added on top.

#![allow(dead_code)]

pub const BEGIN_STRING: u32 = 8;
pub const BODY_LENGTH: u32 = 9;
pub const MSG_TYPE: u32 = 35;
pub const SENDER_COMP_ID: u32 = 49;
pub const TARGET_COMP_ID: u32 = 56;
pub const MSG_SEQ_NUM: u32 = 34;
pub const SENDING_TIME: u32 = 52;
pub const POSS_DUP_FLAG: u32 = 43;
pub const LAST_MSG_SEQ_NUM: u32 = 369;

pub const ENCRYPT_METHOD: u32 = 98;
pub const HEART_BT_INT: u32 = 108;
pub const RESET_SEQ_NUM_FLAG: u32 = 141;
pub const DEFAULT_APPL_VER_ID: u32 = 1137;
pub const TEST_REQ_ID: u32 = 112;
pub const REF_MSG_TYPE: u32 = 372;
pub const REF_SEQ_NUM: u32 = 45;
pub const SESSION_STATUS: u32 = 1409;
pub const TRAD_SES_STATUS: u32 = 340;

pub const SYMBOL: u32 = 55;
pub const SIDE: u32 = 54;
pub const ORDER_QTY: u32 = 38;
pub const ORD_TYPE: u32 = 40;
pub const PRICE: u32 = 44;
pub const STOP_PX: u32 = 99;
pub const TIME_IN_FORCE: u32 = 59;
pub const EXEC_INST: u32 = 18;
pub const MAX_FLOOR: u32 = 111;

pub const ORDER_ID: u32 = 37;
pub const EXEC_ID: u32 = 17;
pub const EXEC_TYPE: u32 = 150;
pub const ORD_STATUS: u32 = 39;
pub const LEAVES_QTY: u32 = 151;
pub const CUM_QTY: u32 = 14;
pub const AVG_PX: u32 = 6;
pub const LAST_PX: u32 = 31;
pub const LAST_QTY: u32 = 32;
pub const TRADE_DATE: u32 = 75;
pub const TRANSACT_TIME: u32 = 60;

pub const MD_REQ_ID: u32 = 262;
pub const MD_ENTRY_TYPE: u32 = 269;
pub const MD_ENTRY_PX: u32 = 270;
pub const MD_ENTRY_SIZE: u32 = 271;
pub const NO_MD_ENTRIES: u32 = 268;
pub const MD_REQ_REUSE: u32 = 1608;

pub const CHECK_SUM: u32 = 10;
