use std::time::{SystemTime, UNIX_EPOCH};

/// 返回当前系统时间戳，mills
pub fn now_ts() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}