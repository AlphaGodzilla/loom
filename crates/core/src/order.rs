use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use anyhow::anyhow;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::order::OrderAction::{CANCEL, PLACE};
use crate::order::OrderState::{CANCELED, FULL_FILLED, INIT, LIVE, PARTIAL_CANCELLED, PARTIAL_FILLED};
use crate::order::OrderTimeInForce::{FOK, GTC, IOC};
use crate::order::OrderType::{LIMIT, MARKET};
use crate::order::TradeSide::{BUY, SELL};

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum TradeSide {
    SELL,
    BUY,
}

impl Display for TradeSide {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SELL => write!(f, "SELL"),
            BUY => write!(f, "BUY")
        }
    }
}

impl FromStr for TradeSide {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "SELL" => Ok(SELL),
            "BUY" => Ok(BUY),
            _ => Err(anyhow!("no match TradeSide value={}",s ))
        }
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum OrderType {
    MARKET,
    LIMIT,
}

impl Display for OrderType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MARKET => write!(f, "MARKET"),
            LIMIT => write!(f, "LIMIT")
        }
    }
}

impl FromStr for OrderType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "MARKET" => Ok(MARKET),
            "LIMIT" => Ok(LIMIT),
            _ => Err(anyhow!("no match OrderType value={}", s))
        }
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum OrderState {
    INIT,
    LIVE,
    PARTIAL_FILLED,
    PARTIAL_CANCELLED,
    FULL_FILLED,
    CANCELED,
}

impl OrderState {
    pub fn del_flag(&self) -> bool {
        match self {
            INIT => false,
            LIVE => false,
            PARTIAL_FILLED => false,
            PARTIAL_CANCELLED => true,
            FULL_FILLED => true,
            CANCELED => true
        }
    }
}

impl Display for OrderState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            INIT => write!(f, "INIT"),
            LIVE => write!(f, "LIVE"),
            PARTIAL_FILLED => write!(f, "PARTIAL_FILLED"),
            PARTIAL_CANCELLED => write!(f, "PARTIAL_CANCELLED"),
            FULL_FILLED => write!(f, "FULL_FILLED"),
            CANCELED => write!(f, "CANCELED"),
        }
    }
}
impl FromStr for OrderState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "INIT" => Ok(INIT),
            "LIVE" => Ok(LIVE),
            "PARTIAL_FILLED" => Ok(PARTIAL_FILLED),
            "PARTIAL_CANCELLED" => Ok(PARTIAL_CANCELLED),
            "FULL_FILLED" => Ok(FULL_FILLED),
            "CANCELED" => Ok(CANCELED),
            _ => Err(anyhow!("no match OrderState value={}", s))
        }
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum OrderTimeInForce {
    GTC,
    IOC,
    FOK,
}
impl Display for OrderTimeInForce {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GTC => write!(f, "GTC"),
            IOC => write!(f, "IOC"),
            FOK => write!(f, "FOK"),
        }
    }
}
impl FromStr for OrderTimeInForce {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GTC" => Ok(GTC),
            "IOC" => Ok(IOC),
            "FOK" => Ok(FOK),
            _ => Err(anyhow!("no match OrderTimeInForce value={}", s))
        }
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum OrderAction {
    PLACE,
    CANCEL,
}
impl Display for OrderAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PLACE => write!(f, "PLACE"),
            CANCEL => write!(f, "CANCEL")
        }
    }
}
impl FromStr for OrderAction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PLACE" => Ok(PLACE),
            "CANCEL" => Ok(CANCEL),
            _ => Err(anyhow!("no match OrderTimeInForce value={}", s))
        }
    }
}

/// 委托订单结构体
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Order {
    /// 订单序列号
    pub id: u64,
    /// 交易对
    pub symbol: String,
    /// 交易方向
    pub side: TradeSide,
    /// 委托数量
    pub qty: u64,
    /// 委托价格
    pub price: BigDecimal,
    /// 累计成交数量
    pub acc_fill_qty: u64,
    /// 订单类型
    pub ord_type: OrderType,
    /// 订单时间戳
    pub ts: u128,
    /// 订单更新时间
    pub update_ts: u128,
    /// 订单撮合状态
    pub state: OrderState,
    /// 订单撮合行为
    pub tif: OrderTimeInForce,
    /// 订单动作
    pub action: OrderAction,
}

// unsafe impl Send for Order {}

impl Order {
    pub fn from_map(map: &HashMap<String, String>) -> anyhow::Result<Order> {
        Ok(Order {
            id: map.get("id").unwrap().parse()?,
            symbol: map.get("symbol").unwrap().to_string(),
            side: map.get("side").unwrap().parse()?,
            qty: map.get("qty").unwrap().parse()?,
            price: BigDecimal::from_str(&map.get("price").unwrap())?,
            acc_fill_qty: map.get("acc_fill_qty").unwrap().parse()?,
            ord_type: map.get("ord_type").unwrap().parse()?,
            ts: map.get("ts").unwrap().parse()?,
            update_ts: map.get("update_ts").unwrap().parse()?,
            state: map.get("state").unwrap().parse()?,
            tif: map.get("tif").unwrap().parse()?,
            action: map.get("action").unwrap().parse()?,
        })
    }

    /// 订单剩余未撮合的数量
    pub fn remain(&self) -> u64 {
        return self.qty - self.acc_fill_qty;
    }

    /// 当前订单是否可与传入的订单撮合
    pub fn can_trade(&self, order: &Order) -> bool {
        if self.side == order.side {
            // same side can't trade
            return false;
        }
        if self.ord_type == OrderType::MARKET {
            // 如果为市价单，跳过比价可以直接成交
            return true;
        }
        match self.side {
            TradeSide::BUY => {
                // buy price >= sell price can trade
                self.price >= order.price
            }
            TradeSide::SELL => {
                // sell price <= buy price can trade
                self.price <= order.price
            }
        }
    }

    /// 填充撮合结果
    pub fn fill(&mut self, filled_qty: u64, state: OrderState) {
        self.state = state;
        self.acc_fill_qty += filled_qty;
    }
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub struct OrderKey {
    /// 订单序列
    pub sequence_id: u64,
    /// 订单价格
    pub price: BigDecimal,
    /// 交易方向
    pub side: TradeSide,
}

impl PartialOrd<Self> for OrderKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderKey {
    // buy side price high -> low
    // sell side price low -> high
    fn cmp(&self, other: &Self) -> Ordering {
        if self.side != other.side {
            panic!("order side mismatch!")
        }
        let ordering: Ordering;
        match other.side {
            TradeSide::BUY => {
                ordering = other.price.cmp(&self.price);
            }
            TradeSide::SELL => {
                ordering = self.price.cmp(&other.price);
            }
        }
        match ordering {
            Ordering::Equal => {
                // if price eq else cmp sequence
                self.sequence_id.cmp(&other.sequence_id)
            }
            ordering => ordering,
        }
    }
}

impl OrderKey {
    pub fn new(order: &Order) -> OrderKey {
        OrderKey {
            sequence_id: order.id,
            price: order.price.clone(),
            side: order.side.clone(),
        }
    }
}

#[cfg(test)]
mod order_key_test {
    use std::cmp::Ordering;

    use bigdecimal::BigDecimal;

    use crate::order::OrderKey;
    use crate::order::TradeSide;

    #[test]
    fn order_key_buy_test() {
        let o1 = OrderKey {
            sequence_id: 0,
            price: BigDecimal::from(0),
            side: TradeSide::BUY,
        };
        let o2 = OrderKey {
            sequence_id: 1,
            price: BigDecimal::from(1),
            side: TradeSide::BUY,
        };
        let o3 = OrderKey {
            sequence_id: 2,
            price: BigDecimal::from(2),
            side: TradeSide::BUY,
        };
        // 期望排序：o3,o2,o1
        assert_eq!(o3.cmp(&o2), Ordering::Less);
        assert_eq!(o2.cmp(&o1), Ordering::Less);
        assert_eq!(o3.cmp(&o1), Ordering::Less);
    }

    #[test]
    fn order_key_sell_test() {
        let o1 = OrderKey {
            sequence_id: 0,
            price: BigDecimal::from(0),
            side: TradeSide::SELL,
        };
        let o2 = OrderKey {
            sequence_id: 1,
            price: BigDecimal::from(1),
            side: TradeSide::SELL,
        };
        let o3 = OrderKey {
            sequence_id: 2,
            price: BigDecimal::from(2),
            side: TradeSide::SELL,
        };
        // 期望排序：o1,o2,o3
        assert_eq!(o3.cmp(&o2), Ordering::Greater);
        assert_eq!(o2.cmp(&o1), Ordering::Greater);
        assert_eq!(o3.cmp(&o1), Ordering::Greater);
    }

    #[test]
    fn order_key_eq_price_test() {
        let o1 = OrderKey {
            sequence_id: 0,
            price: BigDecimal::from(1),
            side: TradeSide::BUY,
        };
        let o2 = OrderKey {
            sequence_id: 1,
            price: BigDecimal::from(1),
            side: TradeSide::BUY,
        };
        let o3 = OrderKey {
            sequence_id: 2,
            price: BigDecimal::from(1),
            side: TradeSide::BUY,
        };
        // 期望排序：o1,o2,o3
        assert_eq!(o3.cmp(&o2), Ordering::Greater);
        assert_eq!(o2.cmp(&o1), Ordering::Greater);
        assert_eq!(o3.cmp(&o1), Ordering::Greater);
    }
}
