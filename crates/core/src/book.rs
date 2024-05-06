use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use crate::order::{Order, OrderKey, TradeSide};

pub type OrderRef = Rc<RefCell<Order>>;

/// 订单薄结构体，结构体中保存了同交易对同方向的的所有订单
#[derive(Debug)]
pub struct OrderBook {
    /// 交易对
    symbol: String,
    /// 交易方向
    side: TradeSide,
    /// 订单列表
    orders: BTreeMap<OrderKey, OrderRef>,
}

impl OrderBook {
    /// 构造一个订单簿，订单名称和订单方向
    pub fn new(symbol: &str, side: TradeSide) -> OrderBook {
        OrderBook {
            symbol: String::from(symbol),
            side,
            orders: BTreeMap::default(),
        }
    }

    /// 添加订单
    pub fn add(&mut self, order: Order) -> anyhow::Result<&Self> {
        // 判断订单方向
        if order.side != self.side {
            // r("order trade side mismatch")
            return Err(anyhow::Error::msg("order trade side mismatch"));
        }
        // 排序键
        let order_key = OrderKey::new(&order);
        if !self.exist_by_key(&order_key) {
            // 插入订单
            self.orders.insert(order_key, Rc::new(RefCell::new(order)));
        }
        Ok(self)
    }

    /// 删除订单
    pub fn del(&mut self, order: &Order) -> Option<OrderRef> {
        self.orders.remove(&OrderKey::new(&order))
    }

    pub fn del_by_key(&mut self, order_key: &OrderKey) -> Option<OrderRef> {
        self.orders.remove(order_key)
    }

    /// 取订单簿头
    pub fn head(&mut self) -> Option<OrderRef> {
        return self.orders.first_key_value().map(|(_, v)| Rc::clone(v));
    }

    pub fn size(&self) -> usize {
        self.orders.len()
    }

    pub fn exist_by_key(&self, key: &OrderKey) -> bool {
        self.orders.contains_key(key)
    }
}
