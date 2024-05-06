use bigdecimal::BigDecimal;
use log::debug;
use serde::{Deserialize, Serialize};

use crate::book::OrderBook;
use crate::order::{Order, OrderKey, OrderState};
use crate::order::OrderState::{CANCELED, FULL_FILLED, INIT, PARTIAL_CANCELLED, PARTIAL_FILLED};
use crate::order::OrderTimeInForce::{FOK, GTC, IOC};
use crate::order::OrderType::LIMIT;
use crate::order::TradeSide::{BUY, SELL};
use crate::utils;

/// 市场结构体，其中记录了最新成交价格和买卖双方的订单簿
#[derive(Debug)]
pub struct MarketBook {
    /// 交易对
    pub symbol: String,
    /// 买方订单簿
    buy: OrderBook,
    /// 卖方订单簿
    sell: OrderBook,
    /// 最新成交价
    px: BigDecimal,
    /// 最新成交时间
    ts: u128,
}

unsafe impl Send for MarketBook {}

impl MarketBook {
    pub fn new(symbol: &str) -> MarketBook {
        MarketBook {
            symbol: String::from(symbol),
            buy: OrderBook::new(symbol, BUY),
            sell: OrderBook::new(symbol, SELL),
            px: BigDecimal::from(0),
            ts: Self::now_ts(),
        }
    }

    fn now_ts() -> u128 {
        utils::now_ts()
    }

    /// 取消订单
    pub fn try_cancel(&mut self, cancel: Order) -> Vec<MatchTrade> {
        match cancel.side {
            BUY => Self::cancel_book(&mut self.buy, cancel),
            SELL => Self::cancel_book(&mut self.sell, cancel)
        }
    }

    /// 传入taker_order尝试撮合订单
    pub fn try_match(&mut self, taker_order: Order) -> Vec<MatchTrade> {
        let trades = match taker_order.side {
            BUY => Self::match_book(taker_order, &mut self.sell, &mut self.buy),
            SELL => Self::match_book(taker_order, &mut self.buy, &mut self.sell),
        };
        // 更新时间
        self.ts = Self::now_ts();
        // 更新最新成交价格
        if let Some(px) = trades.last().map(|trade| trade.px.clone()) {
            self.px = px;
        }
        trades
    }

    fn cancel_book(book: &mut OrderBook, cancel: Order) -> Vec<MatchTrade> {
        let mut trades = Vec::new();
        let order_key = OrderKey::new(&cancel);
        if let Some(order) = book.del_by_key(&order_key) {
            let order = order.borrow_mut();
            let trade;
            if order.remain() != order.qty {
                // 有部分成交
                trade = MatchTrade::new_taker_partial_cancel(&order.symbol, order.id);
            } else {
                // 没有成交数量
                trade = MatchTrade::new_taker_cancel(&order.symbol, order.id);
            }
            trades.push(trade);
        }
        trades
    }

    fn match_book(
        mut taker_order: Order,
        maker_book: &mut OrderBook,
        taker_book: &mut OrderBook,
    ) -> Vec<MatchTrade> {
        // 检查taker_order是否存在，防止重复请求
        if taker_book.exist_by_key(&OrderKey::new(&taker_order)) {
            return Vec::with_capacity(0);
        }
        let mut trades = Vec::with_capacity(10);
        let mut taker_remain = taker_order.remain();
        loop {
            if taker_remain <= 0 {
                // 已撮合完成，直接退出
                break;
            }

            let maker_order = match maker_book.head() {
                Some(order) => order,
                None => {
                    break;
                }
            };

            let mut maker_order = maker_order.borrow_mut();
            // 检查是否可成交
            if !taker_order.can_trade(&maker_order) {
                // 与买/卖一不能成交，跳出循环
                break;
            }

            // 确定撮合数量
            let mut maker_remain = maker_order.remain();
            let matched_qty;
            if taker_order.tif == FOK && maker_remain < taker_remain {
                // 不能完全成交,直接跳出
                break;
            } else {
                matched_qty = taker_remain.min(maker_remain);
            }
            if matched_qty <= 0 {
                break;
            }

            // 修改maker订单
            maker_remain -= matched_qty;
            if maker_remain > 0 {
                // 有剩余部分成交
                maker_order.fill(matched_qty, PARTIAL_FILLED);
            } else {
                // 无剩余，完全成交
                maker_order.fill(matched_qty, FULL_FILLED);
                // 从订单簿中删除
                maker_book.del(&maker_order);
            }

            // 修改taker订单
            taker_remain -= matched_qty;
            if taker_remain > 0 {
                // 部分成交
                taker_order.fill(matched_qty, PARTIAL_FILLED)
            } else {
                // 全部成交
                taker_order.fill(matched_qty, FULL_FILLED)
            }

            // 构造撮合结果
            let trade = MatchTrade {
                symbol: taker_order.symbol.clone(),
                qty: matched_qty,
                px: maker_order.price.clone(),
                taker_oid: taker_order.id,
                maker_oid: maker_order.id,
                taker_state: taker_order.state,
                maker_state: maker_order.state,
                ts: Self::now_ts(),
            };
            trades.push(trade);

            // 检查IOC订单是否要继续匹配
            if taker_order.tif == IOC && taker_order.state == PARTIAL_CANCELLED {
                // 对于IOC的订单，无需继续匹配
                break;
            }
        }

        if taker_remain > 0 {
            match taker_order.tif {
                GTC => {
                    match taker_order.ord_type {
                        LIMIT => {
                            // 不能立即成交的限价单放入订单簿等待以后成交
                            taker_book.add(taker_order).unwrap();
                        }
                        _ => {}
                    }
                }
                IOC => {
                    if taker_remain == taker_order.qty {
                        // 完全没有成交
                        taker_order.fill(0, CANCELED);
                        trades.push(MatchTrade::new_taker_cancel(&taker_order.symbol, taker_order.id));
                    } else {
                        // 有部分成交
                        taker_order.fill(0, PARTIAL_CANCELLED);
                        trades.push(MatchTrade::new_taker_partial_cancel(&taker_order.symbol, taker_order.id));
                    }
                }
                FOK => {
                    taker_order.fill(0, CANCELED);
                    trades.push(MatchTrade::new_taker_cancel(&taker_order.symbol, taker_order.id));
                }
            }
        }
        trades
    }
}

/// 成交结构体，记录了撮合的成交
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct MatchTrade {
    /// 成交ID
    // id: u64,
    /// symbol
    pub symbol: String,
    /// 撮合数量
    pub qty: u64,
    /// 撮合价格
    pub px: BigDecimal,
    /// taker的订单id
    pub taker_oid: u64,
    /// maker的订单ID
    pub maker_oid: u64,
    /// taker订单撮合后状态
    pub taker_state: OrderState,
    /// marker订单撮合后状态
    pub maker_state: OrderState,
    /// 成交时间
    pub ts: u128,
}

impl MatchTrade {
    fn new_taker_cancel(s: &str, oid: u64) -> MatchTrade {
        MatchTrade {
            symbol: s.to_owned(),
            qty: 0,
            px: BigDecimal::from(0),
            taker_oid: oid,
            maker_oid: 0,
            taker_state: CANCELED,
            maker_state: INIT,
            ts: MarketBook::now_ts(),
        }
    }

    fn new_taker_partial_cancel(s: &str, oid: u64) -> MatchTrade {
        MatchTrade {
            symbol: s.to_owned(),
            qty: 0,
            px: BigDecimal::from(0),
            taker_oid: oid,
            maker_oid: 0,
            taker_state: PARTIAL_CANCELLED,
            maker_state: INIT,
            ts: MarketBook::now_ts(),
        }
    }
}
