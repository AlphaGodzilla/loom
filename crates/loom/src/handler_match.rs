use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use bigdecimal::{BigDecimal, Zero};
use bigdecimal::num_traits::zero;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use validator::{Validate, ValidationError};

use loom_core::order::{Order, OrderAction, OrderState, OrderTimeInForce, OrderType, TradeSide};
use loom_core::order::OrderTimeInForce::{GTC, IOC};
use loom_core::order::OrderType::MARKET;
use loom_core::utils;
use loom_engine::engine::MatchEngine;

use crate::http_server::AppError;

pub type TraderMarketWrap = Arc<Mutex<MatchEngine>>;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, Validate)]
pub struct MatchOrderParam {
    /// 订单序列号
    #[validate(range(min = 1))]
    pub id: u64,
    /// 交易对
    #[validate(length(min = 2, max = 50))]
    pub symbol: String,
    /// 交易方向
    pub side: TradeSide,
    /// 委托数量
    #[validate(range(min = 1))]
    pub qty: u64,
    /// 委托价格
    pub price: Option<BigDecimal>,
    /// 订单类型
    pub ord_type: OrderType,
    /// 订单撮合行为
    pub tif: Option<OrderTimeInForce>,
    /// 订单动作
    pub action: OrderAction,
    pub ts: Option<u128>,
}

impl MatchOrderParam {
    pub fn to_order(&self) -> Order {
        let now_ts = self.ts.unwrap_or_else(|| { utils::now_ts() });
        Order {
            id: self.id,
            symbol: self.symbol.clone(),
            side: self.side,
            qty: self.qty,
            price: self.price.as_ref().map(|x| x.clone()).unwrap_or(zero()),
            acc_fill_qty: 0,
            ord_type: self.ord_type,
            ts: now_ts,
            update_ts: now_ts,
            state: OrderState::LIVE,
            tif: self.tif.unwrap_or_else(|| {
                if self.ord_type == MARKET {
                    IOC
                } else {
                    GTC
                }
            }),
            action: self.action,
        }
    }
}

pub async fn handler_match(State(state): State<TraderMarketWrap>, Json(param): Json<MatchOrderParam>) -> Result<String, AppError> {
    param.validate()?;
    // 检查价格不能小于0
    if let Some(price) = &param.price {
        let price = price.clone();
        // info!("price is {}", &price);
        if price < Zero::zero() {
            return Err(ValidationError::new("price < 0").into());
        }
    };
    // 检查市价单TimeInForce不能为GTC
    if let Some(tif) = param.tif {
        if param.ord_type == MARKET && tif == GTC {
            return Err(ValidationError::new("market price type order's tif can not be GTC").into());
        }
    }
    let mut market = state.lock().await;
    let order = param.to_order();
    market.feed(order).await?;
    Ok(String::from("ACCEPTED"))
}