use std::collections::HashMap;

use anyhow::anyhow;
use log::info;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use loom_core::order::{Order, OrderAction};

use crate::cache::CacheManager;
use crate::consumer::TradeConsumer;
use crate::trader::Trader;

/// 交易员市场，其中注册了多个交易对交易员
// #[derive(Debug)]
pub struct MatchEngine {
    traders: HashMap<String, Trader>,
    handlers: Vec<JoinHandle<()>>,
    ctx: broadcast::Sender<bool>,
    is_shutdown: bool,
    cache_manager: CacheManager,
}

impl MatchEngine {
    pub fn new(cache_manager: CacheManager) -> MatchEngine {
        let sender = broadcast::Sender::new(1);
        MatchEngine {
            traders: HashMap::new(),
            handlers: Vec::new(),
            ctx: sender,
            is_shutdown: false,
            cache_manager,
        }
    }

    /// 创建交易员并开始交易
    pub async fn new_trader(&mut self, symbol: &str, consumer: TradeConsumer) -> anyhow::Result<&Self> {
        let exist = self.traders.contains_key(symbol);
        if exist {
            let msg = format!("engine already exist, symbol={}", symbol);
            return Err(anyhow!(msg));
        }
        // 构造交易员
        let trader = Trader::new(symbol, consumer);
        // 启动交易员
        let handler = trader.launch(self.ctx.subscribe());
        // 保存协程句柄
        self.handlers.push(handler);
        // 保存交易员句柄
        self.traders.insert(String::from(symbol), trader);
        if let Some(trader) = self.traders.get(symbol) {
            // 从缓存中恢复
            let oid_buffer = &mut Vec::new();
            self.cache_manager.get_ids(symbol, |id| {
                oid_buffer.push(id);
                Ok(())
            }).await?;
            let mut orders = self.cache_manager.get_orders_by_ids(symbol, &oid_buffer).await?;
            let mut recover_cnt = 0;
            for order in orders.drain(..) {
                trader.feed(order).await?;
                recover_cnt += 1;
            };
            info!("RECOVER: symbol={}, orders_cnt={}", symbol, recover_cnt);
        }
        Ok(self)
    }

    /// 发送撮合请求
    pub async fn feed(&mut self, order: Order) -> anyhow::Result<()> {
        if self.is_shutdown {
            // 引擎关闭，无法提交
            return Err(anyhow!("engine stopping or stopped"));
        }
        match order.action {
            OrderAction::PLACE => {
                // 加入缓存，防止关机内存丢失
                let success = self.cache_manager.add_if_absent(order.clone()).await?;
                if !success {
                    // 已经存在订单
                    return Err(anyhow!("order existed"));
                }
            }
            OrderAction::CANCEL => {}
        }
        // 提供撮合请求
        if let Some(trader) = self.traders.get(&order.symbol) {
            trader.feed(order).await?;
        }
        Ok(())
    }

    /// 关闭市场
    pub async fn shutdown(&mut self) {
        if !self.is_shutdown {
            // 发送中断信号
            self.ctx.send(true).unwrap();
            // 等待所有协程停止
            for handler in self.handlers.drain(..) {
                handler.await.unwrap();
            }
        }
    }
}



