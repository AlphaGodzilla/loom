use std::sync::Arc;

use log::{debug, info};
use tokio::{
    select,
    sync::{
        mpsc::{self},
    },
    task::JoinHandle,
};
use tokio::sync::{broadcast, Mutex, oneshot};

use loom_core::{
    market::{MarketBook, MatchTrade},
    order::Order,
};
use loom_core::order::OrderAction;

use crate::consumer::TradeConsumer;

pub type TraderMatchRequest = (Order, oneshot::Sender<Vec<MatchTrade>>);

/// 市场交易员
#[derive(Debug)]
pub struct Trader {
    /// 交易对
    symbol: String,
    /// 交易对市场
    book: Arc<Mutex<MarketBook>>,
    /// 撮合请求输入器
    req_sender: mpsc::Sender<Order>,
    /// 撮合结果输出器
    req_receiver: Arc<Mutex<mpsc::Receiver<Order>>>,
    /// 消费器
    consumer: Arc<Mutex<TradeConsumer>>,
}

impl Trader {
    /// 新建交易员
    pub fn new(symbol: &str, consumer: TradeConsumer) -> Trader {
        let (sender, receiver) = mpsc::channel(16);
        Trader {
            symbol: String::from(symbol),
            book: Arc::new(Mutex::new(MarketBook::new(symbol))),
            req_sender: sender,
            req_receiver: Arc::new(Mutex::new(receiver)),
            consumer: Arc::new(Mutex::new(consumer)),
        }
    }

    /// 开始交易，返回协程句柄
    pub fn launch(&self, mut ctx: broadcast::Receiver<bool>) -> JoinHandle<()> {
        let symbol = self.symbol.to_owned();
        let receiver = Arc::clone(&self.req_receiver);
        let book = Arc::clone(&self.book);
        let consumer = Arc::clone(&self.consumer);
        let handler = tokio::spawn(async move {
            let mut receiver = receiver.lock().await;
            let mut book = book.lock().await;
            let mut consumer = consumer.lock().await;
            loop {
                select! {
                    Ok(terminal) = ctx.recv() => {
                        info!("Rev terminal signal, symbol={}, terminal={}", &symbol, terminal);
                        if terminal {
                            info!("Terminal..., symbol={}", &symbol);
                            break
                        }
                    }
                    Some(order) = receiver.recv() => {
                        let _ = handle_request(&mut book, order, &mut consumer).await;
                    }
                }
            }
            receiver.close();
            info!("TRADER EXIT: {}", &symbol);
        });
        info!("NEW TRADER LAUNCHED: {}", &self.symbol);
        handler
    }


    /// 获取新的发送器
    pub fn get_input_sender(&self) -> mpsc::Sender<Order> {
        self.req_sender.clone()
    }

    /// 撮合订单
    pub async fn feed(&self, order: Order) -> anyhow::Result<()> {
        self.req_sender.send(order).await?;
        Ok(())
    }
}

async fn handle_request(book: &mut MarketBook, order: Order, consumer: &mut TradeConsumer) -> anyhow::Result<()> {
    debug!("NEW MATCH: {}", serde_json::to_string(&order)?);
    let trades;
    match order.action {
        OrderAction::PLACE => {
            // 撮合动作
            trades = book.try_match(order);
        }
        OrderAction::CANCEL => {
            // 撤单动作
            trades = book.try_cancel(order);
        }
    }
    debug!("NEW TRADES: {}", serde_json::to_string(&trades)?);
    consumer.consume(trades).await?;
    Ok(())
}
