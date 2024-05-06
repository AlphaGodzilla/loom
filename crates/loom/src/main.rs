use std::sync::Arc;

use env_logger::Env;
use tokio::sync::Mutex;

use loom::http_server::start_http_server;
use loom_engine::cache::CacheManager;
use loom_engine::consumer::{RedisQueueConsumer, TradeConsumer};
use loom_engine::engine::MatchEngine;

#[tokio::main]
async fn main() {
    // 初始化日志
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    // 初始化缓存管理器
    let cache_manager = CacheManager::new("redis://localhost:6379").await.unwrap();

    // 启动撮合引擎
    let mut market = MatchEngine::new(cache_manager.clone());
    let consumer = TradeConsumer::RedisQueue(RedisQueueConsumer::new_with_cache_manager(cache_manager.clone()).await.unwrap());
    market.new_trader("LOOM-USDT-SPOT", consumer).await.unwrap();
    let trader_market = Arc::new(Mutex::new(market));

    // 启动Server
    start_http_server(Arc::clone(&trader_market)).await
}







