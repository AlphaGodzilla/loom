use std::sync::Arc;

use env_logger::Env;
use tokio::sync::Mutex;

use loom::config::CacheBackend::Redis;
use loom::config::{Config, ConsumerKind};
use loom::http_server::start_http_server;
use loom_core::market;
use loom_engine::cache::CacheManager;
use loom_engine::consumer::{ConsoleConsumer, RedisQueueConsumer, TradeConsumer};
use loom_engine::engine::MatchEngine;

#[tokio::main]
async fn main() {
    // 初始化日志
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    // 初始化配置
    let config = Config::from_file(None).unwrap();

    // 初始化缓存管理器
    let cache_manager = init_cache_manager(&config).await;

    // 初始化引擎
    let market = init_engine(&config, cache_manager).await;

    // 启动HttpServer
    let trader_market = Arc::new(Mutex::new(market));
    start_http_server(&config, Arc::clone(&trader_market)).await
}

async fn init_cache_manager(config: &Config) -> CacheManager {
    let backend = config.cache.backend.clone().unwrap_or(Redis);
    match backend {
        Redis => {
            let uri = config.cache.redis.to_redis_uri();
            CacheManager::new(uri.as_str()).await.unwrap()
        }
    }
}

async fn init_engine(config: &Config, cache_manager: CacheManager) -> MatchEngine {
    let mut market = MatchEngine::new(cache_manager.clone());
    let kind = config.consumer.clone();

    let consumer = match kind {
        ConsumerKind::Console => {
            TradeConsumer::Console(ConsoleConsumer {})
        }
        ConsumerKind::Redis => {
            TradeConsumer::RedisQueue(
                RedisQueueConsumer::new_with_cache_manager(cache_manager.clone())
                    .await
                    .unwrap()
            )
        }
    };

    let symbols = config.market.symbols.clone().unwrap_or(Vec::new());
    for symbol in symbols {
        market.new_trader(symbol.as_str(), consumer.clone()).await.unwrap();
    }

    market
}







