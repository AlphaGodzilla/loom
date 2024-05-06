use async_trait::async_trait;
use log::info;

use loom_core::market::MatchTrade;

use crate::cache::CacheManager;

#[derive(Debug)]
pub enum TradeConsumer {
    Console(ConsoleConsumer),
    RedisQueue(RedisQueueConsumer),
}

#[async_trait]
pub trait Consumer {
    async fn consume(&self, trades: Vec<MatchTrade>) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub struct ConsoleConsumer {}

#[async_trait]
impl Consumer for ConsoleConsumer {
    async fn consume(&self, trades: Vec<MatchTrade>) -> anyhow::Result<()> {
        info!("{}", serde_json::to_string(&trades)?);
        Ok(())
    }
}

impl TradeConsumer {
    pub async fn consume(&self, trades: Vec<MatchTrade>) -> anyhow::Result<()> {
        match self {
            TradeConsumer::Console(consumer) => {
                consumer.consume(trades).await?;
            }
            TradeConsumer::RedisQueue(consumer) => {
                consumer.consume(trades).await?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RedisQueueConsumer {
    cache_manager: CacheManager,
}

impl RedisQueueConsumer {
    pub async fn new(uri: &str) -> anyhow::Result<RedisQueueConsumer> {
        Ok(RedisQueueConsumer {
            cache_manager: CacheManager::new(uri).await?
        })
    }

    pub async fn new_with_cache_manager(cache_manager: CacheManager) -> anyhow::Result<RedisQueueConsumer> {
        Ok(RedisQueueConsumer {
            cache_manager
        })
    }
}

#[async_trait]
impl Consumer for RedisQueueConsumer {
    async fn consume(&self, trades: Vec<MatchTrade>) -> anyhow::Result<()> {
        self.cache_manager.offer_trades(trades).await?;
        Ok(())
    }
}