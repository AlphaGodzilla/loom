use std::collections::HashMap;
use std::time::Duration;

use bb8_redis::{bb8, RedisConnectionManager};
use bb8_redis::bb8::Pool;
use log::debug;
use redis::aio::MultiplexedConnection;
use redis::ConnectionLike;
use serde::{Deserialize, Serialize};
use validator::HasLen;

use loom_core::market::MatchTrade;
use loom_core::order::{Order, OrderState};
use loom_core::utils;

pub const CACHE_PREFIX: &str = "Loom";

#[derive(Clone, Debug)]
pub struct CacheManager {
    pool: Pool<RedisConnectionManager>,
}


impl CacheManager {
    pub async fn new(redis_uri: &str) -> anyhow::Result<CacheManager> {
        let manager = RedisConnectionManager::new(redis_uri)?;
        let pool = bb8::Pool::builder()
            .connection_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(60))
            .build(manager)
            .await
            .unwrap();
        Ok(
            CacheManager { pool }
        )
    }

    fn cache_key_id(symbol: &str) -> String {
        format!("{}:ID:{}", CACHE_PREFIX, symbol)
    }

    fn cache_key_order(symbol: &str, id: u64) -> String {
        format!("{}:ORDER:{}:{}", CACHE_PREFIX, symbol, id)
    }

    fn cache_key_trades(symbol: &str) -> String {
        format!("{}:TRADES:{}", CACHE_PREFIX, symbol)
    }

    pub fn cache_key(order_ref: &Order) -> (String, String) {
        (
            Self::cache_key_id(&order_ref.symbol),
            Self::cache_key_order(&order_ref.symbol, order_ref.id)
        )
    }

    pub async fn add_if_absent(&self, order: Order) -> anyhow::Result<bool> {
        let conn = self.pool.get().await?.to_owned();
        let (id_key, order_key) = Self::cache_key(&order);
        let resp = redis::pipe()
            .atomic()
            .cmd("ZADD").arg(id_key).arg("NX").arg(order.ts.to_string()).arg(order.id.to_string())
            .cmd("HSETNX").arg(&order_key).arg("id").arg(order.id.to_string())
            .cmd("HSETNX").arg(&order_key).arg("symbol").arg(order.symbol.clone())
            .cmd("HSETNX").arg(&order_key).arg("side").arg(order.side.to_string())
            .cmd("HSETNX").arg(&order_key).arg("qty").arg(order.qty.to_string())
            .cmd("HSETNX").arg(&order_key).arg("price").arg(order.price.to_string())
            .cmd("HSETNX").arg(&order_key).arg("acc_fill_qty").arg(order.acc_fill_qty.to_string())
            .cmd("HSETNX").arg(&order_key).arg("ord_type").arg(order.ord_type.to_string())
            .cmd("HSETNX").arg(&order_key).arg("ts").arg(order.ts.to_string())
            .cmd("HSETNX").arg(&order_key).arg("update_ts").arg(order.update_ts.to_string())
            .cmd("HSETNX").arg(&order_key).arg("state").arg(order.state.to_string())
            .cmd("HSETNX").arg(&order_key).arg("tif").arg(order.tif.to_string())
            .cmd("HSETNX").arg(&order_key).arg("action").arg(order.action.to_string())
            .query_async::<MultiplexedConnection, Vec<i32>>(&mut conn.to_owned())
            .await?;
        Ok(resp.get(0).map(|i| i.to_owned() == 1).unwrap_or(false) &&
            resp.get(1).map(|i| i.to_owned() == 1).unwrap_or(false)
        )
    }

    pub async fn del(&self, order_ref: &Order) -> anyhow::Result<()> {
        let conn = self.pool.get().await?;
        let (id_key, order_key) = Self::cache_key(order_ref);
        redis::pipe()
            .atomic()
            .zrem(id_key, order_ref.id.to_string())
            .del(order_key)
            .query_async(&mut conn.to_owned())
            .await?;
        Ok(())
    }

    pub async fn get_ids<F>(&self, symbol: &str, mut consumer: F) -> anyhow::Result<()>
        where
            F: FnMut(u64) -> anyhow::Result<()>
    {
        let mut conn = self.pool.get().await?.to_owned();
        let id_key = Self::cache_key_id(symbol);
        let mut cmd = redis::cmd("ZRANGEBYSCORE");
        cmd.arg(id_key)
            .arg("0")
            .arg(utils::now_ts().to_string());
        let mut iter = cmd.iter_async::<String>(&mut conn).await?;
        while let Some(id) = iter.next_item().await {
            let id = id.parse::<u64>()?;
            consumer(id)?;
        }
        Ok(())
    }

    pub async fn get_orders_by_ids(&self, symbol: &str, ids: &Vec<u64>) -> anyhow::Result<Vec<Order>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.pool.get().await?.to_owned();
        let order_keys: Vec<String> = ids.iter().map(|id| Self::cache_key_order(symbol, id.clone())).collect();
        let mut pipe = redis::pipe();
        for order_key in order_keys {
            pipe.cmd("HGETALL").arg(order_key);
        }
        let orders = pipe
            .query_async::<_, Vec<HashMap<String, String>>>(&mut conn)
            .await?;
        let orders = orders.iter()
            .filter(|i| !i.is_empty())
            .map(|i| Order::from_map(i))
            .filter(|i| i.is_ok())
            .map(|i| i.unwrap())
            .collect();
        Ok(orders)
    }

    pub async fn get_order_batch<F>(&self, symbol: &str, batch: usize, consumer: F) -> anyhow::Result<()>
        where F: Fn(Order) -> anyhow::Result<()>
    {
        let mut conn = self.pool.get().await?.to_owned();
        let id_key = Self::cache_key_id(symbol);
        let mut cmd = redis::cmd("ZRANGEBYSCORE");
        cmd.arg(id_key)
            .arg("0")
            .arg(utils::now_ts().to_string());
        let mut iter = cmd.iter_async::<String>(&mut conn).await?;
        let mut buffer = Vec::with_capacity(batch);
        while let Some(id) = iter.next_item().await {
            let id = id.parse::<u64>()?;
            buffer.push(id);
            if buffer.length() >= batch as u64 {
                // 获取order
                let mut orders = self.get_orders_by_ids(symbol, &buffer).await?;
                for order in orders.drain(..) {
                    consumer(order)?;
                }
                // 重置buffer
                buffer.clear();
            }
        }
        Ok(())
    }

    pub async fn offer_trades(&self, trades: Vec<MatchTrade>) -> anyhow::Result<()> {
        if trades.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get().await?.to_owned();
        /// 脚本参数描述
        /// KEYS
        /// 1. trades_key
        /// ARGV:
        /// 1.OrderUpdates: [{...}]
        /// 2. trades
        let script = redis::Script::new(r"
            local function update_order(oid_key, order_key, oid, acc_fill_qty, state, ts, del_flag)
                local exist = redis.call('EXISTS', order_key);
                if exist == 1 then
                    -- 判断是否需要删除order
                    if del_flag or del_flag == 'true' then
                        -- 删除订单
                        redis.call('DEL', order_key);
                        -- 删除ID
                        redis.call('ZREM', oid_key, oid);
                        return;
                    end
                    -- 判断是否需要更新累计成交量
                    if acc_fill_qty > 0 then
                        local pre_acc_fill_qty = tonumber(redis.call('HGET', order_key, 'acc_fill_qty'));
                        redis.call('HSET', order_key, 'acc_fill_qty', pre_acc_fill_qty + acc_fill_qty);
                    end
                    -- 判断是否需要更新订单状态
                    if not del_flag or del_flag == 'false' then
                        redis.call('HSET', order_key, 'state', state);
                        redis.call('HSET', order_key, 'update_ts', ts);
                    end
                end
            end


            local updates = cjson.decode(ARGV[1]);
            for key,update in ipairs(updates) do
                local oid_key = update['oid_key'];
                local acc_fill_qty = update['qty'];
                local ts = update['ts'];

                local taker_order_key = update['taker_order_key'];
                local taker_state = update['taker_state'];
                local del_taker_flag = update['del_taker_flag'];
                local taker_oid = update['taker_oid'];
                update_order(oid_key, taker_order_key, taker_oid, acc_fill_qty, taker_state, ts, del_taker_flag);

                local maker_order_key = update['maker_order_key'];
                local maker_state = update['maker_state'];
                local del_maker_flag = update['del_maker_flag'];
                local maker_oid = update['maker_oid'];
                update_order(oid_key, maker_order_key, maker_oid, acc_fill_qty, maker_state, ts, del_maker_flag);
            end

            -- add trade queue
            local trades_key = KEYS[1];
            redis.call('XADD', trades_key, 'MAXLEN', '~', '1000', '*', 'trades', ARGV[2]);
        ");
        let updates: Vec<OrderUpdate> = trades.iter().map(|i| OrderUpdate::new(i)).collect();
        let symbol = &(trades.get(0).unwrap().symbol);
        let trades_key = CacheManager::cache_key_trades(symbol);
        let updates = serde_json::to_string(&updates)?;
        let trades = serde_json::to_string(&trades)?;
        debug!("NEW UPDATES: {}", &updates);
        script.key(trades_key)
            .arg(updates)
            .arg(trades)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    /// 撮合数量
    qty: u64,
    /// 订单ID的key
    oid_key: String,
    /// taker订单ID
    taker_oid: String,
    /// maker订单ID
    maker_oid: String,
    /// taker订单key
    taker_order_key: String,
    /// maker订单key
    maker_order_key: String,
    /// taker订单撮合后状态
    taker_state: OrderState,
    /// marker订单撮合后状态
    maker_state: OrderState,
    /// 是否删除taker
    del_taker_flag: bool,
    /// 是否删除maker
    del_maker_flag: bool,
    /// 成交时间
    ts: u128,
}

impl OrderUpdate {
    pub fn new(trade: &MatchTrade) -> OrderUpdate {
        OrderUpdate {
            qty: trade.qty.clone(),
            oid_key: CacheManager::cache_key_id(&trade.symbol),
            taker_oid: trade.taker_oid.to_string(),
            maker_oid: trade.maker_oid.to_string(),
            taker_order_key: CacheManager::cache_key_order(&trade.symbol, trade.taker_oid),
            maker_order_key: CacheManager::cache_key_order(&trade.symbol, trade.maker_oid),
            taker_state: trade.taker_state.clone(),
            maker_state: trade.maker_state.clone(),
            del_taker_flag: trade.taker_state.del_flag(),
            del_maker_flag: trade.maker_state.del_flag(),
            ts: trade.ts.clone(),
        }
    }
}

#[cfg(test)]
pub mod test {
    use bigdecimal::BigDecimal;
    use bigdecimal::num_traits::zero;
    use serde::{Deserialize, Serialize};
    use validator::Validate;

    use loom_core::order::{Order, OrderAction, OrderState, OrderTimeInForce, OrderType, TradeSide};
    use loom_core::order::OrderTimeInForce::{GTC, IOC};
    use loom_core::order::OrderType::MARKET;
    use loom_core::utils;

    use crate::cache::CacheManager;

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
                state: OrderState::INIT,
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

    fn new_order() -> Order {
        let json = "{\"id\":1,\"symbol\":\"LOOM-USDT-SPOT\",\"side\":\"SELL\",\"qty\":3,\"price\":100,\"ord_type\":\"LIMIT\",\"action\":\"PLACE\"}";
        serde_json::from_str::<MatchOrderParam>(json).unwrap().to_order()
    }

    async fn get_cache() -> CacheManager {
        return CacheManager::new("redis://localhost:6379").await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn add_test() {
        let cache = get_cache().await;
        // 先删除
        cache.del(&new_order()).await.unwrap();
        assert!(cache.add_if_absent(new_order()).await.unwrap());
        assert!(!cache.add_if_absent(new_order()).await.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn get_orders_by_ids_test() {
        let ids = vec![1];
        let orders = get_cache().await.get_orders_by_ids("LOOM-USDT-SPOT", &ids).await.unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders.get(0).unwrap().id, 1);
    }

    #[tokio::test]
    #[ignore]
    async fn get_ids_test() {
        let buffer = &mut Vec::new();
        get_cache().await.get_ids("LOOM-USDT-SPOT", |i| {
            buffer.push(i);
            Ok(())
        }).await.unwrap();
        assert_eq!(buffer.len(), 1)
    }

    #[tokio::test]
    #[ignore]
    async fn del_test() {
        let order = new_order();
        let cache = get_cache().await;
        // 添加
        cache.add_if_absent(order.clone()).await.unwrap();
        // 执行删除
        cache.del(&order).await.unwrap();
        // 再次查询
        let orders = cache.get_orders_by_ids(&order.symbol, &vec![1]).await.unwrap();
        assert_eq!(orders.len(), 0);
    }
}