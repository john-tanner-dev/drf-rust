use once_cell::sync::OnceCell;
use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::types::DbConfig;


static POOLS: OnceCell<Mutex<HashMap<String, AnyPool>>> = OnceCell::new();


fn get_pool_map() -> &'static Mutex<HashMap<String, AnyPool>> {
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}


pub async fn get_or_create_pool(
    db_alias: &str,
    config: &DbConfig,
) -> Result<AnyPool, String> {
    // Check if pool already exists (fast path — no connection needed)
    // 检查连接池是否已存在（快速路径 — 无需连接）
    {
        let guard = get_pool_map().lock()
            .map_err(|e: std::sync::PoisonError<_>| e.to_string())?;
        if let Some(pool) = guard.get(db_alias) {
            return Ok(pool.clone());
        }
    }
    // Lock is released here before the expensive connect() call
    // 在昂贵的 connect() 调用之前释放锁

    // Create new pool by building DSN from config and connecting
    // 通过从配置构建 DSN 并连接来创建新连接池
    let dsn = config.to_dsn();
    let pool: AnyPool = AnyPoolOptions::new()
        .max_connections(20)  // Max concurrent connections / 最大并发连接数
        .min_connections(2)   // Keep 2 connections warm / 保持 2 个预热连接
        .connect(&dsn)
        .await
        .map_err(|e| format!("Connection failed for '{}': {}", db_alias, e))?;

    // Store the new pool in the global cache
    // 将新连接池存储到全局缓存中
    let mut guard = get_pool_map().lock()
        .map_err(|e: std::sync::PoisonError<_>| e.to_string())?;
    guard.insert(db_alias.to_string(), pool.clone());
    Ok(pool)
}
