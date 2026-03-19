// ============================================================================
// pool.rs — Database connection pool management.
// pool.rs — 数据库连接池管理。
//
// Manages a global, lazy-initialized map of connection pools keyed by db_alias.
// 管理一个全局的、延迟初始化的连接池映射，以 db_alias 为键。
//
// Pool lifecycle / 连接池生命周期:
//   1. First request for a db_alias → create new pool (connect to DB).
//      首次请求某 db_alias → 创建新连接池（连接到数据库）。
//   2. Subsequent requests → return cached pool (O(1) lookup).
//      后续请求 → 返回缓存的连接池（O(1) 查找）。
//   3. Pools live for the process lifetime (no explicit cleanup).
//      连接池存活于进程生命周期（无显式清理）。
//
// Pool config / 连接池配置:
//   - max_connections: 20 (per database alias / 每个数据库别名)
//   - min_connections: 2  (warm connections / 预热连接)
// ============================================================================

use once_cell::sync::OnceCell;
use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::types::DbConfig;

/// Global connection pool storage, keyed by database alias (e.g., "default").
/// 全局连接池存储，以数据库别名为键（如 "default"）。
///
/// Uses OnceCell for lazy initialization and Mutex for thread-safe access.
/// 使用 OnceCell 进行延迟初始化，使用 Mutex 进行线程安全访问。
static POOLS: OnceCell<Mutex<HashMap<String, AnyPool>>> = OnceCell::new();

/// Get the global pool map (lazily initialized).
/// 获取全局连接池映射（延迟初始化）。
fn get_pool_map() -> &'static Mutex<HashMap<String, AnyPool>> {
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Get or create a connection pool for the given database alias.
/// 获取或创建给定数据库别名的连接池。
///
/// Thread-safe: multiple concurrent calls for the same db_alias will
/// correctly share the same pool (only one pool is created).
/// 线程安全：对同一 db_alias 的多个并发调用将正确共享同一连接池
/// （只创建一个连接池）。
///
/// Parameters / 参数:
///   db_alias: Database alias from Django DATABASES (e.g., "default")
///             Django DATABASES 中的数据库别名
///   config:   Connection parameters (ENGINE, HOST, PORT, etc.)
///             连接参数
///
/// Returns / 返回:
///   AnyPool (sqlx's dialect-agnostic connection pool)
///   AnyPool（sqlx 的方言无关连接池）
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
