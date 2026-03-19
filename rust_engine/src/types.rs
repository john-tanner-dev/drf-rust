// ============================================================================
// types.rs — Core type definitions for the Rust engine.
// types.rs — Rust 引擎的核心类型定义。
//
// This module defines:
// 本模块定义：
//   - TypedValue: Enum representing all possible field values returned to Python.
//     TypedValue：枚举，表示返回给 Python 的所有可能字段值。
//   - DbConfig: Database connection configuration (deserialized from DATABASES JSON).
//     DbConfig：数据库连接配置（从 DATABASES JSON 反序列化）。
//   - DbDialect: Enum for database engine types (PostgreSQL, MySQL, SQLite).
//     DbDialect：数据库引擎类型枚举。
//   - DjangoSettings: DateTime/timezone settings (deserialized from settings JSON).
//     DjangoSettings：日期时间/时区设置（从 settings JSON 反序列化）。
// ============================================================================

use serde::Deserialize;
use std::collections::HashMap;

/// Typed value enum — all possible return types for serialized fields.
/// 类型化值枚举 — 序列化字段的所有可能返回类型。
///
/// Maps directly to Python native types; no PyAny needed.
/// 直接映射到 Python 原生类型；不需要 PyAny。
///
/// Design rule: Each variant corresponds to exactly one Python type:
/// 设计规则：每个变体恰好对应一个 Python 类型：
///   None  → Python None
///   Bool  → Python bool
///   Int   → Python int
///   Float → Python float
///   Str   → Python str
///   List  → Python list[dict]  (for prefetch/M2M nested results / 用于预取/M2M 嵌套结果)
///   Json  → Python native       (for JSONField / 用于 JSONField)
#[derive(Debug, Clone)]
pub enum TypedValue {
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// Nested list of dicts (for prefetch / M2M fields)
    /// 嵌套的字典列表（用于预取/M2M 字段）
    List(Vec<HashMap<String, TypedValue>>),
    /// Raw JSON value (for JSONField)
    /// 原始 JSON 值（用于 JSONField）
    Json(serde_json::Value),
}

/// Database connection config, deserialized from DATABASES JSON (Parameter 3).
/// 数据库连接配置，从 DATABASES JSON 反序列化（参数 3）。
///
/// Fields match Django's DATABASES setting structure.
/// 字段匹配 Django 的 DATABASES 设置结构。
#[derive(Debug, Clone, Deserialize)]
pub struct DbConfig {
    /// Database engine string (e.g., "django.db.backends.postgresql")
    /// 数据库引擎字符串（如 "django.db.backends.postgresql"）
    #[serde(rename = "ENGINE")]
    pub engine: String,

    /// Database name or file path (for SQLite)
    /// 数据库名或文件路径（SQLite 用）
    #[serde(rename = "NAME")]
    pub name: String,

    /// Connection username / 连接用户名
    #[serde(rename = "USER", default)]
    pub user: String,

    /// Connection password / 连接密码
    #[serde(rename = "PASSWORD", default)]
    pub password: String,

    /// Database host address / 数据库主机地址
    #[serde(rename = "HOST", default)]
    pub host: String,

    /// Database port (as string) / 数据库端口（字符串形式）
    #[serde(rename = "PORT", default)]
    pub port: String,

    /// Driver-specific options / 驱动程序特定选项
    #[serde(rename = "OPTIONS", default)]
    pub options: HashMap<String, serde_json::Value>,
}

impl DbConfig {
    /// Detect database dialect from the ENGINE string.
    /// 从 ENGINE 字符串检测数据库方言。
    ///
    /// Matches against known Django backend patterns:
    /// 匹配已知的 Django 后端模式：
    ///   - "postgresql" or "postgis" → PostgreSQL
    ///   - "mysql" → MySQL
    ///   - "sqlite" → SQLite
    pub fn dialect(&self) -> DbDialect {
        let engine = self.engine.to_lowercase();
        if engine.contains("postgresql") || engine.contains("postgis") {
            DbDialect::PostgreSQL
        } else if engine.contains("mysql") {
            DbDialect::MySQL
        } else if engine.contains("sqlite") {
            DbDialect::SQLite
        } else {
            DbDialect::Unknown
        }
    }

    /// Build connection DSN (Data Source Name) string for sqlx.
    /// 构建 sqlx 的连接 DSN（数据源名称）字符串。
    ///
    /// Format per dialect / 各方言的格式:
    ///   PostgreSQL: postgres://user:pass@host:port/dbname
    ///   MySQL:      mysql://user:pass@host:port/dbname
    ///   SQLite:     sqlite:///path/to/db
    pub fn to_dsn(&self) -> String {
        match self.dialect() {
            DbDialect::PostgreSQL => {
                let host = if self.host.is_empty() { "localhost" } else { &self.host };
                let port = if self.port.is_empty() { "5432" } else { &self.port };
                format!(
                    "postgres://{}:{}@{}:{}/{}",
                    urlencoding(&self.user),
                    urlencoding(&self.password),
                    host,
                    port,
                    urlencoding(&self.name),
                )
            }
            DbDialect::MySQL => {
                let host = if self.host.is_empty() { "localhost" } else { &self.host };
                let port = if self.port.is_empty() { "3306" } else { &self.port };
                format!(
                    "mysql://{}:{}@{}:{}/{}",
                    urlencoding(&self.user),
                    urlencoding(&self.password),
                    host,
                    port,
                    urlencoding(&self.name),
                )
            }
            DbDialect::SQLite => {
                // SQLite uses file path directly
                // SQLite 直接使用文件路径
                format!("sqlite://{}", &self.name)
            }
            DbDialect::Unknown => {
                format!("unknown://{}", &self.name)
            }
        }
    }
}

/// Database dialect enum — identifies the type of database engine.
/// 数据库方言枚举 — 标识数据库引擎类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbDialect {
    PostgreSQL,
    MySQL,
    SQLite,
    Unknown,
}

/// Django settings for datetime/timezone handling (Parameter 4).
/// 用于日期时间/时区处理的 Django 设置（参数 4）。
///
/// These settings control how Rust formats datetime values in the output:
/// 这些设置控制 Rust 如何格式化输出中的日期时间值：
///   - USE_TZ: If true, datetime values are assumed UTC and converted to TIME_ZONE.
///     如果为 true，日期时间值被假定为 UTC 并转换到 TIME_ZONE。
///   - TIME_ZONE: Target timezone for conversion (e.g., "Asia/Shanghai").
///     转换的目标时区（如 "Asia/Shanghai"）。
///   - DATETIME_FORMAT, DATE_FORMAT, TIME_FORMAT: Python strftime format strings.
///     Python strftime 格式字符串。
#[derive(Debug, Clone, Deserialize)]
pub struct DjangoSettings {
    #[serde(rename = "USE_TZ", default)]
    pub use_tz: bool,

    #[serde(rename = "TIME_ZONE", default = "default_timezone")]
    pub time_zone: String,

    #[serde(rename = "DATETIME_FORMAT", default = "default_datetime_format")]
    pub datetime_format: String,

    #[serde(rename = "DATE_FORMAT", default = "default_date_format")]
    pub date_format: String,

    #[serde(rename = "TIME_FORMAT", default = "default_time_format")]
    pub time_format: String,
}

// Default value functions for serde deserialization
// serde 反序列化的默认值函数

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_datetime_format() -> String {
    "%Y-%m-%dT%H:%M:%S%.f".to_string()
}

fn default_date_format() -> String {
    "%Y-%m-%d".to_string()
}

fn default_time_format() -> String {
    "%H:%M:%S".to_string()
}

/// Simple URL encoding for DSN components.
/// DSN 组件的简单 URL 编码。
///
/// Encodes special characters that would break the DSN URL format:
/// 编码会破坏 DSN URL 格式的特殊字符：
///   : / @ ? # % (space)
fn urlencoding(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            ':' => result.push_str("%3A"),
            '/' => result.push_str("%2F"),
            '@' => result.push_str("%40"),
            '?' => result.push_str("%3F"),
            '#' => result.push_str("%23"),
            '%' => result.push_str("%25"),
            ' ' => result.push_str("%20"),
            _ => result.push(c),
        }
    }
    result
}
