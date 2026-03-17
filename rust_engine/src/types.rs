use serde::Deserialize;
use std::collections::HashMap;


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


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbDialect {
    PostgreSQL,
    MySQL,
    SQLite,
    Unknown,
}


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
