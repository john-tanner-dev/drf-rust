use serde::Deserialize;
use std::collections::HashMap;


#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    /// Primary database alias (e.g., "default") — determines which connection pool to use.
    /// 主数据库别名（如 "default"）— 决定使用哪个连接池。
    pub primary_db: String,

    /// Fields that map directly to SQL columns (via JOINs for FK/O2O).
    /// 直接映射到 SQL 列的字段（通过 JOIN 处理 FK/O2O）。
    /// Rust reads these columns from the main SQL result rows.
    /// Rust 从主 SQL 结果行中读取这些列。
    pub sql_fields: Vec<SqlField>,

    /// Fields requiring separate prefetch queries (M2M / reverse relations).
    /// 需要单独预取查询的字段（M2M / 反向关系）。
    /// Each has its own SQL template with {ids} placeholder.
    /// 每个都有自己的带 {ids} 占位符的 SQL 模板。
    pub prefetch_fields: Vec<PrefetchField>,

    /// Field names that must be filled by Python (SerializerMethodField, callable source, etc.).
    /// 必须由 Python 填充的字段名（SerializerMethodField、可调用 source 等）。
    /// Rust ignores these — Python fills them after Rust returns.
    /// Rust 忽略这些 — Python 在 Rust 返回后填充它们。
    pub python_only_fields: Vec<String>,

    /// Internal pk columns for nested serializers that have python_only_fields.
    /// 带有 python_only_fields 的嵌套序列化器的内部 pk 列。
    /// Key: internal alias (e.g., "__author_pk"), Value: column alias in SELECT.
    /// 键：内部别名，值：SELECT 中的列别名。
    #[serde(default)]
    pub internal_pks: HashMap<String, String>,
}


#[derive(Debug, Clone, Deserialize)]
pub struct SqlField {
    /// Serializer field name (used as dict key in Python output).
    /// 序列化器字段名（用作 Python 输出中的字典键）。
    pub name: String,

    /// SQL column alias in the SELECT clause (used to read from result row).
    /// SELECT 子句中的 SQL 列别名（用于从结果行读取）。
    pub alias: String,

    /// Whether the column is nullable (if true, check for NULL before conversion).
    /// 列是否可为空（如果为 true，转换前检查 NULL）。
    #[serde(default)]
    pub nullable: bool,

    /// Django model field type name (e.g., "CharField", "DecimalField", "DateTimeField").
    /// Django 模型字段类型名（如 "CharField"、"DecimalField"、"DateTimeField"）。
    /// Determines the conversion strategy in executor.rs.
    /// 决定 executor.rs 中的转换策略。
    pub field_type: String,

    /// DecimalField: number of decimal places (e.g., 2 for "123.45").
    /// DecimalField：小数位数（如 "123.45" 的 2）。
    #[serde(default)]
    pub decimal_places: Option<u32>,

    /// DecimalField: maximum total digits (e.g., 10).
    /// DecimalField：最大总位数（如 10）。
    #[serde(default)]
    pub max_digits: Option<u32>,

    /// DecimalField: whether to return as string (true) or float (false).
    /// DecimalField：是否返回字符串（true）还是浮点数（false）。
    /// Matches DRF's COERCE_DECIMAL_TO_STRING setting.
    /// 匹配 DRF 的 COERCE_DECIMAL_TO_STRING 设置。
    #[serde(default = "default_coerce_to_string")]
    pub coerce_to_string: Option<bool>,
}


fn default_coerce_to_string() -> Option<bool> {
    None
}


#[derive(Debug, Clone, Deserialize)]
pub struct PrefetchField {
    /// Serializer field name (e.g., "tags", "comments").
    /// 序列化器字段名（如 "tags"、"comments"）。
    pub name: String,

    /// SQL template with {ids} placeholder for parent pks.
    /// 带 {ids} 占位符的 SQL 模板，用于父级 pk。
    pub prefetch_sql_template: String,

    /// Column name used to group prefetch results back to parent records.
    /// 用于将预取结果分组回父记录的列名。
    /// This column appears in the prefetch SELECT as "__prefetch_join_key".
    /// 此列在预取 SELECT 中显示为 "__prefetch_join_key"。
    pub join_key: String,

    /// Schema for the child serializer (recursive — child may have its own prefetch_fields).
    /// 子序列化器的 Schema（递归 — 子项可能有自己的 prefetch_fields）。
    pub child_schema: Schema,
}
