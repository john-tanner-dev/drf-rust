use sqlx::any::AnyRow;
use sqlx::{Column, Row, ValueRef};
use std::collections::HashMap;

use crate::datetime;
use crate::decimal;
use crate::pool;
use crate::schema::{Schema, SqlField, PrefetchField};
use crate::types::{DbConfig, DjangoSettings, TypedValue};


pub async fn execute_all(
    schema: &Schema,
    sql_map: &HashMap<String, String>,
    databases: &HashMap<String, DbConfig>,
    settings: &DjangoSettings,
) -> Result<Vec<HashMap<String, TypedValue>>, String> {
    // Get the main SQL from the sql_map
    // 从 sql_map 获取主 SQL
    let main_sql = sql_map.get(&schema.primary_db)
        .ok_or_else(|| format!("No SQL for primary_db '{}'", schema.primary_db))?;

    // Get database config for connection
    // 获取用于连接的数据库配置
    let db_config = databases.get(&schema.primary_db)
        .ok_or_else(|| format!("No database config for '{}'", schema.primary_db))?;

    // Get or create connection pool (cached per db_alias)
    // 获取或创建连接池（按 db_alias 缓存）
    let pool = pool::get_or_create_pool(&schema.primary_db, db_config).await?;

    // Execute main SQL query
    // 执行主 SQL 查询
    let main_rows: Vec<AnyRow> = sqlx::query(main_sql)
        .fetch_all(&pool)
        .await
        .map_err(|e| format!("Main SQL execution failed: {}", e))?;

    // Convert each row to a HashMap<String, TypedValue> based on field schema
    // 根据字段 schema 将每行转换为 HashMap<String, TypedValue>
    let mut results: Vec<HashMap<String, TypedValue>> = Vec::with_capacity(main_rows.len());
    for row in &main_rows {
        let record = row_to_typed_map(row, &schema.sql_fields, settings)?;
        results.push(record);
    }

    // --- Handle prefetch fields (M2M / reverse FK) ---
    // --- 处理预取字段（M2M / 反向 FK）---
    if !schema.prefetch_fields.is_empty() && !results.is_empty() {
        // Extract pk values from main results for the IN clause
        // 从主结果中提取 pk 值用于 IN 子句
        let pk_values: Vec<String> = main_rows.iter()
            .filter_map(|row| extract_string_value(row, "pk").ok())
            .collect();

        if !pk_values.is_empty() {
            // Execute all prefetch queries concurrently via tokio::spawn
            // 通过 tokio::spawn 并发执行所有预取查询
            let prefetch_results = execute_prefetch_fields(
                &schema.prefetch_fields,
                &pk_values,
                databases,
                settings,
            ).await?;

            // Merge prefetch results into main results by matching pk
            // 通过匹配 pk 将预取结果合并到主结果中
            for record in &mut results {
                // Get the pk value of this record for matching
                // 获取此记录的 pk 值用于匹配
                let record_pk = match record.get("pk") {
                    Some(TypedValue::Int(v)) => v.to_string(),
                    Some(TypedValue::Str(v)) => v.clone(),
                    _ => continue,
                };

                // For each prefetch field, find the child records matching this parent pk
                // 对于每个预取字段，找到匹配此父级 pk 的子记录
                for (field_name, grouped) in &prefetch_results {
                    let items = grouped.get(&record_pk).cloned().unwrap_or_default();
                    record.insert(field_name.clone(), TypedValue::List(items));
                }
            }
        }
    }

    // --- Remove internal columns from results ---
    // --- 从结果中移除内部列 ---
    // These columns are used internally (pk for prefetch matching, __*_pk for python filling)
    // but should not appear in the final Python output.
    // 这些列在内部使用（pk 用于预取匹配，__*_pk 用于 Python 填充），
    // 但不应出现在最终的 Python 输出中。
    for record in &mut results {
        record.remove("pk");
        // Remove any internal pk columns for nested serializers
        // 移除嵌套序列化器的内部 pk 列
        let internal_keys: Vec<String> = record.keys()
            .filter(|k| k.starts_with("__") && k.ends_with("_pk"))
            .cloned()
            .collect();
        for key in internal_keys {
            record.remove(&key);
        }
    }

    Ok(results)
}


async fn execute_prefetch_fields(
    prefetch_fields: &[PrefetchField],
    parent_pks: &[String],
    databases: &HashMap<String, DbConfig>,
    settings: &DjangoSettings,
) -> Result<HashMap<String, HashMap<String, Vec<HashMap<String, TypedValue>>>>, String> {
    let mut handles = Vec::new();

    // Spawn a concurrent task for each prefetch field
    // 为每个预取字段生成一个并发任务
    for field in prefetch_fields {
        let field = field.clone();
        let parent_pks = parent_pks.to_vec();
        let databases = databases.clone();
        let settings = settings.clone();

        handles.push(tokio::spawn(async move {
            execute_one_prefetch(&field, &parent_pks, &databases, &settings).await
        }));
    }

    // Collect all results
    // 收集所有结果
    let mut all_results = HashMap::new();
    for handle in handles {
        let (field_name, grouped) = handle.await
            .map_err(|e| format!("Prefetch task panicked: {}", e))?
            .map_err(|e| format!("Prefetch execution failed: {}", e))?;
        all_results.insert(field_name, grouped);
    }

    Ok(all_results)
}


async fn execute_one_prefetch(
    field: &PrefetchField,
    parent_pks: &[String],
    databases: &HashMap<String, DbConfig>,
    settings: &DjangoSettings,
) -> Result<(String, HashMap<String, Vec<HashMap<String, TypedValue>>>), String> {
    // Fill {ids} placeholder with comma-separated parent pk values
    // 用逗号分隔的父级 pk 值填充 {ids} 占位符
    let ids_str = parent_pks.join(",");
    let sql = field.prefetch_sql_template.replace("{ids}", &ids_str);

    // Determine which database to use (from child schema)
    // 确定使用哪个数据库（来自子 schema）
    let db_alias = &field.child_schema.primary_db;
    let db_config = databases.get(db_alias)
        .ok_or_else(|| format!("No database config for prefetch db '{}'", db_alias))?;

    // Get or create connection pool for the child's database
    // 获取或创建子项数据库的连接池
    let pool = pool::get_or_create_pool(db_alias, db_config).await?;

    // Execute the prefetch SQL
    // 执行预取 SQL
    let rows: Vec<AnyRow> = sqlx::query(&sql)
        .fetch_all(&pool)
        .await
        .map_err(|e| format!("Prefetch SQL execution failed for '{}': {}", field.name, e))?;

    // Convert rows and group by join key
    // 转换行并按 join_key 分组
    let mut grouped: HashMap<String, Vec<HashMap<String, TypedValue>>> = HashMap::new();
    for row in &rows {
        // Extract the join key value (maps this child row to its parent)
        // 提取 join_key 值（将此子行映射到其父项）
        let join_key_value = extract_string_value(row, &field.join_key)
            .unwrap_or_default();

        // Convert the row to a typed map using child schema's sql_fields
        // 使用子 schema 的 sql_fields 将行转换为类型化映射
        let mut record = row_to_typed_map(row, &field.child_schema.sql_fields, settings)?;

        // Remove internal columns from child record (not for Python output)
        // 从子记录中移除内部列（不用于 Python 输出）
        record.remove("__prefetch_join_key");
        record.remove("pk");

        // Group by parent pk (join_key_value)
        // 按父级 pk 分组（join_key_value）
        grouped.entry(join_key_value)
            .or_default()
            .push(record);
    }

    Ok((field.name.clone(), grouped))
}


fn row_to_typed_map(
    row: &AnyRow,
    fields: &[SqlField],
    settings: &DjangoSettings,
) -> Result<HashMap<String, TypedValue>, String> {
    let mut map = HashMap::new();
    let columns = row.columns();

    for col in columns {
        let col_name = col.name();

        // Check if this column matches a schema-defined field (by alias)
        // 检查此列是否匹配 schema 定义的字段（按别名）
        if let Some(field) = fields.iter().find(|f| f.alias == col_name) {
            // Convert using field_type-specific logic
            // 使用 field_type 特定逻辑转换
            let value = convert_column_value(row, col_name, field, settings)?;
            map.insert(field.name.clone(), value);
        } else {
            // Internal column (pk, __prefetch_join_key, etc.) — store as-is
            // 内部列（pk、__prefetch_join_key 等）— 按原样存储
            let value = extract_generic_value(row, col_name)?;
            map.insert(col_name.to_string(), value);
        }
    }

    Ok(map)
}


fn convert_column_value(
    row: &AnyRow,
    col_name: &str,
    field: &SqlField,
    settings: &DjangoSettings,
) -> Result<TypedValue, String> {
    // Check for NULL first — applies to all field types
    // 首先检查 NULL — 适用于所有字段类型
    let raw_ref = row.try_get_raw(col_name)
        .map_err(|e| format!("Failed to get column '{}': {}", col_name, e))?;
    if raw_ref.is_null() {
        return Ok(TypedValue::None);
    }

    let field_type = field.field_type.as_str();
    match field_type {
        // --- Integer types ---
        // --- 整数类型 ---
        "IntegerField" | "BigIntegerField" | "SmallIntegerField"
        | "AutoField" | "BigAutoField" | "SmallAutoField"
        | "PositiveIntegerField" | "PositiveBigIntegerField" | "PositiveSmallIntegerField" => {
            // Try i64 first (most common), then i32, then parse from string
            // 先尝试 i64（最常见），然后 i32，最后从字符串解析
            if let Ok(v) = row.try_get::<i64, _>(col_name) {
                Ok(TypedValue::Int(v))
            } else if let Ok(v) = row.try_get::<i32, _>(col_name) {
                Ok(TypedValue::Int(v as i64))
            } else {
                let v: String = row.try_get(col_name)
                    .map_err(|e| format!("Int conversion failed for '{}': {}", col_name, e))?;
                Ok(TypedValue::Int(v.parse::<i64>().unwrap_or(0)))
            }
        }

        // --- Float type ---
        // --- 浮点类型 ---
        "FloatField" => {
            if let Ok(v) = row.try_get::<f64, _>(col_name) {
                Ok(TypedValue::Float(v))
            } else {
                let v: String = row.try_get(col_name)
                    .map_err(|e| format!("Float conversion failed for '{}': {}", col_name, e))?;
                Ok(TypedValue::Float(v.parse::<f64>().unwrap_or(0.0)))
            }
        }

        // --- Decimal type (precision-aware) ---
        // --- 十进制类型（精度感知）---
        "DecimalField" => {
            // sqlx Any returns decimal as string, f64, or i64 (SQLite integer)
            // sqlx Any 以字符串、f64 或 i64 返回十进制（SQLite 整数）
            if let Ok(v) = row.try_get::<String, _>(col_name) {
                Ok(decimal::format_decimal(&v, field))
            } else if let Ok(v) = row.try_get::<f64, _>(col_name) {
                Ok(decimal::format_decimal_from_f64(v, field))
            } else if let Ok(v) = row.try_get::<i64, _>(col_name) {
                Ok(decimal::format_decimal_from_f64(v as f64, field))
            } else if let Ok(v) = row.try_get::<i32, _>(col_name) {
                Ok(decimal::format_decimal_from_f64(v as f64, field))
            } else {
                Ok(TypedValue::Str("0".to_string()))
            }
        }

        // --- Boolean type ---
        // --- 布尔类型 ---
        "BooleanField" | "NullBooleanField" => {
            if let Ok(v) = row.try_get::<bool, _>(col_name) {
                Ok(TypedValue::Bool(v))
            } else if let Ok(v) = row.try_get::<i64, _>(col_name) {
                // SQLite stores booleans as integers (0/1)
                // SQLite 将布尔值存储为整数（0/1）
                Ok(TypedValue::Bool(v != 0))
            } else {
                Ok(TypedValue::Bool(false))
            }
        }

        // --- String types ---
        // --- 字符串类型 ---
        "CharField" | "TextField" | "EmailField" | "URLField" | "SlugField"
        | "FilePathField" | "IPAddressField" | "GenericIPAddressField"
        | "FileField" | "ImageField" => {
            let v: String = row.try_get(col_name)
                .map_err(|e| format!("String conversion failed for '{}': {}", col_name, e))?;
            Ok(TypedValue::Str(v))
        }

        // --- UUID type (stored as string in most backends) ---
        // --- UUID 类型（大多数后端存储为字符串）---
        "UUIDField" => {
            let v: String = row.try_get(col_name)
                .map_err(|e| format!("UUID conversion failed for '{}': {}", col_name, e))?;
            Ok(TypedValue::Str(v))
        }

        // --- DateTime type (parsed and formatted according to Django settings) ---
        // --- 日期时间类型（根据 Django 设置解析和格式化）---
        "DateTimeField" => {
            let v: String = row.try_get(col_name)
                .map_err(|e| format!("DateTime conversion failed for '{}': {}", col_name, e))?;
            if let Some(dt) = datetime::parse_datetime_str(&v) {
                // Parse succeeded → format with timezone conversion if USE_TZ
                // 解析成功 → 如果 USE_TZ 则带时区转换格式化
                Ok(TypedValue::Str(datetime::format_datetime(dt, settings)))
            } else {
                // Parse failed → return raw string
                // 解析失败 → 返回原始字符串
                Ok(TypedValue::Str(v))
            }
        }

        // --- Date type ---
        // --- 日期类型 ---
        "DateField" => {
            let v: String = row.try_get(col_name)
                .map_err(|e| format!("Date conversion failed for '{}': {}", col_name, e))?;
            if let Some(d) = datetime::parse_date_str(&v) {
                Ok(TypedValue::Str(datetime::format_date(d, settings)))
            } else {
                // Try to extract date from datetime string (first 10 chars: YYYY-MM-DD)
                // 尝试从日期时间字符串中提取日期（前 10 个字符：YYYY-MM-DD）
                if v.len() >= 10 {
                    if let Some(d) = datetime::parse_date_str(&v[..10]) {
                        return Ok(TypedValue::Str(datetime::format_date(d, settings)));
                    }
                }
                Ok(TypedValue::Str(v))
            }
        }

        // --- Time type ---
        // --- 时间类型 ---
        "TimeField" => {
            let v: String = row.try_get(col_name)
                .map_err(|e| format!("Time conversion failed for '{}': {}", col_name, e))?;
            if let Some(t) = datetime::parse_time_str(&v) {
                Ok(TypedValue::Str(datetime::format_time(t, settings)))
            } else {
                Ok(TypedValue::Str(v))
            }
        }

        // --- JSON type (parse to serde_json::Value for rich Python conversion) ---
        // --- JSON 类型（解析为 serde_json::Value 以进行丰富的 Python 转换）---
        "JSONField" => {
            let v: String = row.try_get(col_name)
                .map_err(|e| format!("JSON conversion failed for '{}': {}", col_name, e))?;
            let json_val: serde_json::Value = serde_json::from_str(&v)
                .unwrap_or(serde_json::Value::String(v));  // Fallback: treat as string / 回退：视为字符串
            Ok(TypedValue::Json(json_val))
        }

        // --- Binary type (base64 encode for safe string representation) ---
        // --- 二进制类型（base64 编码为安全的字符串表示）---
        "BinaryField" => {
            let v: Vec<u8> = row.try_get(col_name)
                .map_err(|e| format!("Binary conversion failed for '{}': {}", col_name, e))?;
            let encoded = base64_encode(&v);
            Ok(TypedValue::Str(encoded))
        }

        // --- Unknown field type: best-effort extraction ---
        // --- 未知字段类型：尽力提取 ---
        _ => {
            if let Ok(v) = row.try_get::<String, _>(col_name) {
                Ok(TypedValue::Str(v))
            } else if let Ok(v) = row.try_get::<i64, _>(col_name) {
                Ok(TypedValue::Str(v.to_string()))
            } else if let Ok(v) = row.try_get::<f64, _>(col_name) {
                Ok(TypedValue::Str(v.to_string()))
            } else {
                // Last resort: return empty string
                // 最后手段：返回空字符串
                Ok(TypedValue::Str(String::new()))
            }
        }
    }
}


fn extract_string_value(row: &AnyRow, col_name: &str) -> Result<String, String> {
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Ok(v.to_string());
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Ok(v.to_string());
    }
    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Ok(v);
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return Ok(v.to_string());
    }
    Err(format!("Cannot extract string value from column '{}'", col_name))
}


fn extract_generic_value(row: &AnyRow, col_name: &str) -> Result<TypedValue, String> {
    // Check for NULL / 检查 NULL
    let raw_ref = row.try_get_raw(col_name)
        .map_err(|e| format!("Failed to get column '{}': {}", col_name, e))?;
    if raw_ref.is_null() {
        return Ok(TypedValue::None);
    }
    // Try types in most-likely order / 按最可能的顺序尝试类型
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Ok(TypedValue::Int(v));
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Ok(TypedValue::Int(v as i64));
    }
    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Ok(TypedValue::Str(v));
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return Ok(TypedValue::Float(v));
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Ok(TypedValue::Bool(v));
    }
    // Fallback: empty string / 回退：空字符串
    Ok(TypedValue::Str(String::new()))
}


fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    // Process 3 bytes at a time → 4 base64 characters
    // 每次处理 3 个字节 → 4 个 base64 字符
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');  // Padding / 填充
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');  // Padding / 填充
        }
    }
    result
}
