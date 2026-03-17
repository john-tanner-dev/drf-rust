use crate::schema::SqlField;
use crate::types::TypedValue;


pub fn format_decimal(raw: &str, field: &SqlField) -> TypedValue {
    let decimal_places = field.decimal_places.unwrap_or(2);
    let coerce_to_string = field.coerce_to_string.unwrap_or(true);

    // Parse the raw decimal string
    // 解析原始十进制字符串
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return TypedValue::None;
    }

    // Try to parse as f64 for formatting
    // 尝试解析为 f64 进行格式化
    let value: f64 = match trimmed.parse() {
        Ok(v) => v,
        Err(_) => {
            // If we can't parse, return as string (preserves original value)
            // 如果无法解析，返回字符串（保留原始值）
            return TypedValue::Str(trimmed.to_string());
        }
    };

    if coerce_to_string {
        // Format with exact decimal_places (e.g., 2 → "123.46")
        // 使用精确的 decimal_places 格式化（如 2 → "123.46"）
        TypedValue::Str(format!("{:.prec$}", value, prec = decimal_places as usize))
    } else {
        // Return as float (Python float, not string)
        // 返回浮点数（Python float，非字符串）
        TypedValue::Float(value)
    }
}


pub fn format_decimal_from_f64(value: f64, field: &SqlField) -> TypedValue {
    let decimal_places = field.decimal_places.unwrap_or(2);
    let coerce_to_string = field.coerce_to_string.unwrap_or(true);

    if coerce_to_string {
        // Format with exact decimal_places / 使用精确的 decimal_places 格式化
        TypedValue::Str(format!("{:.prec$}", value, prec = decimal_places as usize))
    } else {
        // Return as float / 返回浮点数
        TypedValue::Float(value)
    }
}
