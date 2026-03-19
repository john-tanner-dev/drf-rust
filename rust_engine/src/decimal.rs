// ============================================================================
// decimal.rs — DecimalField precision handling.
// decimal.rs — DecimalField 精度处理。
//
// Django's DecimalField has three key parameters:
// Django 的 DecimalField 有三个关键参数：
//   - decimal_places: Number of digits after the decimal point (e.g., 2 → "123.45").
//     小数点后的位数（如 2 → "123.45"）。
//   - max_digits: Maximum total number of digits (not enforced here).
//     最大总位数（此处不强制执行）。
//   - coerce_to_string: If true, return as string; if false, return as float.
//     如果为 true，返回字符串；如果为 false，返回浮点数。
//
// DRF's default is coerce_to_string=True (controlled by COERCE_DECIMAL_TO_STRING).
// DRF 的默认值是 coerce_to_string=True（由 COERCE_DECIMAL_TO_STRING 控制）。
// This matches Python's Decimal("123.45") → "123.45" behavior.
// 这匹配 Python 的 Decimal("123.45") → "123.45" 行为。
// ============================================================================

use crate::schema::SqlField;
use crate::types::TypedValue;

/// Format a decimal value from a raw string according to DecimalField parameters.
/// 根据 DecimalField 参数从原始字符串格式化十进制值。
///
/// Input is the raw string from the database (e.g., "123.456789").
/// 输入是来自数据库的原始字符串（如 "123.456789"）。
/// Output is either TypedValue::Str (if coerce_to_string) or TypedValue::Float.
/// 输出是 TypedValue::Str（如果 coerce_to_string）或 TypedValue::Float。
///
/// Examples / 示例:
///   - decimal_places=2, coerce_to_string=true:  "123.456" → Str("123.46")
///   - decimal_places=2, coerce_to_string=false: "123.456" → Float(123.456)
///   - Empty string → TypedValue::None
///     空字符串 → TypedValue::None
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

/// Format a decimal from a float value (when database returns numeric type).
/// 从浮点值格式化十进制（当数据库返回数值类型时）。
///
/// Same logic as format_decimal, but input is already a float.
/// 与 format_decimal 相同的逻辑，但输入已经是浮点数。
///
/// This handles cases where sqlx returns the value as f64 or i64 instead
/// of a string (common with SQLite and some PostgreSQL configurations).
/// 这处理 sqlx 以 f64 或 i64 而非字符串返回值的情况
/// （常见于 SQLite 和某些 PostgreSQL 配置）。
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
