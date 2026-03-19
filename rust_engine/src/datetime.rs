// ============================================================================
// datetime.rs — DateTime/Date/Time parsing and formatting.
// datetime.rs — 日期时间/日期/时间解析和格式化。
//
// This module handles the conversion of database datetime strings to
// Python-compatible formatted strings, respecting Django settings:
// 本模块处理数据库日期时间字符串到 Python 兼容格式字符串的转换，
// 遵循 Django 设置：
//
//   - USE_TZ: If true, values are assumed UTC and converted to TIME_ZONE.
//     如果为 true，值被假定为 UTC 并转换到 TIME_ZONE。
//   - TIME_ZONE: Target timezone for conversion (e.g., "Asia/Shanghai").
//     转换的目标时区（如 "Asia/Shanghai"）。
//   - DATETIME_FORMAT, DATE_FORMAT, TIME_FORMAT: Python strftime format strings.
//     Python strftime 格式字符串。
//
// The sqlx Any driver returns datetime/date/time as strings (not chrono types),
// so we must parse them from various database formats first.
// sqlx 的 Any 驱动以字符串形式返回日期时间（非 chrono 类型），
// 因此我们必须先从各种数据库格式解析它们。
// ============================================================================

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, DateTime, Utc};
use chrono_tz::Tz;

use crate::types::DjangoSettings;

/// Convert a Python strftime format string to a chrono format string.
/// 将 Python strftime 格式字符串转换为 chrono 格式字符串。
///
/// Most tokens are identical between Python and chrono, with one key difference:
/// 大多数标记在 Python 和 chrono 之间是相同的，一个关键区别是：
///   - Python %f = microseconds (6 digits)
///     Python %f = 微秒（6 位）
///   - chrono %f = nanoseconds (9 digits), %6f = microseconds (6 digits)
///     chrono %f = 纳秒（9 位），%6f = 微秒（6 位）
pub fn python_format_to_chrono(fmt: &str) -> String {
    fmt.replace("%f", "%6f")
}

/// Format a NaiveDateTime according to Django settings.
/// 根据 Django 设置格式化 NaiveDateTime。
///
/// If DATETIME_FORMAT is "iso-8601" (DRF's default):
/// 如果 DATETIME_FORMAT 是 "iso-8601"（DRF 的默认值）：
///   Produces Python's datetime.isoformat()-compatible output:
///   产生与 Python 的 datetime.isoformat() 兼容的输出：
///     USE_TZ=True:  "2025-10-12T19:31:30.101286+08:00"
///     USE_TZ=False: "2025-10-12T19:31:30.101286"
///
/// Otherwise, uses the strftime format string:
/// 否则，使用 strftime 格式字符串：
///   If USE_TZ is true:
///   如果 USE_TZ 为 true：
///     1. Assume the value is in UTC (as stored in the database).
///        假定值是 UTC（如数据库中存储的）。
///     2. Convert to the target TIME_ZONE (e.g., "Asia/Shanghai").
///        转换到目标 TIME_ZONE（如 "Asia/Shanghai"）。
///     3. Format with DATETIME_FORMAT.
///        使用 DATETIME_FORMAT 格式化。
///   If USE_TZ is false:
///   如果 USE_TZ 为 false：
///     Format the value as-is (no timezone conversion).
///     按原样格式化（不进行时区转换）。
pub fn format_datetime(
    value: NaiveDateTime,
    settings: &DjangoSettings,
) -> String {
    // Handle 'iso-8601' format (DRF's default DATETIME_FORMAT)
    // 处理 'iso-8601' 格式（DRF 的默认 DATETIME_FORMAT）
    if settings.datetime_format == "iso-8601" {
        if settings.use_tz {
            let utc_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(value, Utc);
            if let Ok(tz) = settings.time_zone.parse::<Tz>() {
                let local_dt = utc_dt.with_timezone(&tz);
                // ISO 8601 with timezone: "2025-10-12T19:31:30.101286+08:00"
                // ISO 8601 带时区
                return local_dt.format("%Y-%m-%dT%H:%M:%S%.6f%:z").to_string();
            }
            // Fallback to UTC / 回退到 UTC
            return utc_dt.format("%Y-%m-%dT%H:%M:%S%.6f%:z").to_string();
        }
        // No timezone: "2025-10-12T19:31:30.101286"
        // 无时区
        return value.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
    }

    let fmt = python_format_to_chrono(&settings.datetime_format);

    if settings.use_tz {
        // Treat value as UTC / 将值视为 UTC
        let utc_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(value, Utc);

        // Try to parse TIME_ZONE as a chrono_tz timezone (e.g., "Asia/Shanghai")
        // 尝试将 TIME_ZONE 解析为 chrono_tz 时区
        if let Ok(tz) = settings.time_zone.parse::<Tz>() {
            let local_dt = utc_dt.with_timezone(&tz);
            return local_dt.format(&fmt).to_string();
        }

        // Fallback: format as UTC if timezone parsing fails
        // 回退：如果时区解析失败则格式化为 UTC
        return utc_dt.format(&fmt).to_string();
    }

    // USE_TZ=False: format as-is (naive datetime, no timezone)
    // USE_TZ=False：按原样格式化（朴素日期时间，无时区）
    value.format(&fmt).to_string()
}

/// Format a NaiveDate according to Django settings.
/// 根据 Django 设置格式化 NaiveDate。
///
/// If DATE_FORMAT is "iso-8601", produces "YYYY-MM-DD".
/// 如果 DATE_FORMAT 是 "iso-8601"，产生 "YYYY-MM-DD"。
pub fn format_date(value: NaiveDate, settings: &DjangoSettings) -> String {
    if settings.date_format == "iso-8601" {
        return value.format("%Y-%m-%d").to_string();
    }
    let fmt = python_format_to_chrono(&settings.date_format);
    value.format(&fmt).to_string()
}

/// Format a NaiveTime according to Django settings.
/// 根据 Django 设置格式化 NaiveTime。
///
/// If TIME_FORMAT is "iso-8601", produces "HH:MM:SS.ffffff".
/// 如果 TIME_FORMAT 是 "iso-8601"，产生 "HH:MM:SS.ffffff"。
pub fn format_time(value: NaiveTime, settings: &DjangoSettings) -> String {
    if settings.time_format == "iso-8601" {
        return value.format("%H:%M:%S%.6f").to_string();
    }
    let fmt = python_format_to_chrono(&settings.time_format);
    value.format(&fmt).to_string()
}

/// Normalize a timezone offset in a datetime string for chrono parsing.
/// 规范化日期时间字符串中的时区偏移量以便 chrono 解析。
///
/// PostgreSQL CAST(timestamptz AS TEXT) produces short offsets like "+00" or "-05",
/// but chrono's %:z expects "+00:00" or "-05:00". This function normalizes them.
/// PostgreSQL 的 CAST(timestamptz AS TEXT) 产生短偏移量如 "+00" 或 "-05"，
/// 但 chrono 的 %:z 期望 "+00:00" 或 "-05:00"。此函数对其进行规范化。
///
/// Examples / 示例:
///   - "2025-10-12 11:31:30.101286+00" → "2025-10-12 11:31:30.101286+00:00"
///   - "2025-10-12 11:31:30+08" → "2025-10-12 11:31:30+08:00"
///   - "2025-10-12 11:31:30+08:00" → unchanged / 不变
///   - "2025-10-12 11:31:30" → unchanged / 不变
fn normalize_tz_offset(s: &str) -> String {
    let bytes = s.as_bytes();
    let len = bytes.len();

    // Look for patterns like "+HH" or "-HH" at the end (exactly 3 chars: sign + 2 digits)
    // 查找末尾的 "+HH" 或 "-HH" 模式（正好 3 个字符：符号 + 2 位数字）
    if len >= 3 {
        let sign = bytes[len - 3];
        if (sign == b'+' || sign == b'-')
            && bytes[len - 2].is_ascii_digit()
            && bytes[len - 1].is_ascii_digit()
        {
            // Short offset like "+00" → append ":00" to get "+00:00"
            // 短偏移量如 "+00" → 追加 ":00" 得到 "+00:00"
            return format!("{}:00", s);
        }
    }

    s.to_string()
}

/// Parse a database datetime string to NaiveDateTime.
/// 将数据库日期时间字符串解析为 NaiveDateTime。
///
/// Handles common formats across PostgreSQL, MySQL, SQLite:
/// 处理 PostgreSQL、MySQL、SQLite 的常见格式：
///   - ISO 8601 with T separator: "2024-01-15T10:30:00.123456"
///     ISO 8601 带 T 分隔符
///   - Space separator (MySQL style): "2024-01-15 10:30:00.123456"
///     空格分隔（MySQL 风格）
///   - Without fractional seconds: "2024-01-15T10:30:00"
///     无小数秒
///   - PostgreSQL timestamptz with offset: "2024-01-15 10:30:00+08:00"
///     PostgreSQL 带偏移量的 timestamptz
///   - PostgreSQL short offset: "2024-01-15 10:30:00+00" (normalized to "+00:00")
///     PostgreSQL 短偏移量（规范化为 "+00:00"）
pub fn parse_datetime_str(s: &str) -> Option<NaiveDateTime> {
    let s = s.trim();

    // Try ISO 8601 with T separator and fractional seconds
    // 尝试带 T 分隔符和小数秒的 ISO 8601
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt);
    }
    // Try space separator (MySQL style) with fractional seconds
    // 尝试空格分隔（MySQL 风格）带小数秒
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt);
    }
    // Without fractional seconds (T separator)
    // 无小数秒（T 分隔符）
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt);
    }
    // Without fractional seconds (space separator)
    // 无小数秒（空格分隔）
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt);
    }

    // Normalize short timezone offsets (e.g., "+00" → "+00:00") for chrono compatibility
    // 规范化短时区偏移量（如 "+00" → "+00:00"）以兼容 chrono
    let normalized = normalize_tz_offset(s);
    let s = normalized.as_str();

    // PostgreSQL timestamptz format with timezone offset
    // PostgreSQL timestamptz 格式带时区偏移
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
        return Some(dt.naive_utc());  // Convert to UTC naive / 转换为 UTC 朴素时间
    }
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%:z") {
        return Some(dt.naive_utc());
    }
    // PostgreSQL format with offset (without fractional seconds)
    // PostgreSQL 格式带偏移（无小数秒）
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%:z") {
        return Some(dt.naive_utc());
    }
    None
}

/// Parse a date string to NaiveDate.
/// 将日期字符串解析为 NaiveDate。
///
/// Expected format: "YYYY-MM-DD" (standard across all database backends).
/// 预期格式："YYYY-MM-DD"（所有数据库后端的标准格式）。
pub fn parse_date_str(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()
}

/// Parse a time string to NaiveTime.
/// 将时间字符串解析为 NaiveTime。
///
/// Handles with and without fractional seconds:
/// 处理有无小数秒的情况：
///   - "10:30:00.123456" (with microseconds / 带微秒)
///   - "10:30:00" (without / 不带)
pub fn parse_time_str(s: &str) -> Option<NaiveTime> {
    let s = s.trim();
    // Try with fractional seconds first
    // 先尝试带小数秒
    if let Ok(t) = NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
        return Some(t);
    }
    // Then without fractional seconds
    // 然后不带小数秒
    NaiveTime::parse_from_str(s, "%H:%M:%S").ok()
}
