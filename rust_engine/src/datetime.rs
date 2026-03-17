use chrono::{NaiveDate, NaiveDateTime, NaiveTime, DateTime, Utc};
use chrono_tz::Tz;

use crate::types::DjangoSettings;


pub fn python_format_to_chrono(fmt: &str) -> String {
    fmt.replace("%f", "%6f")
}


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


pub fn format_date(value: NaiveDate, settings: &DjangoSettings) -> String {
    if settings.date_format == "iso-8601" {
        return value.format("%Y-%m-%d").to_string();
    }
    let fmt = python_format_to_chrono(&settings.date_format);
    value.format(&fmt).to_string()
}


pub fn format_time(value: NaiveTime, settings: &DjangoSettings) -> String {
    if settings.time_format == "iso-8601" {
        return value.format("%H:%M:%S%.6f").to_string();
    }
    let fmt = python_format_to_chrono(&settings.time_format);
    value.format(&fmt).to_string()
}


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


pub fn parse_date_str(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()
}


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
