# -*- coding: utf-8 -*-
import json
from typing import Any, Dict


def extract_databases() -> Dict[str, Any]:
    """
    Extract DATABASES from django.conf.settings.
    从 django.conf.settings 中提取 DATABASES 配置。

    Keeps only the fields needed by Rust for establishing database connections.
    The OPTIONS dict is passed through as-is for driver-specific config.
    仅保留 Rust 建立数据库连接所需的字段。
    OPTIONS 字典按原样传递，用于驱动程序特定的配置。
    """
    from django.conf import settings

    result = {}
    for alias, config in settings.DATABASES.items():
        result[alias] = {
            "ENGINE": config.get("ENGINE", ""),         # Database backend / 数据库后端
            "NAME": config.get("NAME", ""),             # Database name or file path / 数据库名或文件路径
            "USER": config.get("USER", ""),             # Connection username / 连接用户名
            "PASSWORD": config.get("PASSWORD", ""),     # Connection password / 连接密码
            "HOST": config.get("HOST", ""),             # Database host / 数据库主机
            "PORT": str(config.get("PORT", "")),        # Database port (converted to string) / 数据库端口（转为字符串）
            "OPTIONS": config.get("OPTIONS", {}),       # Driver-specific options / 驱动程序特定选项
        }
    return result


def extract_django_settings() -> Dict[str, Any]:
    """
    Extract datetime/timezone-related settings from Django and DRF.
    从 Django 和 DRF 中提取日期时间/时区相关设置。

    Priority for format strings (highest to lowest):
    格式字符串的优先级（从高到低）：
      1. REST_FRAMEWORK setting (e.g., REST_FRAMEWORK['DATETIME_FORMAT'])
         REST_FRAMEWORK 设置
      2. Django setting (e.g., settings.DATETIME_FORMAT)
         Django 设置
      3. Sensible defaults for JSON APIs (ISO 8601 / common formats)
         JSON API 的合理默认值（ISO 8601 / 常用格式）

    Note: Django's default format strings (like "N j, Y, P") are human-readable
    but not suitable for JSON APIs, so we fall back to ISO 8601 in those cases.
    注意：Django 的默认格式字符串（如 "N j, Y, P"）是人类可读的，
    但不适合 JSON API，因此在这些情况下回退到 ISO 8601。
    """
    from django.conf import settings
    from rest_framework.settings import api_settings

    # Use DRF's api_settings which already handles the priority chain:
    # REST_FRAMEWORK setting > DRF default ('iso-8601' for all three formats).
    # 使用 DRF 的 api_settings，它已经处理了优先级链：
    # REST_FRAMEWORK 设置 > DRF 默认值（三种格式都是 'iso-8601'）。

    # DATETIME_FORMAT: DRF api_settings (defaults to 'iso-8601')
    # DATETIME_FORMAT：DRF api_settings（默认为 'iso-8601'）
    # When 'iso-8601', Rust produces datetime.isoformat()-style output:
    #   USE_TZ=True:  "2025-10-12T19:31:30.101286+08:00"
    #   USE_TZ=False: "2025-10-12T19:31:30.101286"
    # 当为 'iso-8601' 时，Rust 产生 datetime.isoformat() 风格的输出
    datetime_format = api_settings.DATETIME_FORMAT
    if datetime_format is None:
        datetime_format = "iso-8601"

    # DATE_FORMAT: DRF api_settings (defaults to 'iso-8601')
    # DATE_FORMAT：DRF api_settings（默认为 'iso-8601'）
    date_format = api_settings.DATE_FORMAT
    if date_format is None:
        date_format = "iso-8601"

    # TIME_FORMAT: DRF api_settings (defaults to 'iso-8601')
    # TIME_FORMAT：DRF api_settings（默认为 'iso-8601'）
    time_format = api_settings.TIME_FORMAT
    if time_format is None:
        time_format = "iso-8601"

    return {
        "USE_TZ": getattr(settings, "USE_TZ", False),      # Whether to enable timezone conversion / 是否启用时区转换
        "TIME_ZONE": getattr(settings, "TIME_ZONE", "UTC"), # Target timezone for conversion / 转换的目标时区
        "DATETIME_FORMAT": datetime_format,                  # strftime format for datetime / datetime 的 strftime 格式
        "DATE_FORMAT": date_format,                          # strftime format for date / date 的 strftime 格式
        "TIME_FORMAT": time_format,                          # strftime format for time / time 的 strftime 格式
    }


def databases_to_json() -> str:
    """
    Return DATABASES config as JSON string (Parameter 3 for Rust).
    返回 DATABASES 配置的 JSON 字符串（Rust 的参数 3）。
    """
    return json.dumps(extract_databases(), default=str)


def settings_to_json() -> str:
    """
    Return Django settings as JSON string (Parameter 4 for Rust).
    返回 Django 设置的 JSON 字符串（Rust 的参数 4）。
    """
    return json.dumps(extract_django_settings(), default=str)
