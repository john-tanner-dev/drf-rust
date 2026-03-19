# -*- coding: utf-8 -*-
"""
drf_rust - DRF ModelSerializer read path accelerated by Rust.
drf_rust - 使用 Rust 加速 DRF ModelSerializer 的读取路径。

This package provides RustModelSerializer as a drop-in replacement for DRF's
ModelSerializer. The read path (to_representation) is delegated to a Rust
extension module for significantly faster serialization performance.
本包提供 RustModelSerializer 作为 DRF ModelSerializer 的直接替代品。
读取路径 (to_representation) 被委托给 Rust 扩展模块，以显著提升序列化性能。

Usage / 用法:
    from drf_rust import RustModelSerializer

    class MySerializer(RustModelSerializer):
        class Meta:
            model = MyModel
            fields = ['id', 'name', ...]
"""
import warnings

# Export the two main serializer classes
# 导出两个主要的序列化器类
from .serializers import RustModelSerializer, RustListSerializer

__all__ = ["RustModelSerializer", "RustListSerializer"]
__version__ = "0.1.0"

# Verify Rust engine is available at import time.
# If the Rust extension module is not built/installed, raise ImproperlyConfigured
# immediately so the developer knows to build it.
# 在导入时验证 Rust 引擎是否可用。
# 如果 Rust 扩展模块未构建/安装，立即抛出 ImproperlyConfigured 异常，
# 以便开发者知道需要先构建它。
try:
    from . import rust_engine  # noqa: F401
except ImportError:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured(
        "drf_rust requires the 'rust_engine' extension module. "
        "Please build it with: cd rust_engine && maturin develop --release"
    )
