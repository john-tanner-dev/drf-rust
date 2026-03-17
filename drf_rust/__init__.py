# -*- coding: utf-8 -*-
import warnings

# Export the two main serializer classes
# 导出两个主要的序列化器类
from .serializers import RustModelSerializer, RustListSerializer

__all__ = ["RustModelSerializer", "RustListSerializer"]
__version__ = "0.1.0"

try:
    from . import rust_engine  # noqa: F401
except ImportError:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured(
        "drf_rust requires the 'rust_engine' extension module. "
        "Please build it with: cd rust_engine && maturin develop --release"
    )
