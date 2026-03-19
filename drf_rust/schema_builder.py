# -*- coding: utf-8 -*-
"""
Build Schema JSON for the Rust engine from classification results.
从分类结果构建传递给 Rust 引擎的 Schema JSON。

The schema describes the field structure, types, and prefetch SQL for the Rust
engine to execute and build Python dict results. This is Parameter 1 of the
4 parameters passed to rust_engine.execute_serialization().
该 schema 描述了字段结构、类型和预取 SQL，供 Rust 引擎执行并构建 Python 字典结果。
这是传递给 rust_engine.execute_serialization() 的 4 个参数中的参数 1。

Schema structure / Schema 结构:
{
    "primary_db": "default",           -- Database alias / 数据库别名
    "sql_fields": [...],               -- Fields from main SQL / 主 SQL 中的字段
    "prefetch_fields": [...],          -- Fields needing prefetch SQL / 需要预取 SQL 的字段
    "python_only_fields": [...],       -- Field names for Python fallback / Python 回退的字段名
    "internal_pks": {}                 -- Internal pk aliases / 内部 pk 别名
}
"""
import json
import logging
from typing import Any, Dict, List, Optional

from django.db import router

from .field_classifier import (
    ClassificationResult,
    SqlFieldInfo,
    PrefetchFieldInfo,
    classify_fields,
)
from .sql_generator import generate_prefetch_sql

logger = logging.getLogger("drf_rust.schema_builder")


def build_schema(serializer, child_classifications=None, classification=None) -> Dict[str, Any]:
    """
    Build the Schema JSON dict for the Rust engine.
    为 Rust 引擎构建 Schema JSON 字典。

    Parameters / 参数:
        serializer: An instantiated serializer (fields are bound).
                    一个已实例化的序列化器（字段已绑定）。
        child_classifications: Pre-computed child classifications (for recursive calls).
                              预计算的子分类（用于递归调用）。
        classification: Pre-computed ClassificationResult (avoids double classify_fields call).
                       预计算的 ClassificationResult（避免重复调用 classify_fields）。

    Returns / 返回:
        dict matching the Rust Schema struct (see schema.rs).
        与 Rust Schema 结构体匹配的字典（见 schema.rs）。
    """
    # Use pre-computed classification if provided, otherwise classify now
    # 如果提供了预计算的分类则使用，否则现在分类
    if classification is None:
        classification = classify_fields(serializer)
    model = classification.model
    db_alias = router.db_for_read(model)

    # --- Build sql_fields schema ---
    # --- 构建 sql_fields schema ---
    # Each entry describes a field that Rust reads from a SQL result row.
    # 每个条目描述一个 Rust 从 SQL 结果行读取的字段。
    sql_fields_schema = []
    for sf in classification.sql_fields:
        field_entry = {
            "name": sf.name,         # Dict key in output / 输出中的字典键
            "alias": sf.name,        # Column alias in SQL (same as name for main SQL) / SQL 中的列别名
            "nullable": sf.nullable,
            "field_type": sf.field_type,  # Django field type for Rust-side conversion / Rust 端转换用的 Django 字段类型
        }
        # DecimalField-specific params for precision handling
        # DecimalField 特有参数用于精度处理
        if sf.decimal_places is not None:
            field_entry["decimal_places"] = sf.decimal_places
        if sf.max_digits is not None:
            field_entry["max_digits"] = sf.max_digits
        if sf.coerce_to_string is not None:
            field_entry["coerce_to_string"] = sf.coerce_to_string

        sql_fields_schema.append(field_entry)

    # --- Build prefetch_fields schema ---
    # --- 构建 prefetch_fields schema ---
    # Each entry includes the prefetch SQL template and a recursive child schema.
    # 每个条目包含预取 SQL 模板和递归的子 schema。
    prefetch_fields_schema = []
    for pf in classification.prefetch_fields:
        child_ser_class = pf.child_serializer_class
        if child_ser_class is None:
            # No explicit child serializer → auto-create a default one
            # 没有显式的子序列化器 → 自动创建默认序列化器
            child_ser_class = _make_default_serializer(pf.related_model)

        # Instantiate child serializer to get its field classification
        # 实例化子序列化器以获取其字段分类
        child_ser = child_ser_class()
        child_classification = classify_fields(child_ser)

        # Generate prefetch SQL template (returns tuple of (sql, join_key))
        # 生成预取 SQL 模板（返回 (sql, join_key) 元组）
        prefetch_result = generate_prefetch_sql(pf, child_classification, model)
        if not prefetch_result or not prefetch_result[0]:
            # Skip if SQL generation failed
            # 如果 SQL 生成失败则跳过
            continue
        prefetch_sql, join_key = prefetch_result

        # Build child schema recursively (child may have its own prefetch_fields)
        # 递归构建子 schema（子项可能有自己的 prefetch_fields）
        child_schema = _build_child_schema(child_ser, child_classification)

        prefetch_fields_schema.append({
            "name": pf.name,
            "prefetch_sql_template": prefetch_sql,
            "join_key": join_key,          # Column name for grouping results / 用于分组结果的列名
            "child_schema": child_schema,  # Recursive schema for child fields / 子字段的递归 schema
        })

    # Internal pks for nested serializers with python_only_fields
    # 带有 python_only_fields 的嵌套序列化器的内部 pk
    internal_pks = {}
    # TODO: detect nested serializers via FK/O2O with python_only_fields
    # TODO: 检测通过 FK/O2O 连接且带有 python_only_fields 的嵌套序列化器

    schema = {
        "primary_db": db_alias,
        "sql_fields": sql_fields_schema,
        "prefetch_fields": prefetch_fields_schema,
        "python_only_fields": classification.python_only_fields,
        "internal_pks": internal_pks,
    }

    return schema


def _build_child_schema(child_serializer, child_classification: ClassificationResult) -> Dict:
    """
    Build a child schema for prefetch fields (recursive).
    为预取字段构建子 schema（递归）。

    The child schema has the same structure as the root schema, allowing Rust
    to handle arbitrarily nested prefetch relations.
    子 schema 与根 schema 具有相同的结构，允许 Rust 处理任意深度嵌套的预取关系。
    """
    from django.db import router

    model = child_classification.model
    db_alias = router.db_for_read(model)

    # Build sql_fields schema for the child
    # 为子项构建 sql_fields schema
    sql_fields_schema = []
    for sf in child_classification.sql_fields:
        field_entry = {
            "name": sf.name,
            "alias": sf.name,
            "nullable": sf.nullable,
            "field_type": sf.field_type,
        }
        if sf.decimal_places is not None:
            field_entry["decimal_places"] = sf.decimal_places
        if sf.max_digits is not None:
            field_entry["max_digits"] = sf.max_digits
        if sf.coerce_to_string is not None:
            field_entry["coerce_to_string"] = sf.coerce_to_string
        sql_fields_schema.append(field_entry)

    # Recursive: handle nested prefetch in child
    # 递归：处理子项中的嵌套预取
    prefetch_fields_schema = []
    for pf in child_classification.prefetch_fields:
        child_ser_class = pf.child_serializer_class
        if child_ser_class is None:
            child_ser_class = _make_default_serializer(pf.related_model)
        nested_ser = child_ser_class()
        nested_classification = classify_fields(nested_ser)
        nested_result = generate_prefetch_sql(pf, nested_classification, model)
        if not nested_result or not nested_result[0]:
            continue
        nested_prefetch_sql, nested_join_key = nested_result
        nested_child_schema = _build_child_schema(nested_ser, nested_classification)
        prefetch_fields_schema.append({
            "name": pf.name,
            "prefetch_sql_template": nested_prefetch_sql,
            "join_key": nested_join_key,
            "child_schema": nested_child_schema,
        })

    return {
        "primary_db": db_alias,
        "sql_fields": sql_fields_schema,
        "prefetch_fields": prefetch_fields_schema,
        "python_only_fields": child_classification.python_only_fields,
        "internal_pks": {},
    }


def _make_default_serializer(model):
    """
    Create a minimal default serializer for a model.
    为模型创建一个最小的默认序列化器。

    Used when a prefetch field doesn't have an explicit child serializer class.
    This auto-generates a ModelSerializer that includes all concrete fields.
    当预取字段没有显式的子序列化器类时使用。
    自动生成一个包含所有具体字段的 ModelSerializer。
    """
    from rest_framework import serializers

    pk_name = model._meta.pk.name
    # Get all concrete field names on the model
    # 获取模型上的所有具体字段名
    field_names = [f.name for f in model._meta.fields]
    # Ensure pk is included (may not be in fields for some edge cases)
    # 确保 pk 被包含（在某些边缘情况下可能不在 fields 中）
    if pk_name not in field_names:
        field_names.insert(0, pk_name)

    # Dynamically create a Meta class and ModelSerializer subclass
    # 动态创建 Meta 类和 ModelSerializer 子类
    meta = type("Meta", (), {"model": model, "fields": field_names})
    return type(
        "{}DefaultSerializer".format(model.__name__),
        (serializers.ModelSerializer,),
        {"Meta": meta},
    )


def schema_to_json(schema: Dict) -> str:
    """
    Serialize schema dict to JSON string for passing to Rust.
    将 schema 字典序列化为 JSON 字符串以传递给 Rust。

    Uses default=str to handle any non-serializable types (e.g., model classes).
    使用 default=str 处理任何不可序列化的类型（如模型类）。
    """
    return json.dumps(schema, default=str)
