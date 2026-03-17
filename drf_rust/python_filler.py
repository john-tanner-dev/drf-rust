# -*- coding: utf-8 -*-
import logging
import traceback
from typing import Any, Dict, List, Type

logger = logging.getLogger("drf_rust.python_filler")


def fill_python_only_fields(
    results: list,
    python_only_fields: List[str],
    model,
    serializer_class: Type,
    context: dict,
    pk_field_name: str = "pk",
    instances=None,
    original_serializer=None,
) -> None:
    """
    Fill python_only_fields in-place on the Rust-returned result dicts.
    在 Rust 返回的结果字典上就地填充 python_only_fields。

    Parameters / 参数:
        results:             List of dicts returned by Rust (will be modified in-place).
                             Rust 返回的字典列表（将被就地修改）。
        python_only_fields:  List of field names that need Python filling.
                             需要 Python 填充的字段名列表。
        model:               Django model class.
                             Django 模型类。
        serializer_class:    The RustModelSerializer class (used to create partial serializer).
                             RustModelSerializer 类（用于创建部分序列化器）。
        context:             Serializer context dict (request, view, format, etc.).
                             序列化器上下文字典（request、view、format 等）。
        pk_field_name:       The key name for pk in the result dicts (default: "pk").
                             结果字典中 pk 的键名（默认："pk"）。
        instances:           Optional pre-fetched instances (e.g., from paginator).
                             Preserves dynamic attributes set by serializer __init__.
                             可选的预取实例（如来自分页器）。保留序列化器 __init__ 设置的动态属性。
        original_serializer: Optional original serializer instance. When provided, use it
                             directly instead of creating a new one — this preserves dynamic
                             defaults set in __init__ (e.g., self.fields['x'].default = val).
                             可选的原始序列化器实例。提供时直接使用它而不是创建新实例——
                             这样可以保留 __init__ 中设置的动态默认值。
    """
    if not python_only_fields or not results:
        return

    # Collect pk values from Rust results
    # 从 Rust 结果中收集 pk 值
    pk_name = model._meta.pk.name
    pks = []
    for record in results:
        # The pk field should be present in the result dict as one of the sql_fields
        # pk 字段应该作为 sql_fields 之一存在于结果字典中
        pk_value = record.get(pk_name)
        if pk_value is not None:
            pks.append(pk_value)

    if not pks:
        return

    if instances is not None:
        # Use provided instances (preserves dynamic attributes like _my_URL)
        # 使用提供的实例（保留动态属性如 _my_URL）
        instance_map = {getattr(inst, pk_name): inst for inst in instances}
    else:
        # Batch-query all model instances by pk (single SQL query, efficient)
        # 按 pk 批量查询所有模型实例（单条 SQL 查询，高效）
        db_instances = model.objects.filter(pk__in=pks)
        # Build pk → instance lookup map
        # 构建 pk → 实例的查找映射
        instance_map = {getattr(inst, pk_name): inst for inst in db_instances}

    # Use the original serializer if provided (preserves dynamic defaults),
    # otherwise build a new partial serializer.
    # 如果提供了原始序列化器则直接使用（保留动态默认值），
    # 否则构建新的部分序列化器。
    if original_serializer is not None:
        partial_serializer = original_serializer
    else:
        partial_serializer = _build_partial_serializer(
            serializer_class, python_only_fields, context
        )

    # Fill each result dict with python_only field values
    # 为每个结果字典填充 python_only 字段的值
    for record in results:
        pk_value = record.get(pk_name)
        if pk_value is None:
            continue

        instance = instance_map.get(pk_value)
        if instance is None:
            continue

        # Get python_only field values via standard DRF mechanism
        # 通过标准 DRF 机制获取 python_only 字段的值
        partial_data = _get_partial_representation(
            partial_serializer, instance, python_only_fields
        )
        # Merge into the Rust-returned dict (in-place)
        # 合并到 Rust 返回的字典中（就地修改）
        record.update(partial_data)


def _build_partial_serializer(serializer_class, python_only_fields, context):
    """
    Build a lightweight serializer instance that only processes python_only_fields.
    构建一个仅处理 python_only_fields 的轻量级序列化器实例。

    We instantiate the full serializer but will only call specific fields' methods.
    我们实例化完整的序列化器，但只调用特定字段的方法。
    """
    # Create a new serializer with the given context
    # 使用给定的上下文创建新的序列化器
    serializer = serializer_class(context=context)
    return serializer


def _get_partial_representation(serializer, instance, field_names):
    """
    Get representation for only the specified fields.
    仅获取指定字段的表示。

    Uses the same logic as DRF's Serializer.to_representation(), but only
    processes the specified python_only fields.
    使用与 DRF 的 Serializer.to_representation() 相同的逻辑，但仅处理
    指定的 python_only 字段。

    Key differences from the previous version:
    与之前版本的关键区别：
      - SkipField is handled correctly (skip, not None)
        正确处理 SkipField（跳过，而非设置 None）
      - PKOnlyObject is handled correctly (like DRF does)
        正确处理 PKOnlyObject（与 DRF 一致）
      - None attribute → None value (without calling to_representation)
        None 属性 → None 值（不调用 to_representation）
      - Exceptions are logged for debugging instead of silently swallowed
        异常会被记录以便调试，而不是被静默吞掉
    """
    from rest_framework.fields import SkipField, empty
    from rest_framework.relations import PKOnlyObject

    result = {}
    for field_name in field_names:
        field = serializer.fields.get(field_name)
        if field is None:
            continue

        try:
            # Get the raw attribute value from the model instance
            # 从模型实例获取原始属性值
            attribute = field.get_attribute(instance)
        except SkipField:
            # DRF raises SkipField when the source attribute doesn't exist on
            # the instance but the field has a default value set.
            # For serialization (output), we should use the default value
            # instead of skipping — this supports patterns like:
            #   corp_name = CharField(default=SITE_NAME)
            #   self.fields['sale_value0'].default = weight_t  (dynamic default)
            # DRF 在源属性不存在于实例上但字段有默认值时会抛出 SkipField。
            # 对于序列化（输出），我们应该使用默认值而不是跳过——
            # 这支持如下模式：
            #   corp_name = CharField(default=SITE_NAME)
            #   self.fields['sale_value0'].default = weight_t（动态默认值）
            if field.default is not empty:
                try:
                    result[field_name] = field.to_representation(field.default)
                except Exception as e:
                    print("[python_filler] to_representation(default) FAILED for '{}': {}".format(
                        field_name, e))
                    traceback.print_exc()
                    result[field_name] = None
            continue
        except Exception as e:
            print("[python_filler] get_attribute FAILED for '{}': {}".format(
                field_name, e))
            traceback.print_exc()
            result[field_name] = None
            continue

        # Match DRF's to_representation: check for None before calling to_representation
        # 匹配 DRF 的 to_representation：在调用 to_representation 之前检查 None
        check_for_none = attribute.pk if isinstance(attribute, PKOnlyObject) else attribute
        if check_for_none is None:
            result[field_name] = None
        else:
            try:
                # Convert to serializable representation
                # 转换为可序列化的表示
                result[field_name] = field.to_representation(attribute)
            except Exception as e:
                print("[python_filler] to_representation FAILED for '{}': {}".format(
                    field_name, e))
                traceback.print_exc()
                result[field_name] = None

    return result
