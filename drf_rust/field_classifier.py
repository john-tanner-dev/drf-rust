# -*- coding: utf-8 -*-
"""
Three-way field classification for RustModelSerializer.
RustModelSerializer 的三向字段分类。

Every field on a serializer is classified into one of:
每个序列化器字段被归类为以下三类之一：

  - sql_fields:          Source resolves to a database column (possibly through FK/O2O JOINs).
                         source 解析为数据库列（可能通过 FK/O2O JOIN）。
  - prefetch_fields:     Source path hits a ManyToMany or reverse ForeignKey relation,
                         requiring a separate prefetch query.
                         source 路径命中 ManyToMany 或反向 ForeignKey 关系，需要单独的预取查询。
  - python_only_fields:  Everything else (SerializerMethodField, callable source, source='*', etc.)
                         that must be computed by Python.
                         其他所有情况（SerializerMethodField、可调用 source、source='*' 等），必须由 Python 计算。

This classification determines how each field's value is obtained during serialization:
sql_fields → Rust via SQL, prefetch_fields → Rust via prefetch SQL, python_only → Python fallback.
该分类决定了序列化时每个字段值的获取方式：
sql_fields → Rust 通过 SQL, prefetch_fields → Rust 通过预取 SQL, python_only → Python 回退。
"""
import logging
from dataclasses import dataclass, field as dc_field
from typing import Any, Dict, List, Optional, Tuple, Type

from django.db.models.fields.related import (
    ForeignKey,
    OneToOneField,
    ManyToManyField,
    ManyToOneRel,
    ManyToManyRel,
    ForeignObjectRel,
)

logger = logging.getLogger("drf_rust.field_classifier")


@dataclass
class JoinStep:
    """
    One FK/O2O hop in a JOIN chain.
    JOIN 链中的一个 FK/O2O 跳跃步骤。

    When a serializer field's source traverses FK/O2O relations (e.g., 'author.name'),
    each relation hop is recorded as a JoinStep so the SQL generator can build
    the corresponding LEFT JOIN clauses.
    当序列化器字段的 source 遍历 FK/O2O 关系时（如 'author.name'），
    每个关系跳跃被记录为一个 JoinStep，以便 SQL 生成器构建相应的 LEFT JOIN 子句。
    """
    field_name: str       # Django model field name (e.g., 'author') / Django 模型字段名
    from_column: str      # Column on the source table (e.g., 'author_id') / 源表上的列名
    to_column: str        # Column on the target table (e.g., 'id') / 目标表上的列名
    to_table: str         # Target table name / 目标表名
    to_model: Any         # Target model class / 目标模型类


@dataclass
class SqlFieldInfo:
    """
    A field that maps to a SQL column.
    映射到 SQL 列的字段。

    Contains all the information needed to include this field in a SQL SELECT:
    the column name, which table it belongs to, any JOIN hops needed to reach it,
    and type metadata for Rust-side value conversion.
    包含将此字段包含在 SQL SELECT 中所需的所有信息：
    列名、所属表、到达它所需的 JOIN 跳跃步骤，以及 Rust 端值转换所需的类型元数据。
    """
    name: str                        # Serializer field name / 序列化器字段名
    source: str                      # Original source string (e.g., 'author.name') / 原始 source 字符串
    column: str                      # Final DB column name (e.g., 'name') / 最终数据库列名
    table: str                       # Table the column belongs to / 列所属的表名
    join_chain: List[JoinStep]       # FK/O2O hops to reach the column / 到达该列的 FK/O2O 跳跃链
    model_field: Any                 # Django model field object / Django 模型字段对象
    field_type: str                  # Django field class name (e.g., 'CharField') / Django 字段类名
    nullable: bool = False           # Whether the column allows NULL / 该列是否允许 NULL
    # DecimalField-specific attributes / DecimalField 专有属性
    decimal_places: Optional[int] = None   # Number of decimal places / 小数位数
    max_digits: Optional[int] = None       # Max total digits / 最大总位数
    coerce_to_string: Optional[bool] = None  # Return as string or float / 返回字符串还是浮点数


@dataclass
class PrefetchFieldInfo:
    """
    A field requiring a separate prefetch query.
    需要单独预取查询的字段。

    Used for ManyToMany and reverse ForeignKey relations, where one parent row
    maps to multiple child rows. A separate SQL query with IN clause is executed.
    用于 ManyToMany 和反向 ForeignKey 关系，其中一个父行映射到多个子行。
    通过带 IN 子句的单独 SQL 查询来执行。
    """
    name: str                        # Serializer field name / 序列化器字段名
    source: str                      # Original source string / 原始 source 字符串
    relation_field: Any              # Django relation field object / Django 关系字段对象
    related_model: Any               # The related model class / 关联的模型类
    child_serializer_class: Any      # Serializer class for the related model (or None for auto) / 关联模型的序列化器类
    is_many_to_many: bool = False    # Whether this is a M2M relation / 是否为多对多关系
    is_reverse_fk: bool = False      # Whether this is a reverse FK relation / 是否为反向外键关系


@dataclass
class NestedFkFieldInfo:
    """
    A nested serializer (many=False) pointing to FK/O2O, optimized via SQL JOIN.
    指向 FK/O2O 的嵌套序列化器（many=False），通过 SQL JOIN 优化。

    Instead of falling back to Python (N+1 queries), child fields are flattened
    into the parent's main SQL query with prefixed aliases, then reconstructed
    into nested dicts in Python after Rust returns.
    不再回退到 Python（N+1 查询），而是将子字段展平到父级的主 SQL 查询中
    使用前缀别名，然后在 Rust 返回后用 Python 重建为嵌套字典。
    """
    name: str                                    # Serializer field name (e.g., 'customer_') / 序列化器字段名
    source: str                                  # Source string (e.g., 'customer') / source 字符串
    fk_join_step: JoinStep                       # FK/O2O hop to related table / 到关联表的 FK/O2O 跳跃
    child_classification: 'ClassificationResult' # Recursive classification of child / 子序列化器的递归分类
    child_serializer: Any                        # Instantiated child serializer / 已实例化的子序列化器


@dataclass
class ClassificationResult:
    """
    Result of classifying all fields on a serializer.
    序列化器所有字段分类的结果。

    This is the primary output of classify_fields() and is consumed by
    sql_generator.py, schema_builder.py, and serializers.py.
    这是 classify_fields() 的主要输出，被 sql_generator.py、schema_builder.py
    和 serializers.py 使用。
    """
    sql_fields: List[SqlFieldInfo]           # Fields resolvable to SQL columns / 可解析为 SQL 列的字段
    prefetch_fields: List[PrefetchFieldInfo]  # Fields needing prefetch queries / 需要预取查询的字段
    python_only_fields: List[str]            # Field names needing Python computation / 需要 Python 计算的字段名
    nested_fk_fields: List[NestedFkFieldInfo] = dc_field(default_factory=list)  # Nested FK serializers optimized via JOIN / 通过 JOIN 优化的嵌套 FK 序列化器
    model: Any = None                        # The Django model class / Django 模型类
    db_table: str = ""                       # The model's database table name / 模型的数据库表名


def classify_fields(serializer) -> ClassificationResult:
    """
    Classify all readable fields on a serializer instance into three categories.
    将序列化器实例上的所有可读字段分为三类。

    The serializer must already be instantiated (fields bound via bind()).
    序列化器必须已经实例化（字段已通过 bind() 绑定）。

    Parameters / 参数:
        serializer: An instantiated DRF serializer with Meta.model defined.
                    一个已实例化的 DRF 序列化器，需定义 Meta.model。

    Returns / 返回:
        ClassificationResult containing the three field lists plus model info.
        包含三个字段列表和模型信息的 ClassificationResult。
    """
    model = serializer.Meta.model
    db_table = model._meta.db_table

    sql_fields = []
    prefetch_fields = []
    python_only_fields = []
    nested_fk_fields = []

    for field_name, field_obj in serializer.fields.items():
        # Skip write-only fields — they are not part of the read path
        # 跳过只写字段 —— 它们不属于读取路径
        if getattr(field_obj, "write_only", False):
            continue

        # Classify this individual field
        # 对单个字段进行分类
        classification = _classify_one_field(field_name, field_obj, model)

        if classification is None:
            # None means python_only
            # None 表示 python_only
            python_only_fields.append(field_name)
        elif isinstance(classification, SqlFieldInfo):
            sql_fields.append(classification)
        elif isinstance(classification, PrefetchFieldInfo):
            prefetch_fields.append(classification)
        elif isinstance(classification, NestedFkFieldInfo):
            nested_fk_fields.append(classification)
        else:
            # Unexpected return → treat as python_only
            # 意外返回值 → 视为 python_only
            python_only_fields.append(field_name)

    return ClassificationResult(
        sql_fields=sql_fields,
        prefetch_fields=prefetch_fields,
        python_only_fields=python_only_fields,
        nested_fk_fields=nested_fk_fields,
        model=model,
        db_table=db_table,
    )


def _classify_one_field(field_name, field_obj, model):
    """
    Classify a single serializer field.
    对单个序列化器字段进行分类。

    Returns SqlFieldInfo, PrefetchFieldInfo, or None (python_only).
    返回 SqlFieldInfo、PrefetchFieldInfo 或 None（python_only）。

    Algorithm / 算法:
      1. SerializerMethodField → always python_only
         SerializerMethodField → 始终为 python_only
      2. source='*' or callable source → python_only
         source='*' 或可调用 source → python_only
      3. Split source by '.' and walk through model._meta:
         按 '.' 分割 source 并遍历 model._meta：
         - FK/O2O at intermediate position → record as JoinStep, continue walking
           中间位置的 FK/O2O → 记录为 JoinStep，继续遍历
         - M2M or reverse relation at any position → PrefetchFieldInfo
           任意位置的 M2M 或反向关系 → PrefetchFieldInfo
         - Concrete column at final position → SqlFieldInfo
           最终位置的具体列 → SqlFieldInfo
         - Unresolvable segment → python_only
           无法解析的段 → python_only
    """
    from rest_framework.fields import SerializerMethodField
    from rest_framework import serializers

    # SerializerMethodField → always python_only (calls get_<field_name> method)
    # SerializerMethodField → 始终为 python_only（调用 get_<field_name> 方法）
    if isinstance(field_obj, SerializerMethodField):
        return None

    # Get the source attribute (defaults to field_name after bind())
    # 获取 source 属性（bind() 后默认为 field_name）
    source = getattr(field_obj, "source", None)
    if source is None:
        source = field_name

    # source='*' means the entire object → python_only
    # source='*' 表示整个对象 → python_only
    if source == "*":
        return None

    # callable source → python_only (e.g., a function reference)
    # 可调用的 source → python_only（如函数引用）
    if callable(source):
        return None

    # Nested serializer (many=False) → try SQL JOIN optimization, else python_only
    # 嵌套序列化器（many=False）→ 尝试 SQL JOIN 优化，否则 python_only
    #
    # If source points to FK/O2O, we can flatten the child's fields into the
    # parent SQL via JOINs (NestedFkFieldInfo). Otherwise, fall back to python_only.
    # ListSerializer (many=True) is NOT caught here; it goes through path walking.
    # 如果 source 指向 FK/O2O，我们可以通过 JOIN 将子字段展平到父 SQL 中
    # （NestedFkFieldInfo）。否则回退到 python_only。
    # ListSerializer（many=True）不会在这里被捕获；它通过路径遍历处理。
    if isinstance(field_obj, serializers.BaseSerializer) and not isinstance(field_obj, serializers.ListSerializer):
        nfk = _try_classify_nested_fk(field_name, field_obj, source, model)
        if nfk is not None:
            return nfk
        return None

    # Split source into path segments (e.g., 'author.department.name' → ['author', 'department', 'name'])
    # 将 source 分割为路径段（如 'author.department.name' → ['author', 'department', 'name']）
    segments = source.split(".")
    if not segments:
        return None

    # Walk the source path through model._meta, building JOIN chain
    # 通过 model._meta 遍历 source 路径，构建 JOIN 链
    current_model = model
    join_chain = []

    for i, segment in enumerate(segments):
        is_last = (i == len(segments) - 1)

        try:
            django_field = current_model._meta.get_field(segment)
        except Exception:
            # Segment doesn't resolve to a model field → python_only
            # 段无法解析为模型字段 → python_only
            return None

        if is_last:
            # ---- Final segment: determine field category ----
            # ---- 最终段：确定字段类别 ----

            if isinstance(django_field, (ForeignKey, OneToOneField)):
                # FK at the end of path: maps to the FK column (e.g., author_id)
                # 路径末尾的 FK：映射到 FK 列（如 author_id）
                # Resolve field_type to the related model's PK type so Rust
                # knows the actual value type (e.g., BigAutoField → Int)
                # 将 field_type 解析为关联模型的 PK 类型，以便 Rust
                # 知道实际的值类型（如 BigAutoField → Int）
                related_pk = django_field.related_model._meta.pk
                actual_field_type = type(related_pk).__name__
                return SqlFieldInfo(
                    name=field_name,
                    source=source,
                    column=django_field.column,
                    table=current_model._meta.db_table,
                    join_chain=join_chain,
                    model_field=django_field,
                    field_type=actual_field_type,
                    nullable=django_field.null,
                )
            elif isinstance(django_field, ManyToManyField):
                # M2M at the end → prefetch query needed
                # 末尾的 M2M → 需要预取查询
                child_ser = _get_child_serializer(field_obj, django_field)
                return PrefetchFieldInfo(
                    name=field_name,
                    source=source,
                    relation_field=django_field,
                    related_model=django_field.related_model,
                    child_serializer_class=child_ser,
                    is_many_to_many=True,
                )
            elif isinstance(django_field, (ManyToOneRel, ManyToManyRel)):
                # Reverse relation at the end → prefetch query needed
                # 末尾的反向关系 → 需要预取查询
                child_ser = _get_child_serializer(field_obj, django_field)
                related_model = django_field.related_model
                return PrefetchFieldInfo(
                    name=field_name,
                    source=source,
                    relation_field=django_field,
                    related_model=related_model,
                    child_serializer_class=child_ser,
                    is_reverse_fk=isinstance(django_field, ManyToOneRel),
                    is_many_to_many=isinstance(django_field, ManyToManyRel),
                )
            elif isinstance(django_field, ForeignObjectRel):
                # Other reverse relation (e.g., GenericRelation) → prefetch
                # 其他反向关系（如 GenericRelation）→ 预取
                child_ser = _get_child_serializer(field_obj, django_field)
                return PrefetchFieldInfo(
                    name=field_name,
                    source=source,
                    relation_field=django_field,
                    related_model=django_field.related_model,
                    child_serializer_class=child_ser,
                )
            else:
                # Regular concrete column field (CharField, IntegerField, etc.)
                # 普通具体列字段（CharField、IntegerField 等）
                return _make_sql_field_info(
                    field_name, source, django_field, current_model, join_chain, field_obj
                )
        else:
            # ---- Intermediate segment: must be a relation to continue ----
            # ---- 中间段：必须是关系才能继续 ----

            if isinstance(django_field, (ForeignKey, OneToOneField)):
                # FK/O2O hop → record as JoinStep and continue walking
                # FK/O2O 跳跃 → 记录为 JoinStep 并继续遍历
                related_model = django_field.related_model
                join_chain.append(JoinStep(
                    field_name=segment,
                    from_column=django_field.column,
                    to_column=related_model._meta.pk.column,
                    to_table=related_model._meta.db_table,
                    to_model=related_model,
                ))
                current_model = related_model
            elif isinstance(django_field, (ManyToManyField, ManyToOneRel, ManyToManyRel)):
                # Multi-value relation in the middle → cannot continue with single-value path
                # → classify as prefetch
                # 中间位置的多值关系 → 无法继续单值路径 → 归类为预取
                child_ser = _get_child_serializer(field_obj, django_field)
                related_model = (
                    django_field.related_model
                    if hasattr(django_field, "related_model")
                    else django_field.model
                )
                return PrefetchFieldInfo(
                    name=field_name,
                    source=source,
                    relation_field=django_field,
                    related_model=related_model,
                    child_serializer_class=child_ser,
                    is_many_to_many=True,
                )
            elif isinstance(django_field, ForeignObjectRel):
                # Reverse O2O → can still be treated as single-value (one-to-one)
                # 反向 O2O → 仍可视为单值（一对一）
                if getattr(django_field, "one_to_one", False):
                    related_model = django_field.related_model
                    # For reverse O2O, the FK column is on the related model
                    # 对于反向 O2O，FK 列在关联模型上
                    fk_field = django_field.field
                    join_chain.append(JoinStep(
                        field_name=segment,
                        from_column=current_model._meta.pk.column,
                        to_column=fk_field.column,
                        to_table=related_model._meta.db_table,
                        to_model=related_model,
                    ))
                    current_model = related_model
                else:
                    # Reverse FK (one_to_many) → prefetch
                    # 反向 FK（一对多）→ 预取
                    child_ser = _get_child_serializer(field_obj, django_field)
                    return PrefetchFieldInfo(
                        name=field_name,
                        source=source,
                        relation_field=django_field,
                        related_model=django_field.related_model,
                        child_serializer_class=child_ser,
                        is_reverse_fk=True,
                    )
            else:
                # Not a relation and not the last segment → can't continue → python_only
                # 不是关系且不是最后一段 → 无法继续 → python_only
                return None

    # Should not reach here (segments is never empty after split)
    # 不应到达这里（split 后 segments 不会为空）
    return None


def _try_classify_nested_fk(field_name, field_obj, source, model):
    """
    Try to classify a nested serializer (many=False) as NestedFkFieldInfo.
    尝试将嵌套序列化器（many=False）归类为 NestedFkFieldInfo。

    Conditions for SQL JOIN optimization / SQL JOIN 优化的条件:
      - source is a single segment (e.g., 'customer', not 'a.b.c')
        source 是单个段（如 'customer'，而非 'a.b.c'）
      - source resolves to FK/O2O field on the model
        source 解析为模型上的 FK/O2O 字段
      - child serializer has Meta.model defined (so classify_fields works)
        子序列化器定义了 Meta.model（以便 classify_fields 正常工作）

    Returns NestedFkFieldInfo or None (fall back to python_only).
    返回 NestedFkFieldInfo 或 None（回退到 python_only）。
    """
    # Only single-segment source is optimizable (e.g., 'customer', not 'a.b')
    # 仅单段 source 可优化（如 'customer'，而非 'a.b'）
    segments = source.split(".")
    if len(segments) != 1:
        return None

    # Check child serializer has Meta.model (required for classify_fields)
    # 检查子序列化器有 Meta.model（classify_fields 所需）
    child_meta = getattr(field_obj, "Meta", None)
    if child_meta is None or not hasattr(child_meta, "model"):
        return None

    # Try to resolve source as a FK/O2O field
    # 尝试将 source 解析为 FK/O2O 字段
    try:
        django_field = model._meta.get_field(segments[0])
    except Exception:
        return None

    if not isinstance(django_field, (ForeignKey, OneToOneField)):
        return None

    # Build JoinStep for the FK hop
    # 为 FK 跳跃构建 JoinStep
    related_model = django_field.related_model
    fk_join_step = JoinStep(
        field_name=segments[0],
        from_column=django_field.column,
        to_column=related_model._meta.pk.column,
        to_table=related_model._meta.db_table,
        to_model=related_model,
    )

    # Recursively classify the child serializer's fields
    # 递归分类子序列化器的字段
    try:
        child_classification = classify_fields(field_obj)
    except Exception:
        return None

    return NestedFkFieldInfo(
        name=field_name,
        source=source,
        fk_join_step=fk_join_step,
        child_classification=child_classification,
        child_serializer=field_obj,
    )


def _make_sql_field_info(field_name, source, django_field, model, join_chain, serializer_field):
    """
    Create SqlFieldInfo from a concrete Django model field.
    从具体的 Django 模型字段创建 SqlFieldInfo。

    Handles special cases for DecimalField (extracts decimal_places, max_digits,
    coerce_to_string from either the serializer field or the model field).
    处理 DecimalField 的特殊情况（从序列化器字段或模型字段提取
    decimal_places、max_digits、coerce_to_string）。
    """
    from rest_framework.fields import DecimalField as DrfDecimalField
    from rest_framework import settings as drf_settings

    field_type = type(django_field).__name__
    nullable = getattr(django_field, "null", False)
    # Get the actual DB column name (may differ from field name, e.g., 'author_id' vs 'author')
    # 获取实际的数据库列名（可能与字段名不同，如 'author_id' 与 'author'）
    column = getattr(django_field, "column", django_field.name)

    info = SqlFieldInfo(
        name=field_name,
        source=source,
        column=column,
        table=model._meta.db_table,
        join_chain=join_chain,
        model_field=django_field,
        field_type=field_type,
        nullable=nullable,
    )

    # Extract DecimalField-specific parameters for Rust-side precision handling
    # 提取 DecimalField 特有参数用于 Rust 端精度处理
    if isinstance(serializer_field, DrfDecimalField):
        # DRF DecimalField: use serializer-level params (highest priority)
        # DRF DecimalField：使用序列化器级参数（最高优先级）
        info.decimal_places = getattr(serializer_field, "decimal_places", None)
        info.max_digits = getattr(serializer_field, "max_digits", None)
        coerce = getattr(serializer_field, "coerce_to_string", None)
        if coerce is None:
            # Fall back to DRF global setting
            # 回退到 DRF 全局设置
            coerce = getattr(drf_settings.api_settings, "COERCE_DECIMAL_TO_STRING", True)
        info.coerce_to_string = coerce
    elif field_type == "DecimalField":
        # Model field is DecimalField but serializer field may be a generic field
        # 模型字段是 DecimalField 但序列化器字段可能是通用字段
        info.decimal_places = getattr(django_field, "decimal_places", None)
        info.max_digits = getattr(django_field, "max_digits", None)
        info.coerce_to_string = True

    return info


def _get_child_serializer(field_obj, django_field):
    """
    Get the child serializer class for a relation field.
    获取关系字段的子序列化器类。

    If the serializer field is itself a nested serializer (e.g., TagSerializer(many=True)),
    use that serializer's class. Otherwise, return None — the schema builder will
    create a default serializer automatically.
    如果序列化器字段本身是嵌套序列化器（如 TagSerializer(many=True)），
    则使用该序列化器的类。否则返回 None —— schema 构建器将自动创建默认序列化器。
    """
    from rest_framework import serializers

    # If the field is a nested serializer, use its class
    # 如果字段是嵌套序列化器，使用其类
    if isinstance(field_obj, serializers.BaseSerializer):
        return type(field_obj)

    # Otherwise, return None — the schema builder will create a default serializer
    # 否则返回 None —— schema 构建器将创建默认序列化器
    return None
