# -*- coding: utf-8 -*-
"""
RustModelSerializer and RustListSerializer.
RustModelSerializer 和 RustListSerializer。

Drop-in replacement for DRF ModelSerializer that accelerates the read path
(to_representation) via Rust-based SQL execution and result building.
作为 DRF ModelSerializer 的直接替代品，通过 Rust 实现的 SQL 执行和结果构建
来加速读取路径 (to_representation)。

Architecture / 架构:
  1. Field classification: Categorize serializer fields into sql_fields,
     prefetch_fields, and python_only_fields.
     字段分类：将序列化器字段归类为 sql_fields、prefetch_fields 和 python_only_fields。
  2. SQL generation: Generate main SQL (with JOINs) and prefetch SQL templates.
     SQL 生成：生成主 SQL（带 JOIN）和预取 SQL 模板。
  3. Schema building: Describe field types and structure as JSON for Rust.
     Schema 构建：将字段类型和结构描述为 JSON 传递给 Rust。
  4. Rust execution: Execute SQL, build Python dicts, return list[dict].
     Rust 执行：执行 SQL，构建 Python 字典，返回 list[dict]。
  5. Python filling: Fill python_only_fields (SerializerMethodField etc.) via
     standard DRF mechanism.
     Python 填充：通过标准 DRF 机制填充 python_only_fields（如 SerializerMethodField 等）。
"""
import json
import logging
import warnings
from typing import Any, Dict, List, Optional

from rest_framework import serializers
from rest_framework.utils.serializer_helpers import ReturnDict, ReturnList

logger = logging.getLogger("drf_rust.serializers")

# kwargs that belong to ListSerializer, not the child serializer.
# Pop these from kwargs before constructing the child instance.
# 属于 ListSerializer 而非子序列化器的参数，在构造子实例前从 kwargs 中弹出。
_LIST_SERIALIZER_KWARGS_REMOVE = (
    'many', 'allow_empty', 'max_length', 'min_length',
)

# kwargs that are valid for ListSerializer (may be shared with child).
# After child creation, copy any of these still present in kwargs to list_kwargs.
# 对 ListSerializer 有效的参数（可与子序列化器共享）。
# 子实例创建后，将 kwargs 中仍存在的这些参数复制到 list_kwargs。
_LIST_SERIALIZER_KWARGS = (
    'read_only', 'write_only', 'required', 'default', 'initial', 'source',
    'label', 'help_text', 'style', 'error_messages', 'allow_empty',
    'instance', 'data', 'partial', 'context', 'allow_null',
    'max_length', 'min_length',
)


def _check_rust_available():
    """
    Check if the Rust engine extension module is importable.
    检查 Rust 引擎扩展模块是否可以导入。

    Returns (is_available: bool, module_or_None).
    返回 (是否可用: bool, 模块或None)。
    """
    try:
        from . import rust_engine
        return True, rust_engine
    except ImportError:
        return False, None


# Module-level check: is the Rust engine compiled and available?
# 模块级检查：Rust 引擎是否已编译且可用？
RUST_AVAILABLE, _rust_engine = _check_rust_available()


def _flatten_nested_fk_fields(nested_fk_fields, parent_fk_chain=None, counter=None):
    """
    Flatten nested FK fields into virtual SqlFieldInfo with composed join chains.
    将嵌套 FK 字段展平为带有组合 join 链的虚拟 SqlFieldInfo。

    For each child sql_field, creates a SqlFieldInfo with:
    对于每个子 sql_field，创建一个 SqlFieldInfo：
      - name = "__nfk_{counter}" (short alias within 60-char limit)
        name = "__nfk_{counter}"（短别名，在 60 字符限制内）
      - join_chain = [parent_fk_join_step] + child.join_chain (composed)
        join_chain = [父 FK join 步骤] + 子.join_chain（组合）
      - nullable = True (FK can be NULL → all children nullable)
        nullable = True（FK 可以为 NULL → 所有子字段可为空）

    Returns (extra_sql_fields, recon_plans):
    返回 (额外的 sql_fields, 重建计划)：
      - extra_sql_fields: List of SqlFieldInfo to add to parent's sql_fields
        要添加到父级 sql_fields 的 SqlFieldInfo 列表
      - recon_plans: List of dicts for nested dict reconstruction after Rust returns
        Rust 返回后用于嵌套字典重建的字典列表
    """
    from .field_classifier import SqlFieldInfo, NestedFkFieldInfo

    if parent_fk_chain is None:
        parent_fk_chain = []
    if counter is None:
        counter = [0]

    extra_fields = []
    recon_plans = []

    for nfk in nested_fk_fields:
        current_chain = parent_fk_chain + [nfk.fk_join_step]
        alias_map = {}

        # Flatten each child sql_field with composed join chain
        # 用组合 join 链展平每个子 sql_field
        for child_sf in nfk.child_classification.sql_fields:
            alias = "__nfk_{}".format(counter[0])
            counter[0] += 1

            composed_chain = list(current_chain) + list(child_sf.join_chain)

            extra_fields.append(SqlFieldInfo(
                name=alias,
                source=child_sf.source,
                column=child_sf.column,
                table=child_sf.table,
                join_chain=composed_chain,
                model_field=child_sf.model_field,
                field_type=child_sf.field_type,
                nullable=True,
                decimal_places=child_sf.decimal_places,
                max_digits=child_sf.max_digits,
                coerce_to_string=child_sf.coerce_to_string,
            ))
            alias_map[child_sf.name] = alias

        # Recurse for child's own nested FK fields (multi-level nesting)
        # 递归处理子级自身的嵌套 FK 字段（多级嵌套）
        child_plans = []
        if nfk.child_classification.nested_fk_fields:
            child_extra, child_plans = _flatten_nested_fk_fields(
                nfk.child_classification.nested_fk_fields,
                parent_fk_chain=current_chain,
                counter=counter,
            )
            extra_fields.extend(child_extra)

        recon_plans.append({
            'field_name': nfk.name,
            'alias_map': alias_map,
            'child_python_only': nfk.child_classification.python_only_fields,
            'child_serializer': nfk.child_serializer,
            'child_plans': child_plans,
        })

    return extra_fields, recon_plans


def _reconstruct_nested_dicts(results, recon_plans):
    """
    Reconstruct nested dicts from flat Rust results using reconstruction plans.
    使用重建计划从扁平的 Rust 结果重建嵌套字典。

    For each record, pops __nfk_N prefixed keys and rebuilds nested dicts.
    If all values are None (FK is NULL), the nested field is set to None.
    对于每条记录，弹出 __nfk_N 前缀的键并重建嵌套字典。
    如果所有值都为 None（FK 为 NULL），嵌套字段设为 None。
    """
    for record in results:
        _apply_recon_plans(record, record, recon_plans)


def _apply_recon_plans(flat_record, target, plans):
    """
    Apply reconstruction plans to build nested dicts from a flat record.
    从扁平记录应用重建计划以构建嵌套字典。

    Parameters / 参数:
      flat_record: The original flat dict from Rust (all __nfk_* keys live here).
                   Rust 返回的原始扁平字典（所有 __nfk_* 键都在这里）。
      target: The dict to place the nested result into (root or parent nested dict).
              放置嵌套结果的目标字典（根字典或父级嵌套字典）。
      plans: List of reconstruction plan dicts.
             重建计划字典列表。
    """
    from rest_framework.fields import empty

    for plan in plans:
        nested = {}
        all_none = True

        # Pop prefixed keys from the flat root record → build nested dict
        # 从扁平根记录中弹出前缀键 → 构建嵌套字典
        for child_name, alias in plan['alias_map'].items():
            val = flat_record.pop(alias, None)
            nested[child_name] = val
            if val is not None:
                all_none = False

        # Fill child python_only fields from serializer field defaults
        # 从序列化器字段默认值填充子 python_only 字段
        child_ser = plan['child_serializer']
        for py_field_name in plan['child_python_only']:
            field_obj = child_ser.fields.get(py_field_name)
            if field_obj is None:
                nested[py_field_name] = None
                continue
            try:
                default = field_obj.default
                if default is not empty:
                    nested[py_field_name] = field_obj.to_representation(default)
                    if nested[py_field_name] is not None:
                        all_none = False
                else:
                    nested[py_field_name] = None
            except Exception:
                nested[py_field_name] = None

        # Recurse for child's nested FK (multi-level)
        # Pop from same flat_record, place results into nested dict
        # 递归处理子级嵌套 FK（多级）
        # 从同一个扁平记录弹出，将结果放入嵌套字典中
        if plan['child_plans'] and not all_none:
            _apply_recon_plans(flat_record, nested, plan['child_plans'])

        # Reorder nested dict to match child serializer's field order
        # 重新排序嵌套字典以匹配子序列化器的字段顺序
        if not all_none:
            ordered = {}
            for field_name in child_ser.fields:
                if field_name in nested:
                    ordered[field_name] = nested[field_name]
            target[plan['field_name']] = ordered
        else:
            # If FK is NULL, all child values are None → set field to None
            # 如果 FK 为 NULL，所有子值都为 None → 设置字段为 None
            target[plan['field_name']] = None


def _reorder_to_field_order(results, serializer):
    """
    Reorder result dict keys to match serializer's Meta.fields order in-place.
    就地重新排序结果字典的键以匹配序列化器的 Meta.fields 顺序。

    Rust's HashMap does not preserve insertion order, so the dict keys returned
    by Rust are in arbitrary (and non-deterministic) order. This function
    reorders them to match the declared field order on the serializer.
    Rust 的 HashMap 不保持插入顺序，因此 Rust 返回的字典键是任意的（且不确定的）
    顺序。此函数将它们重新排序以匹配序列化器上声明的字段顺序。
    """
    field_order = list(serializer.fields.keys())
    for i, record in enumerate(results):
        ordered = {}
        for name in field_order:
            if name in record:
                ordered[name] = record[name]
        results[i] = ordered


class RustListSerializer(serializers.ListSerializer):
    """
    Replaces DRF's ListSerializer for many=True serialization.
    替代 DRF 的 ListSerializer，用于 many=True 的序列化。

    Instead of iterating through each instance and calling to_representation()
    one by one (O(N) Python calls), this generates a single SQL query and
    delegates to Rust for bulk execution — returning all results at once.
    不再逐个遍历实例并逐个调用 to_representation()（O(N) 次 Python 调用），
    而是生成一条 SQL 查询并委托给 Rust 批量执行 —— 一次性返回所有结果。
    """

    def to_representation(self, data):
        """
        Convert a list of object instances to a list of dicts of primitive datatypes.
        将对象实例列表转换为原始数据类型的字典列表。

        Attempts the Rust path first; falls back to standard DRF on failure.
        优先尝试 Rust 路径；失败时回退到标准 DRF。
        """
        from .field_classifier import classify_fields
        from .schema_builder import build_schema, schema_to_json
        from .sql_generator import generate_main_sql
        from .settings_extractor import databases_to_json, settings_to_json
        from .python_filler import fill_python_only_fields

        child_serializer = self.child

        # Check for use_rust flag on the child serializer
        # 检查子序列化器上的 use_rust 标志
        if not getattr(child_serializer, "use_rust", True):
            return super().to_representation(data)

        # If Rust engine is not available, fall back to standard DRF
        # 如果 Rust 引擎不可用，回退到标准 DRF
        if not RUST_AVAILABLE:
            return super().to_representation(data)

        # Ensure we have a queryset (not a plain list of instances)
        # 确保我们有一个 QuerySet（而不是普通的实例列表）
        #
        # Common case: paginator.paginate_queryset() returns a plain list of
        # model instances, not a QuerySet. We must reconstruct a QuerySet from
        # the PKs so we can generate ONE merged SQL instead of N individual queries.
        # 常见场景：paginator.paginate_queryset() 返回的是模型实例的普通列表，
        # 而不是 QuerySet。我们必须从 PK 重建 QuerySet，以生成一条合并的 SQL，
        # 而不是 N 条单独的查询。
        from django.db import models
        original_instances = None  # 原始实例列表（保留动态属性如 _my_URL）
        if isinstance(data, models.manager.BaseManager):
            queryset = data.all()
        elif isinstance(data, models.QuerySet):
            queryset = data
        elif isinstance(data, (list, tuple)) and data:
            # Paginated list of model instances → reconstruct QuerySet by PKs
            # 分页后的模型实例列表 → 通过 PK 重建 QuerySet
            first = data[0]
            if isinstance(first, models.Model):
                model = type(first)
                pk_list = [obj.pk for obj in data]
                # Preserve original ordering via CASE WHEN
                # 通过 CASE WHEN 保留原始排序
                from django.db.models import Case, When, IntegerField
                ordering = Case(
                    *[When(pk=pk, then=pos) for pos, pk in enumerate(pk_list)],
                    output_field=IntegerField()
                )
                queryset = model.objects.filter(pk__in=pk_list).order_by(ordering)
                # Save original instances for python_only filling — they may have
                # dynamic attributes (e.g., _my_URL) set by serializer __init__
                # that don't exist on fresh DB instances.
                # 保留原始实例用于 python_only 填充——它们可能有序列化器 __init__
                # 设置的动态属性（如 _my_URL），新从 DB 取的实例上没有这些属性。
                original_instances = data
            else:
                return super().to_representation(data)
        else:
            # Not a queryset or list, fall back to standard DRF
            # 不是 QuerySet 或列表，回退到标准 DRF
            return super().to_representation(data)

        try:
            return self._rust_to_representation(
                queryset, child_serializer, original_instances=original_instances)
        except Exception as e:
            # On any Rust-path failure, log warning and fall back gracefully
            # 任何 Rust 路径失败时，记录警告并优雅地回退
            warnings.warn(
                "RustModelSerializer: Rust path failed ({}), falling back to Python. "
                "Error: {}".format(type(e).__name__, e),
                RuntimeWarning,
                stacklevel=2,
            )
            logger.warning("Rust path failed, falling back: %s", e, exc_info=True)
            # Disable Rust on child serializer to prevent N individual Rust calls
            # in fallback. DRF's ListSerializer.to_representation() iterates items
            # and calls child.to_representation(item) for each one — without this
            # guard, 1000 items would trigger 1000 separate Rust SQL calls.
            # 在回退时禁用子序列化器的 Rust，防止 N 次单独的 Rust 调用。
            # DRF 的 ListSerializer.to_representation() 会遍历项目并对每个项目
            # 调用 child.to_representation(item) —— 如果不加此保护，
            # 1000 个项目将触发 1000 次独立的 Rust SQL 调用。
            child_serializer.use_rust = False
            try:
                return super().to_representation(data)
            finally:
                child_serializer.use_rust = True

    def _rust_to_representation(self, queryset, child_serializer,
                                original_instances=None):
        """
        Execute the Rust serialization path for a queryset (many=True).
        对 QuerySet 执行 Rust 序列化路径（many=True）。

        Parameters / 参数:
          queryset: Django QuerySet for SQL generation.
          child_serializer: The child serializer instance.
          original_instances: Optional list of original model instances (from
              paginator). Used for python_only filling to preserve dynamic
              attributes (e.g., _my_URL) that don't exist on fresh DB instances.
              可选的原始模型实例列表（来自分页器）。用于 python_only 填充，
              保留新 DB 实例上不存在的动态属性（如 _my_URL）。
        """
        from .field_classifier import classify_fields, ClassificationResult
        from .schema_builder import build_schema, schema_to_json
        from .sql_generator import generate_main_sql
        from .settings_extractor import databases_to_json, settings_to_json
        from .python_filler import fill_python_only_fields
        from django.db import router

        # Clear select_related/prefetch_related — we generate our own SQL
        # 清除 select_related/prefetch_related —— 我们生成自己的 SQL
        queryset = queryset.select_related(None).prefetch_related(None)

        # Classify fields into categories (sql, prefetch, python_only, nested_fk)
        # 将字段分类（sql、prefetch、python_only、nested_fk）
        classification = classify_fields(child_serializer)

        # Flatten nested FK fields into virtual sql_fields for SQL JOIN optimization
        # 将嵌套 FK 字段展平为虚拟 sql_fields 以进行 SQL JOIN 优化
        nfk_recon_plans = []
        if classification.nested_fk_fields:
            extra_sql_fields, nfk_recon_plans = _flatten_nested_fk_fields(
                classification.nested_fk_fields
            )
            if extra_sql_fields:
                # Build augmented classification with flattened fields
                # 构建包含展平字段的增强分类
                classification = ClassificationResult(
                    sql_fields=classification.sql_fields + extra_sql_fields,
                    prefetch_fields=classification.prefetch_fields,
                    python_only_fields=classification.python_only_fields,
                    nested_fk_fields=[],
                    model=classification.model,
                    db_table=classification.db_table,
                )

        # Build schema from classification (describes field structure for Rust)
        # 从分类构建 schema（为 Rust 描述字段结构）
        schema = build_schema(child_serializer, classification=classification)
        schema_json = schema_to_json(schema)

        # Generate the main SQL query (uses Django compiler for WHERE/ORDER BY/LIMIT)
        # 生成主 SQL 查询（使用 Django 编译器获取 WHERE/ORDER BY/LIMIT）
        db_alias, main_sql = generate_main_sql(classification, queryset)
        sql_map = {db_alias: main_sql}
        sql_map_json = json.dumps(sql_map)

        # Extract database connection config and Django/DRF settings
        # 提取数据库连接配置和 Django/DRF 设置
        databases_json = databases_to_json()
        settings_json = settings_to_json()

        # Call Rust engine — GIL is released inside for SQL execution
        # 调用 Rust 引擎 —— 内部会释放 GIL 进行 SQL 执行
        rust_results = _rust_engine.execute_serialization(
            schema_json, sql_map_json, databases_json, settings_json
        )

        # rust_results is list[dict] — Python native objects returned by Rust
        # rust_results 是 list[dict] —— Rust 返回的 Python 原生对象

        # Reconstruct nested dicts from flat results (nested FK optimization)
        # 从扁平结果重建嵌套字典（嵌套 FK 优化）
        if nfk_recon_plans:
            _reconstruct_nested_dicts(rust_results, nfk_recon_plans)

        # Fill python_only_fields if needed (SerializerMethodField, source='*', etc.)
        # Pass original_serializer to preserve dynamic defaults set in __init__.
        # 如果需要则填充 python_only_fields（SerializerMethodField、source='*' 等）
        # 传递 original_serializer 以保留 __init__ 中设置的动态默认值。
        if classification.python_only_fields:
            fill_python_only_fields(
                results=rust_results,
                python_only_fields=classification.python_only_fields,
                model=classification.model,
                serializer_class=type(child_serializer),
                context=child_serializer.context if hasattr(child_serializer, 'context') else {},
                instances=original_instances,
                original_serializer=child_serializer,
            )

        # Reorder fields to match serializer's Meta.fields declaration order
        # 重新排序字段以匹配序列化器的 Meta.fields 声明顺序
        _reorder_to_field_order(rust_results, child_serializer)

        return rust_results

    @property
    def data(self):
        """
        Wrap the result in ReturnList for DRF compatibility.
        用 ReturnList 包装结果以兼容 DRF。
        """
        ret = super(serializers.ListSerializer, self).data
        return ReturnList(ret, serializer=self)


class RustModelSerializer(serializers.ModelSerializer):
    """
    Drop-in replacement for DRF ModelSerializer.
    DRF ModelSerializer 的直接替代品。

    Read path (to_representation): delegated to Rust via SQL generation.
    读取路径 (to_representation)：通过 SQL 生成委托给 Rust。
    Write path (create, update, validate): fully transparent to native ModelSerializer.
    写入路径 (create, update, validate)：完全透明地使用原生 ModelSerializer。

    Set use_rust = False on a subclass to disable Rust acceleration.
    在子类上设置 use_rust = False 可禁用 Rust 加速。
    """

    # Set to False on a subclass to disable Rust acceleration
    # 在子类上设置为 False 可禁用 Rust 加速
    use_rust = True

    @classmethod
    def many_init(cls, *args, **kwargs):
        """
        Override many_init to use RustListSerializer instead of DRF's ListSerializer.
        重写 many_init 以使用 RustListSerializer 替代 DRF 的 ListSerializer。

        Called when many=True is passed to the serializer constructor.
        当 many=True 传入序列化器构造函数时被调用。
        """
        # Fall back to standard many_init if Rust is not available or disabled
        # 如果 Rust 不可用或已禁用，回退到标准 many_init
        if not RUST_AVAILABLE or not getattr(cls, "use_rust", True):
            return super().many_init(*args, **kwargs)

        # Separate kwargs for list serializer vs child serializer
        # 分离列表序列化器和子序列化器的参数
        list_kwargs = {}
        for key in _LIST_SERIALIZER_KWARGS_REMOVE:
            value = kwargs.pop(key, None)
            if value is not None:
                list_kwargs[key] = value

        # Create child serializer instance
        # 创建子序列化器实例
        list_kwargs["child"] = cls(*args, **kwargs)

        list_kwargs.update({
            key: value for key, value in kwargs.items()
            if key in _LIST_SERIALIZER_KWARGS
        })

        # Use RustListSerializer unless the user specified a custom list_serializer_class
        # 使用 RustListSerializer，除非用户指定了自定义的 list_serializer_class
        meta = getattr(cls, "Meta", None)
        list_serializer_class = getattr(meta, "list_serializer_class", None)
        if list_serializer_class is None:
            list_serializer_class = RustListSerializer

        return list_serializer_class(*args, **list_kwargs)

    def to_representation(self, instance):
        """
        Single object serialization via Rust path.
        通过 Rust 路径进行单对象序列化。

        Falls back to standard DRF on failure.
        失败时回退到标准 DRF。
        """
        # If instance is None (NULL FK, empty detail view), delegate to DRF
        # 如果 instance 为 None（FK 为 NULL、空的详情视图），委托给 DRF
        # If Rust is disabled or unavailable, use standard DRF
        # 如果 Rust 被禁用或不可用，使用标准 DRF
        if instance is None or not self.use_rust or not RUST_AVAILABLE:
            return super().to_representation(instance)

        try:
            return self._rust_to_representation(instance)
        except Exception as e:
            # On any failure, log warning and fall back gracefully
            # 任何失败时，记录警告并优雅地回退
            warnings.warn(
                "RustModelSerializer: Rust path failed ({}), falling back to Python. "
                "Error: {}".format(type(e).__name__, e),
                RuntimeWarning,
                stacklevel=2,
            )
            logger.warning("Rust path failed for single object, falling back: %s", e, exc_info=True)
            return super().to_representation(instance)

    def _rust_to_representation(self, instance):
        """
        Execute Rust serialization for a single object.
        对单个对象执行 Rust 序列化。

        Builds a queryset filtering by pk, then follows the same pipeline as
        RustListSerializer (classify → flatten → schema → SQL → Rust → reconstruct → fill).
        构建按 pk 过滤的 QuerySet，然后遵循与 RustListSerializer 相同的流程
        （分类 → 展平 → schema → SQL → Rust → 重建 → 填充）。
        """
        from .field_classifier import classify_fields, ClassificationResult
        from .schema_builder import build_schema, schema_to_json
        from .sql_generator import generate_main_sql
        from .settings_extractor import databases_to_json, settings_to_json
        from .python_filler import fill_python_only_fields
        from django.db import router

        model = self.Meta.model
        pk_value = instance.pk

        # Build a queryset for this single instance (filter by pk)
        # 为这个单一实例构建 QuerySet（按 pk 过滤）
        queryset = model.objects.filter(pk=pk_value)
        queryset = queryset.select_related(None).prefetch_related(None)

        # Classify fields into categories (sql, prefetch, python_only, nested_fk)
        # 将字段分类（sql、prefetch、python_only、nested_fk）
        classification = classify_fields(self)

        # Flatten nested FK fields into virtual sql_fields for SQL JOIN optimization
        # 将嵌套 FK 字段展平为虚拟 sql_fields 以进行 SQL JOIN 优化
        nfk_recon_plans = []
        if classification.nested_fk_fields:
            extra_sql_fields, nfk_recon_plans = _flatten_nested_fk_fields(
                classification.nested_fk_fields
            )
            if extra_sql_fields:
                # Build augmented classification with flattened fields
                # 构建包含展平字段的增强分类
                classification = ClassificationResult(
                    sql_fields=classification.sql_fields + extra_sql_fields,
                    prefetch_fields=classification.prefetch_fields,
                    python_only_fields=classification.python_only_fields,
                    nested_fk_fields=[],
                    model=classification.model,
                    db_table=classification.db_table,
                )

        # Build schema from classification (describes field structure for Rust)
        # 从分类构建 schema（为 Rust 描述字段类型和结构）
        schema = build_schema(self, classification=classification)
        schema_json = schema_to_json(schema)

        # Generate main SQL (with pk filter in WHERE clause)
        # 生成主 SQL（WHERE 子句中带有 pk 过滤）
        db_alias, main_sql = generate_main_sql(classification, queryset)
        sql_map = {db_alias: main_sql}
        sql_map_json = json.dumps(sql_map)

        # Extract configs
        # 提取配置
        databases_json = databases_to_json()
        settings_json = settings_to_json()

        # Call Rust engine
        # 调用 Rust 引擎
        rust_results = _rust_engine.execute_serialization(
            schema_json, sql_map_json, databases_json, settings_json
        )

        # Return empty dict if no results (shouldn't happen for a valid pk)
        # 如果没有结果返回空字典（对于有效的 pk 不应该发生）
        if not rust_results:
            return {}

        result = rust_results[0]

        # Reconstruct nested dicts from flat results (nested FK optimization)
        # 从扁平结果重建嵌套字典（嵌套 FK 优化）
        if nfk_recon_plans:
            _reconstruct_nested_dicts(rust_results, nfk_recon_plans)

        # Fill python_only_fields if any exist
        # Pass original_serializer=self to preserve dynamic defaults set in __init__.
        # 如果存在 python_only_fields 则进行填充
        # 传递 original_serializer=self 以保留 __init__ 中设置的动态默认值。
        if classification.python_only_fields:
            fill_python_only_fields(
                results=[result],
                python_only_fields=classification.python_only_fields,
                model=model,
                serializer_class=type(self),
                context=self.context if hasattr(self, "context") else {},
                original_serializer=self,
            )

        # Reorder fields to match serializer's Meta.fields declaration order
        # 重新排序字段以匹配序列化器的 Meta.fields 声明顺序
        wrapper = [result]
        _reorder_to_field_order(wrapper, self)
        result = wrapper[0]

        return result
