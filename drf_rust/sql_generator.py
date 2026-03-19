# -*- coding: utf-8 -*-
"""
SQL generation for RustModelSerializer.
RustModelSerializer 的 SQL 生成。

Generates two types of SQL:
生成两种类型的 SQL：

  1. Main SQL: Uses Django's compiled SQL for correct JOINs/WHERE/ORDER BY,
     replaces only the SELECT clause with our custom columns.
     主 SQL：使用 Django 编译的 SQL 以确保正确的 JOINs/WHERE/ORDER BY，
     仅将 SELECT 子句替换为我们自定义的列。

  2. Prefetch SQL templates: One per prefetch_field, with {ids} placeholder
     for the parent primary keys (filled by Rust at runtime).
     预取 SQL 模板：每个 prefetch_field 一个，带有 {ids} 占位符
     用于父级主键（由 Rust 在运行时填充）。

Design choices / 设计选择:
  - Uses Django's queryset compiler for all main-query JOINs to guarantee
    correct table aliases in WHERE/ORDER BY (fixes cross-table filter bugs).
    使用 Django 的 QuerySet 编译器处理所有主查询 JOINs，确保
    WHERE/ORDER BY 中正确的表别名（修复跨表过滤 bug）。
  - Uses connection.ops.quote_name() for dialect-aware quoting (MySQL `, PostgreSQL ", SQLite ").
    使用 connection.ops.quote_name() 进行方言感知的引号处理。
  - Depth-aware keyword search for SQL parsing (correctly handles subqueries).
    深度感知的关键字搜索用于 SQL 解析（正确处理子查询）。
  - Character-by-character param splicing (safe from format string injection).
    逐字符参数拼接（防止格式化字符串注入）。
"""
import datetime
import decimal
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

from django.db import connections

from .field_classifier import (
    ClassificationResult,
    SqlFieldInfo,
    PrefetchFieldInfo,
    JoinStep,
)

logger = logging.getLogger("drf_rust.sql_generator")

# Maximum alias length (PostgreSQL=63, MySQL=64). Use conservative limit.
# 最大别名长度（PostgreSQL=63, MySQL=64）。使用保守限制。
_MAX_ALIAS_LENGTH = 60


# ---------------------------------------------------------------------------
#  Quoting & SQLite helpers / 引号处理 & SQLite 辅助函数
# ---------------------------------------------------------------------------

def _is_sqlite(db_alias: str) -> bool:
    """
    Check if the given database alias uses SQLite backend.
    检查给定的数据库别名是否使用 SQLite 后端。
    """
    connection = connections[db_alias]
    return connection.vendor == "sqlite"


def _get_vendor(db_alias: str) -> str:
    """
    Return the vendor name ('sqlite', 'postgresql', 'mysql') for the given db alias.
    返回给定数据库别名的供应商名称。
    """
    return connections[db_alias].vendor


def _qn(db_alias: str):
    """
    Return a quoting function (connection.ops.quote_name) for the given database.
    返回给定数据库的引号函数 (connection.ops.quote_name)。

    This ensures dialect-correct identifier quoting:
    确保方言正确的标识符引号：
      - MySQL: backticks (`name`)
      - PostgreSQL: double quotes ("name")
      - SQLite: double quotes ("name")
    """
    return connections[db_alias].ops.quote_name


def _bool_cast(col_expr: str, field_type: str, is_sqlite: bool) -> str:
    """Deprecated: use _safe_col_cast instead. Kept for reference."""
    return _safe_col_cast(col_expr, field_type, "sqlite" if is_sqlite else "postgresql")


# Django field types that map to PostgreSQL-specific types unsupported by sqlx Any driver.
# Django 字段类型映射到 sqlx Any 驱动不支持的 PostgreSQL 特定类型。
#
# sqlx Any driver only supports: Null, SmallInt, Integer, BigInt, Real, Double, Text, Blob, Bool.
# sqlx Any 驱动仅支持：Null, SmallInt, Integer, BigInt, Real, Double, Text, Blob, Bool。
#
# Unsupported PG types and their Django field counterparts:
# 不支持的 PG 类型及其对应的 Django 字段：
#   PgTypeInfo(Timestamptz) / PgTypeInfo(Timestamp) → DateTimeField
#   PgTypeInfo(Date)                                 → DateField
#   PgTypeInfo(Time) / PgTypeInfo(Timetz)            → TimeField
#   PgTypeInfo(Jsonb) / PgTypeInfo(Json)             → JSONField
#   PgTypeInfo(Uuid)                                 → UUIDField
#   PgTypeInfo(Numeric)                              → DecimalField
#   PgTypeInfo(Interval)                             → DurationField
_PG_TEXT_CAST_FIELD_TYPES = frozenset({
    "DateTimeField",
    "DateField",
    "TimeField",
    "JSONField",
    "UUIDField",
    "DecimalField",
    "DurationField",
})

# MySQL types that may also need casting (less common but safe to include).
# MySQL 类型也可能需要转换（较少见但包含更安全）。
_MYSQL_TEXT_CAST_FIELD_TYPES = frozenset({
    "JSONField",
    "UUIDField",
})


def _safe_col_cast(col_expr: str, field_type: str, vendor: str) -> str:
    """
    Apply database-specific CAST to handle types unsupported by sqlx Any driver.
    应用数据库特定的 CAST 以处理 sqlx Any 驱动不支持的类型。

    The sqlx Any driver supports only a minimal set of types (Null, SmallInt,
    Integer, BigInt, Real, Double, Text, Blob, Bool). Database-specific types
    like PostgreSQL's Timestamptz, Jsonb, Uuid, Numeric, Date, Time must be
    CAST to TEXT so sqlx can read them as strings.
    sqlx Any 驱动仅支持最小类型集。数据库特定类型如 PostgreSQL 的
    Timestamptz、Jsonb、Uuid、Numeric、Date、Time 必须 CAST 为 TEXT，
    以便 sqlx 可以将它们作为字符串读取。

    Our Rust code already parses these string values into the correct types
    (datetime parsing, decimal formatting, etc.), so CAST to TEXT is safe.
    我们的 Rust 代码已经将这些字符串值解析为正确的类型
    （日期时间解析、十进制格式化等），因此 CAST 为 TEXT 是安全的。

    Rules / 规则:
      - SQLite + BooleanField → CAST(... AS INTEGER)
        (sqlx Any doesn't support SqliteTypeInfo(Bool))
        （sqlx Any 不支持 SqliteTypeInfo(Bool)）
      - PostgreSQL + datetime/json/uuid/decimal/duration → CAST(... AS TEXT)
        (sqlx Any doesn't support these PG-specific types)
        （sqlx Any 不支持这些 PG 特定类型）
      - MySQL + json/uuid → CAST(... AS CHAR)
        (MySQL uses CHAR instead of TEXT for CAST)
        （MySQL 使用 CHAR 而不是 TEXT 进行 CAST）
      - All other cases → no CAST needed
        其他所有情况 → 不需要 CAST
    """
    if vendor == "sqlite":
        if field_type in ("BooleanField", "NullBooleanField"):
            return "CAST({} AS INTEGER)".format(col_expr)
    elif vendor == "postgresql":
        if field_type in _PG_TEXT_CAST_FIELD_TYPES:
            return "CAST({} AS TEXT)".format(col_expr)
    elif vendor == "mysql":
        if field_type in _MYSQL_TEXT_CAST_FIELD_TYPES:
            # MySQL uses CHAR for CAST (TEXT is not valid in MySQL CAST syntax)
            # MySQL 使用 CHAR 进行 CAST（TEXT 在 MySQL CAST 语法中无效）
            return "CAST({} AS CHAR)".format(col_expr)
    return col_expr


# ---------------------------------------------------------------------------
#  Main SQL generation / 主 SQL 生成
# ---------------------------------------------------------------------------

def generate_main_sql(
    classification: ClassificationResult,
    queryset,
    has_python_only_nested: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    Generate the main SQL for all sql_fields.
    为所有 sql_fields 生成主 SQL。

    Approach / 方法:
      1. Add select_related() for all FK/O2O paths needed by sql_fields,
         so Django's compiler builds the correct JOINs.
         为所有 sql_fields 需要的 FK/O2O 路径添加 select_related()，
         使 Django 编译器构建正确的 JOINs。
      2. Compile Django's full SQL (includes correct JOINs, WHERE, ORDER BY, LIMIT).
         编译 Django 的完整 SQL（包含正确的 JOINs、WHERE、ORDER BY、LIMIT）。
      3. Resolve table aliases from Django's query.alias_map.
         从 Django 的 query.alias_map 中解析表别名。
      4. Replace only the SELECT clause with our custom columns.
         仅将 SELECT 子句替换为我们自定义的列。

    Returns (db_alias, complete_sql_string).
    返回 (db_alias, 完整的 SQL 字符串)。

    Note: May raise EmptyResultSet for .none() querysets — caught by serializers.py fallback.
    注意：对于 .none() QuerySet 可能抛出 EmptyResultSet —— 由 serializers.py 回退处理。
    """
    from django.db import router

    model = classification.model
    db_alias = router.db_for_read(model)
    connection = connections[db_alias]
    qn = connection.ops.quote_name  # Dialect-aware identifier quoting / 方言感知的标识符引号
    vendor = connection.vendor       # Database vendor name / 数据库供应商名称

    # --- Step 1: Enrich queryset with select_related for FK/O2O paths ---
    # --- 步骤 1：为 FK/O2O 路径添加 select_related 以丰富 QuerySet ---
    qs = queryset._clone()

    # Ensure deterministic ordering: if no explicit ORDER BY and no Meta.ordering,
    # add ORDER BY pk. Adding select_related JOINs can change PostgreSQL's default
    # row order, leading to non-deterministic results without explicit ORDER BY.
    # 确保确定性排序：如果没有显式 ORDER BY 且没有 Meta.ordering，
    # 添加 ORDER BY pk。添加 select_related JOINs 会改变 PostgreSQL 的默认
    # 行顺序，导致没有显式 ORDER BY 时结果不确定。
    if not qs.query.order_by:
        meta_ordering = getattr(model._meta, 'ordering', [])
        if not meta_ordering or not qs.query.default_ordering:
            qs = qs.order_by('pk')

    related_paths = set()
    for sf in classification.sql_fields:
        if sf.join_chain:
            # Convert join chain to Django's double-underscore path format
            # 将 join 链转换为 Django 的双下划线路径格式
            path = '__'.join(step.field_name for step in sf.join_chain)
            related_paths.add(path)
    if related_paths:
        qs = qs.select_related(*related_paths)

    # --- Step 2: Compile Django's full SQL ---
    # --- 步骤 2：编译 Django 的完整 SQL ---
    compiler = qs.query.get_compiler(using=db_alias)
    django_sql, params = compiler.as_sql()

    # --- Step 3: Resolve table aliases from Django's query ---
    # --- 步骤 3：从 Django 查询中解析表别名 ---
    query = qs.query
    initial_alias = _get_initial_alias(query)  # Base table alias / 基表别名
    alias_cache = {}  # FK path string -> resolved table alias / FK 路径字符串 -> 解析后的表别名

    # --- Step 4: Build our SELECT columns ---
    # --- 步骤 4：构建我们的 SELECT 列 ---
    select_cols = []

    # Use Django's alias-aware quoting: real table names get quoted ("Order_order"),
    # but Django-generated aliases (T1, T7, etc.) stay unquoted to match the FROM clause.
    # 使用 Django 的别名感知引号：真实表名加引号（"Order_order"），
    # 但 Django 生成的别名（T1、T7 等）不加引号以匹配 FROM 子句。
    qn_alias = compiler.quote_name_unless_alias

    # Force include pk column (needed internally for prefetch matching and python_only filling)
    # 强制包含 pk 列（内部需要用于预取匹配和 python_only 填充）
    pk_col = model._meta.pk.column
    select_cols.append('{}.{} AS {}'.format(
        qn_alias(initial_alias), qn(pk_col), qn("pk")))

    for sf in classification.sql_fields:
        if sf.join_chain:
            # Resolve the table alias for this join chain from Django's alias_map
            # 从 Django 的 alias_map 中解析此 join 链的表别名
            path = '__'.join(step.field_name for step in sf.join_chain)
            if path not in alias_cache:
                alias_cache[path] = _resolve_alias_from_query(
                    query, sf.join_chain, initial_alias)
            table_alias = alias_cache[path]
        else:
            # No join chain → use base table alias
            # 无 join 链 → 使用基表别名
            table_alias = initial_alias

        # Build column expression: table_ref."column"
        # Use qn_alias for table ref (handles Django aliases like T7 correctly)
        # 构建列表达式：table_ref."column"
        # 使用 qn_alias 处理表引用（正确处理 Django 别名如 T7）
        col_expr = '{}.{}'.format(qn_alias(table_alias), qn(sf.column))
        # Apply CAST for types unsupported by sqlx Any driver
        # 为 sqlx Any 驱动不支持的类型应用 CAST
        col_expr = _safe_col_cast(col_expr, sf.field_type, vendor)
        select_cols.append('{} AS {}'.format(col_expr, qn(sf.name)))

    # Add internal pk columns for nested children with python_only_fields
    # 为带有 python_only_fields 的嵌套子项添加内部 pk 列
    if has_python_only_nested:
        for internal_alias, info in has_python_only_nested.items():
            select_cols.append(info + ' AS ' + qn(internal_alias))

    # --- Splice params into Django's SQL (make it a complete, executable SQL string) ---
    # --- 将参数拼接到 Django 的 SQL 中（使其成为完整可执行的 SQL 字符串）---
    spliced_sql = _splice_params(django_sql, params, connection)

    # --- Find FROM boundary and replace SELECT clause ---
    # --- 找到 FROM 边界并替换 SELECT 子句 ---
    from_pos = _find_top_level_keyword(spliced_sql, "FROM")
    if from_pos < 0:
        raise ValueError("Cannot find FROM in Django compiled SQL")

    # Preserve DISTINCT if the queryset uses it
    # 如果 QuerySet 使用了 DISTINCT 则保留
    distinct_prefix = "SELECT DISTINCT " if qs.query.distinct else "SELECT "

    rest_of_sql = spliced_sql[from_pos:]  # FROM ... WHERE ... ORDER BY ... LIMIT ...
    complete_sql = distinct_prefix + ", ".join(select_cols) + " " + rest_of_sql

    return db_alias, complete_sql


def _get_initial_alias(query) -> str:
    """
    Get the base table alias from a Django query.
    从 Django 查询中获取基表别名。

    The first key in alias_map is always the base table (the model's own table).
    alias_map 中的第一个键始终是基表（模型自己的表）。
    """
    if query.alias_map:
        return next(iter(query.alias_map))
    return query.model._meta.db_table


def _resolve_alias_from_query(query, join_chain, initial_alias) -> str:
    """
    Resolve the final table alias for a join chain using Django's alias_map.
    使用 Django 的 alias_map 解析 join 链的最终表别名。

    Walks through the join chain, at each step finding the matching Join entry
    in Django's alias_map by matching:
      - parent_alias: the current table alias
      - table_name: the target table
      - join_cols: the (from_column, to_column) pair
    遍历 join 链，每一步在 Django 的 alias_map 中通过匹配以下条件
    找到对应的 Join 条目：
      - parent_alias：当前表别名
      - table_name：目标表
      - join_cols：(from_column, to_column) 对

    This ensures we use the exact same aliases that Django uses in its WHERE/ORDER BY.
    这确保我们使用与 Django 在 WHERE/ORDER BY 中完全相同的别名。
    """
    current_alias = initial_alias

    for step in join_chain:
        found = False
        for alias, join_obj in query.alias_map.items():
            if alias == current_alias:
                continue
            # Skip BaseTable entries (they have no parent_alias)
            # 跳过 BaseTable 条目（它们没有 parent_alias）
            if not hasattr(join_obj, 'parent_alias') or join_obj.parent_alias is None:
                continue
            if (join_obj.parent_alias == current_alias
                    and join_obj.table_name == step.to_table):
                # Verify the join columns match our expected FK relationship
                # 验证 join 列与我们预期的 FK 关系匹配
                if hasattr(join_obj, 'join_cols'):
                    for from_col, to_col in join_obj.join_cols:
                        if from_col == step.from_column and to_col == step.to_column:
                            current_alias = alias
                            found = True
                            break
                if found:
                    break
        if not found:
            # Fallback: try Django's table_map (less precise but still works)
            # 回退：尝试 Django 的 table_map（精度较低但仍可用）
            table_aliases = query.table_map.get(step.to_table, [])
            if table_aliases:
                current_alias = table_aliases[-1]
            else:
                # Last resort: use raw table name (may break if multiple JOINs to same table)
                # 最后手段：使用原始表名（如果同表有多个 JOIN 可能会出错）
                current_alias = step.to_table

    return current_alias


# ---------------------------------------------------------------------------
#  Prefetch SQL generation / 预取 SQL 生成
# ---------------------------------------------------------------------------

def generate_prefetch_sql(
    prefetch_field: PrefetchFieldInfo,
    child_classification: ClassificationResult,
    parent_model,
    db_alias: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Generate a prefetch SQL template for a single prefetch field.
    为单个预取字段生成预取 SQL 模板。

    The SQL contains an {ids} placeholder that Rust fills with actual parent pk
    values at runtime (e.g., "WHERE article_id IN (1,2,3)").
    SQL 中包含 {ids} 占位符，Rust 在运行时用实际的父级 pk 值填充
    （如 "WHERE article_id IN (1,2,3)"）。

    Supports three relation types / 支持三种关系类型:
      - ManyToManyField (forward): JOIN through the intermediate table.
        ManyToManyField（正向）：通过中间表 JOIN。
      - ManyToOneRel (reverse FK): Direct WHERE on FK column.
        ManyToOneRel（反向 FK）：直接在 FK 列上 WHERE。
      - ManyToManyRel (reverse M2M): JOIN through the intermediate table.
        ManyToManyRel（反向 M2M）：通过中间表 JOIN。

    Returns (sql_with_ids_placeholder, join_key_column_name).
    返回 (带 {ids} 占位符的 SQL, join_key 列名)。
    """
    from django.db.models.fields.related import ForeignKey, ManyToManyField
    from django.db.models.fields.related import ManyToOneRel, ManyToManyRel
    from django.db import router

    relation = prefetch_field.relation_field
    related_model = prefetch_field.related_model
    related_table = related_model._meta.db_table

    if db_alias is None:
        db_alias = router.db_for_read(related_model)

    connection = connections[db_alias]
    qn = connection.ops.quote_name  # Dialect-aware quoting / 方言感知引号
    vendor = connection.vendor       # Database vendor name / 数据库供应商名称

    # --- Build child JOINs for sql_fields with join_chains ---
    # --- 为带有 join_chain 的 sql_fields 构建子 JOINs ---
    # Child fields may reference FK/O2O relations (e.g., comment.author.name),
    # requiring additional JOINs in the prefetch SQL.
    # 子字段可能引用 FK/O2O 关系（如 comment.author.name），
    # 需要在预取 SQL 中添加额外的 JOINs。
    child_joins = []
    child_join_aliases = {}

    # --- Build SELECT columns ---
    # --- 构建 SELECT 列 ---
    select_cols = []
    # Always include pk of the related model (for internal use)
    # 始终包含关联模型的 pk（供内部使用）
    pk_col = related_model._meta.pk.column
    select_cols.append('{}.{} AS {}'.format(
        qn(related_table), qn(pk_col), qn("pk")))

    for sf in child_classification.sql_fields:
        if sf.join_chain:
            # Build LEFT JOIN for child's FK/O2O references
            # 为子项的 FK/O2O 引用构建 LEFT JOIN
            table_alias = _ensure_joins(
                child_joins, child_join_aliases, sf.join_chain, related_table, qn)
        else:
            table_alias = related_table

        col_expr = '{}.{}'.format(qn(table_alias), qn(sf.column))
        # Apply CAST for types unsupported by sqlx Any driver
        # 为 sqlx Any 驱动不支持的类型应用 CAST
        col_expr = _safe_col_cast(col_expr, sf.field_type, vendor)
        select_cols.append('{} AS {}'.format(col_expr, qn(sf.name)))

    # --- Determine relation type and build FROM/WHERE clauses ---
    # --- 确定关系类型并构建 FROM/WHERE 子句 ---
    join_key = "__prefetch_join_key"

    if isinstance(relation, ManyToManyField):
        # Forward M2M: related_table → INNER JOIN → through_table
        # 正向 M2M：related_table → INNER JOIN → through_table
        through_model = relation.remote_field.through
        through_table = through_model._meta.db_table
        # Find the FK columns on the through table pointing to parent and related models
        # 找到中间表上指向父模型和关联模型的 FK 列
        source_fk_col, target_fk_col = _find_m2m_fk_columns(
            through_model, parent_model, related_model)

        # Add the join_key column (maps child back to parent pk)
        # 添加 join_key 列（将子项映射回父级 pk）
        select_cols.append('{}.{} AS {}'.format(
            qn(through_table), qn(source_fk_col), qn(join_key)))

        from_clause = (
            "{related} INNER JOIN {through} ON {related}.{rpk} = {through}.{target_fk}"
        ).format(
            related=qn(related_table),
            through=qn(through_table),
            rpk=qn(pk_col),
            target_fk=qn(target_fk_col),
        )
        where_clause = "{through}.{source_fk} IN ({{ids}})".format(
            through=qn(through_table),
            source_fk=qn(source_fk_col),
        )

    elif isinstance(relation, ManyToOneRel):
        # Reverse FK: the FK column is on the related (child) table
        # 反向 FK：FK 列在关联（子）表上
        fk_field = relation.field
        fk_column = fk_field.column

        # The FK column on the child table is our join_key
        # 子表上的 FK 列就是我们的 join_key
        select_cols.append('{}.{} AS {}'.format(
            qn(related_table), qn(fk_column), qn(join_key)))

        from_clause = qn(related_table)
        where_clause = "{related}.{fk} IN ({{ids}})".format(
            related=qn(related_table),
            fk=qn(fk_column),
        )

    elif isinstance(relation, ManyToManyRel):
        # Reverse M2M: same as forward M2M but accessed from the other direction
        # 反向 M2M：与正向 M2M 相同但从另一方向访问
        forward_m2m = relation.field
        through_model = forward_m2m.remote_field.through
        through_table = through_model._meta.db_table
        source_fk_col, target_fk_col = _find_m2m_fk_columns(
            through_model, parent_model, related_model)

        select_cols.append('{}.{} AS {}'.format(
            qn(through_table), qn(source_fk_col), qn(join_key)))

        from_clause = (
            "{related} INNER JOIN {through} ON {related}.{rpk} = {through}.{target_fk}"
        ).format(
            related=qn(related_table),
            through=qn(through_table),
            rpk=qn(pk_col),
            target_fk=qn(target_fk_col),
        )
        where_clause = "{through}.{source_fk} IN ({{ids}})".format(
            through=qn(through_table),
            source_fk=qn(source_fk_col),
        )

    else:
        # Unsupported relation type → skip
        # 不支持的关系类型 → 跳过
        logger.warning("Unsupported relation type for prefetch: %s", type(relation))
        return "", ""

    # --- Assemble the final SQL ---
    # --- 组装最终 SQL ---
    sql_parts = ["SELECT " + ", ".join(select_cols)]
    sql_parts.append("FROM " + from_clause)

    # Add any child JOINs (for child fields that reference FK/O2O)
    # 添加子 JOINs（用于引用 FK/O2O 的子字段）
    for join_sql in child_joins:
        sql_parts.append(join_sql)

    sql_parts.append("WHERE " + where_clause)

    # Add ordering: use Meta.ordering if available, otherwise ORDER BY pk
    # for deterministic results.
    # 添加排序：如果有 Meta.ordering 则使用，否则 ORDER BY pk 以确保确定性结果。
    ordering = _get_model_ordering(related_model, qn)
    if not ordering:
        # Default: order by pk for deterministic results
        # 默认：按 pk 排序以确保确定性结果
        ordering = "{}.{}".format(qn(related_table), qn(pk_col))
    sql_parts.append("ORDER BY " + ordering)

    sql = " ".join(sql_parts)
    return sql, join_key


def _find_m2m_fk_columns(through_model, parent_model, related_model):
    """
    Find the source and target FK columns on a M2M through (intermediate) table.
    找到 M2M 中间表上的源 FK 列和目标 FK 列。

    source_fk_col: FK pointing to parent_model (used in WHERE ... IN clause)
    目标 FK 列：指向 parent_model 的 FK（用于 WHERE ... IN 子句）
    target_fk_col: FK pointing to related_model (used in INNER JOIN ON)
    源 FK 列：指向 related_model 的 FK（用于 INNER JOIN ON）
    """
    from django.db.models.fields.related import ForeignKey

    source_fk_col = None
    target_fk_col = None
    for f in through_model._meta.get_fields():
        if isinstance(f, ForeignKey):
            if f.related_model == parent_model:
                source_fk_col = f.column
            elif f.related_model == related_model:
                target_fk_col = f.column

    # Handle self-referential M2M: both FKs point to the same model
    # 处理自引用 M2M：两个 FK 指向同一个模型
    if parent_model == related_model:
        fks = [f for f in through_model._meta.get_fields()
               if isinstance(f, ForeignKey) and f.related_model == parent_model]
        if len(fks) >= 2:
            source_fk_col = fks[0].column
            target_fk_col = fks[1].column

    return source_fk_col or "id", target_fk_col or "id"


# ---------------------------------------------------------------------------
#  JOIN builder (for prefetch SQL only; main SQL uses Django's JOINs)
#  JOIN 构建器（仅用于预取 SQL；主 SQL 使用 Django 的 JOINs）
# ---------------------------------------------------------------------------

def _ensure_joins(
    joins: list,
    join_aliases: dict,
    join_chain: List[JoinStep],
    root_table: str,
    qn,
) -> str:
    """
    Build LEFT JOIN clauses for a join chain. Returns the alias of the final table.
    为 join 链构建 LEFT JOIN 子句。返回最终表的别名。

    This is used only in prefetch SQL generation (not main SQL, which uses Django's
    built-in JOINs). Each step in the join chain produces a LEFT JOIN if not
    already present.
    仅在预取 SQL 生成中使用（不用于主 SQL，主 SQL 使用 Django 内置的 JOINs）。
    join 链中的每一步如果尚未存在则生成一个 LEFT JOIN。

    Uses counter-based disambiguation to guarantee unique aliases when the same
    table is JOINed multiple times (e.g., two FK fields pointing to the same table).
    使用基于计数器的消歧来保证当同一表被多次 JOIN 时别名唯一
    （如两个 FK 字段指向同一张表）。
    """
    current_alias = root_table
    # Collect all known aliases to prevent collisions
    # 收集所有已知别名以防止冲突
    all_aliases = {root_table} | set(join_aliases.values())

    for step in join_chain:
        # Use (current_alias, from_column, target_table) as unique key
        # 使用 (当前别名, 源列, 目标表) 作为唯一键
        key = (current_alias, step.from_column, step.to_table)
        if key in join_aliases:
            # Already have a JOIN for this exact path → reuse alias
            # 此确切路径已有 JOIN → 重用别名
            current_alias = join_aliases[key]
        else:
            # Need to create a new JOIN
            # 需要创建新的 JOIN
            new_alias = step.to_table
            if new_alias in all_aliases:
                # Disambiguate: append parent alias and field name
                # 消歧：附加父别名和字段名
                base = "{}__{}".format(current_alias, step.field_name)
                new_alias = base
                counter = 2
                # Counter loop guarantees uniqueness
                # 计数器循环保证唯一性
                while new_alias in all_aliases:
                    new_alias = "{}_{}".format(base, counter)
                    counter += 1

            # Truncate overly long aliases (PostgreSQL limit is 63 chars)
            # 截断过长的别名（PostgreSQL 限制为 63 个字符）
            if len(new_alias) > _MAX_ALIAS_LENGTH:
                new_alias = new_alias[:_MAX_ALIAS_LENGTH - 4] + "_{:03d}".format(
                    len(join_aliases) + 1)

            # Generate LEFT JOIN SQL
            # 生成 LEFT JOIN SQL
            join_sql = 'LEFT JOIN {} {} ON {}.{} = {}.{}'.format(
                qn(step.to_table),
                qn(new_alias),
                qn(current_alias),
                qn(step.from_column),
                qn(new_alias),
                qn(step.to_column),
            )
            joins.append(join_sql)
            join_aliases[key] = new_alias
            all_aliases.add(new_alias)
            current_alias = new_alias

    return current_alias


# ---------------------------------------------------------------------------
#  Param splicing (safe from format-string injection)
#  参数拼接（防止格式化字符串注入）
# ---------------------------------------------------------------------------

def _splice_params(sql: str, params, connection) -> str:
    """
    Splice parameters into SQL, replacing %s placeholders one-by-one.
    将参数拼接到 SQL 中，逐一替换 %s 占位符。

    This scans the template character-by-character rather than using Python's
    % formatting, so parameter values containing % characters are safe.
    逐字符扫描 SQL 模板而非使用 Python 的 % 格式化，
    因此包含 % 字符的参数值是安全的。

    Also handles %% escape (Django uses %% for literal % in compiled SQL).
    同时处理 %% 转义（Django 在编译的 SQL 中使用 %% 表示字面量 %）。
    """
    if not params:
        # No params → just unescape %% → %
        # 无参数 → 仅将 %% 反转义为 %
        return sql.replace('%%', '%')

    vendor = connection.vendor
    parts = []
    param_idx = 0
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]
        if ch == '%' and i + 1 < n:
            next_ch = sql[i + 1]
            if next_ch == 's':
                # %s → parameter placeholder → replace with quoted value
                # %s → 参数占位符 → 替换为加引号的值
                if param_idx < len(params):
                    parts.append(_quote_param(params[param_idx], vendor))
                    param_idx += 1
                i += 2
                continue
            elif next_ch == '%':
                # %% → literal percent sign
                # %% → 字面量百分号
                parts.append('%')
                i += 2
                continue
        parts.append(ch)
        i += 1

    return ''.join(parts)


def _quote_param(value, vendor: str) -> str:
    """
    Quote a single parameter value for safe SQL insertion.
    为安全的 SQL 插入引用单个参数值。

    Handles all Python types that Django may pass as query parameters.
    处理 Django 可能传递的所有 Python 类型作为查询参数。
    """
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        # SQLite: use 1/0 (TRUE/FALSE not always supported in older versions)
        # SQLite：使用 1/0（旧版本不总是支持 TRUE/FALSE）
        if vendor == 'sqlite':
            return '1' if value else '0'
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, float):
        return repr(value)  # repr preserves full precision / repr 保留完整精度
    elif isinstance(value, decimal.Decimal):
        return str(value)
    elif isinstance(value, str):
        # Escape single quotes by doubling them
        # 通过双写单引号进行转义
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bytes):
        # Hex literal for binary data
        # 二进制数据的十六进制字面量
        return "X'{}'".format(value.hex())
    elif isinstance(value, datetime.datetime):
        return "'{}'".format(value.isoformat())
    elif isinstance(value, datetime.date):
        return "'{}'".format(value.isoformat())
    elif isinstance(value, datetime.time):
        return "'{}'".format(value.isoformat())
    elif isinstance(value, uuid.UUID):
        return "'{}'".format(str(value))
    elif isinstance(value, (list, tuple)):
        # Shouldn't happen — Django flattens params — but handle gracefully
        # 不应发生 — Django 会展平参数 — 但优雅处理
        return ", ".join(_quote_param(v, vendor) for v in value)
    else:
        # Fallback: convert to string
        # 回退：转换为字符串
        return "'{}'".format(str(value).replace("'", "''"))


# ---------------------------------------------------------------------------
#  Depth-aware SQL keyword search / 深度感知的 SQL 关键字搜索
# ---------------------------------------------------------------------------

def _find_top_level_keyword(sql: str, keyword: str) -> int:
    """
    Find the start position of a keyword at the top level of SQL.
    在 SQL 的顶层查找关键字的起始位置。

    'Top level' means not inside:
    "顶层" 意味着不在以下内容内部：
      - Parentheses (subqueries, function calls) / 括号（子查询、函数调用）
      - Single-quoted strings / 单引号字符串
      - Double-quoted identifiers / 双引号标识符
      - Backtick-quoted identifiers (MySQL) / 反引号标识符（MySQL）

    This is critical for correctly parsing SQL with subqueries — a naive
    str.find() would match keywords inside subqueries and break the SQL.
    这对于正确解析带子查询的 SQL 至关重要 —— 朴素的 str.find()
    会匹配子查询内的关键字并破坏 SQL。

    Returns the index of the keyword start, or -1 if not found.
    返回关键字起始位置的索引，未找到则返回 -1。
    """
    target = keyword.upper()
    upper = sql.upper()
    target_len = len(target)
    n = len(sql)
    depth = 0  # Parenthesis nesting depth / 括号嵌套深度
    i = 0

    while i < n:
        ch = sql[i]

        # Skip single-quoted strings (e.g., 'value' or 'it''s escaped')
        # 跳过单引号字符串（如 'value' 或 'it''s escaped'）
        if ch == "'":
            i += 1
            while i < n:
                if sql[i] == "'" and i + 1 < n and sql[i + 1] == "'":
                    i += 2  # Escaped quote / 转义引号
                elif sql[i] == "'":
                    i += 1
                    break
                else:
                    i += 1
            continue

        # Skip double-quoted identifiers (e.g., "table_name")
        # 跳过双引号标识符（如 "table_name"）
        if ch == '"':
            i += 1
            while i < n:
                if sql[i] == '"' and i + 1 < n and sql[i + 1] == '"':
                    i += 2  # Escaped double quote / 转义双引号
                elif sql[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
            continue

        # Skip backtick-quoted identifiers (MySQL style, e.g., `table_name`)
        # 跳过反引号标识符（MySQL 风格，如 `table_name`）
        if ch == '`':
            i += 1
            while i < n:
                if sql[i] == '`' and i + 1 < n and sql[i + 1] == '`':
                    i += 2  # Escaped backtick / 转义反引号
                elif sql[i] == '`':
                    i += 1
                    break
                else:
                    i += 1
            continue

        # Track parenthesis depth
        # 跟踪括号深度
        if ch == '(':
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth = max(0, depth - 1)
            i += 1
            continue

        # At depth 0 (top level): check for keyword match with word boundaries
        # 在深度 0（顶层）：检查关键字匹配并验证词边界
        if depth == 0 and i + target_len <= n:
            if upper[i:i + target_len] == target:
                # Verify word boundaries (not part of a longer identifier)
                # 验证词边界（不是更长标识符的一部分）
                before_ok = (i == 0 or not upper[i - 1].isalnum())
                after_ok = (i + target_len >= n
                            or not upper[i + target_len].isalnum())
                if before_ok and after_ok:
                    return i

        i += 1

    return -1


# ---------------------------------------------------------------------------
#  Model ordering helper / 模型排序辅助函数
# ---------------------------------------------------------------------------

def _get_model_ordering(model, qn) -> str:
    """
    Get the default ordering SQL for a model, based on Meta.ordering.
    根据 Meta.ordering 获取模型的默认排序 SQL。

    Handles:
    处理：
      - String-based ordering (e.g., '-created_at', 'name')
        字符串排序（如 '-created_at'、'name'）
      - Gracefully skips '?' (random ordering) and expression-based ordering
        优雅地跳过 '?'（随机排序）和基于表达式的排序
    """
    ordering = getattr(model._meta, "ordering", [])
    if not ordering:
        return ""

    parts = []
    for entry in ordering:
        # Skip non-string entries (expression-based ordering like F('field').desc())
        # 跳过非字符串条目（基于表达式的排序如 F('field').desc()）
        if not isinstance(entry, str):
            continue
        # Skip random ordering ('?')
        # 跳过随机排序（'?'）
        if entry == "?" or entry == "-?":
            continue

        desc = False
        field_name = entry
        if field_name.startswith("-"):
            desc = True
            field_name = field_name[1:]

        try:
            django_field = model._meta.get_field(field_name)
            col = getattr(django_field, "column", field_name)
        except Exception:
            col = field_name

        table = model._meta.db_table
        expr = "{}.{}".format(qn(table), qn(col))
        if desc:
            expr += " DESC"
        parts.append(expr)

    return ", ".join(parts)
