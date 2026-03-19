// ============================================================================
// lib.rs — PyO3 entry point for the Rust engine.
// lib.rs — Rust 引擎的 PyO3 入口点。
//
// This is the main module exposed to Python via PyO3. It provides:
// 这是通过 PyO3 暴露给 Python 的主模块。它提供：
//   - execute_serialization(): The single Python-callable function that receives
//     4 JSON parameters, executes SQL, and returns list[dict].
//     execute_serialization()：唯一的 Python 可调用函数，接收 4 个 JSON 参数，
//     执行 SQL，返回 list[dict]。
//   - TypedValue → Python type conversion (no PyAny — concrete types only).
//     TypedValue → Python 类型转换（禁止 PyAny —— 仅使用具体类型）。
//   - Schema caching (hash-based) to avoid repeated JSON deserialization.
//     Schema 缓存（基于哈希）以避免重复的 JSON 反序列化。
//   - Global Tokio runtime (single-threaded, reused across requests).
//     全局 Tokio 运行时（单线程，跨请求复用）。
// ============================================================================

mod schema;
mod types;
mod pool;
mod executor;
mod datetime;
mod decimal;

use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use schema::Schema;
use types::{TypedValue, DbConfig, DjangoSettings};

// Global Tokio runtime, reused across requests to avoid repeated creation overhead.
// 全局 Tokio 运行时，跨请求复用以避免重复创建的开销。
// Uses single-threaded runtime (current_thread) since we're blocking on it
// from Python's synchronous call context.
// 使用单线程运行时（current_thread），因为我们是从 Python 的同步调用上下文中阻塞它。
static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();

/// Get or create the global Tokio runtime.
/// 获取或创建全局 Tokio 运行时。
fn get_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rust_engine: failed to create tokio runtime")
    })
}

// Schema cache: avoids re-parsing the same schema JSON on every request.
// Schema 缓存：避免每次请求都重新解析相同的 schema JSON。
// Key is the hash of the JSON string; value is the parsed Schema wrapped in Arc.
// 键是 JSON 字符串的哈希值；值是用 Arc 包装的解析后的 Schema。
static SCHEMA_CACHE: OnceCell<std::sync::Mutex<HashMap<u64, Arc<Schema>>>> = OnceCell::new();

/// Get a cached schema or parse and cache it.
/// 获取缓存的 schema 或解析并缓存它。
fn get_or_parse_schema(schema_json: &str) -> Result<Arc<Schema>, String> {
    use std::hash::{Hash, Hasher};
    // Compute hash of the schema JSON string
    // 计算 schema JSON 字符串的哈希值
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schema_json.hash(&mut hasher);
    let key = hasher.finish();

    let cache = SCHEMA_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()));

    // Check cache first (fast path)
    // 先检查缓存（快速路径）
    let guard = cache.lock().map_err(|e: std::sync::PoisonError<_>| e.to_string())?;
    if let Some(schema) = guard.get(&key) {
        return Ok(Arc::clone(schema));
    }
    drop(guard);  // Release lock before parsing (parsing is expensive)
                  // 解析前释放锁（解析代价较高）

    // Parse schema JSON via serde
    // 通过 serde 解析 schema JSON
    let schema: Schema = serde_json::from_str(schema_json).map_err(|e| e.to_string())?;
    let schema = Arc::new(schema);

    // Insert into cache
    // 插入缓存
    let mut guard = cache.lock().map_err(|e: std::sync::PoisonError<_>| e.to_string())?;
    guard.insert(key, Arc::clone(&schema));
    Ok(schema)
}

/// Convert a TypedValue into a Python object using concrete types (no PyAny).
/// 将 TypedValue 转换为 Python 对象，使用具体类型（禁止 PyAny）。
///
/// Type mapping / 类型映射:
///   TypedValue::None   → Python None
///   TypedValue::Bool   → Python bool
///   TypedValue::Int    → Python int
///   TypedValue::Float  → Python float
///   TypedValue::Str    → Python str
///   TypedValue::List   → Python list[dict] (recursive for prefetch fields)
///                        Python list[dict]（预取字段递归）
///   TypedValue::Json   → Python native (dict/list/str/int/float/bool/None)
///                        Python 原生类型
fn typed_value_to_py<'py>(py: Python<'py>, value: &TypedValue) -> PyResult<Bound<'py, pyo3::PyAny>> {
    match value {
        TypedValue::None => Ok(py.None().into_bound(py)),
        TypedValue::Bool(true) => Ok(true.into_pyobject(py)?.to_owned().into_any()),
        TypedValue::Bool(false) => Ok(false.into_pyobject(py)?.to_owned().into_any()),
        TypedValue::Int(v) => Ok(v.into_pyobject(py)?.into_any()),
        TypedValue::Float(v) => Ok(v.into_pyobject(py)?.into_any()),
        TypedValue::Str(v) => Ok(PyString::new(py, v).into_any()),
        TypedValue::List(items) => {
            // Recursive: each item is a HashMap<String, TypedValue> → PyDict
            // 递归：每个项是 HashMap<String, TypedValue> → PyDict
            let py_list = PyList::empty(py);
            for item in items {
                let py_dict = typed_value_map_to_pydict(py, item)?;
                py_list.append(py_dict)?;
            }
            Ok(py_list.into_any())
        }
        TypedValue::Json(v) => {
            // JSONField: convert serde_json::Value to Python native
            // JSONField：将 serde_json::Value 转换为 Python 原生类型
            json_value_to_py(py, v)
        }
    }
}

/// Convert a serde_json::Value to Python object (for JSONField support).
/// 将 serde_json::Value 转换为 Python 对象（用于 JSONField 支持）。
///
/// Handles all JSON types: null, bool, number, string, array, object.
/// 处理所有 JSON 类型：null、bool、number、string、array、object。
fn json_value_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Bound<'py, pyo3::PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None().into_bound(py)),
        serde_json::Value::Bool(true) => Ok(true.into_pyobject(py)?.to_owned().into_any()),
        serde_json::Value::Bool(false) => Ok(false.into_pyobject(py)?.to_owned().into_any()),
        serde_json::Value::Number(n) => {
            // Try integer first, then float, then string fallback
            // 先尝试整数，然后浮点数，最后字符串回退
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any())
            } else {
                Ok(PyString::new(py, &n.to_string()).into_any())
            }
        }
        serde_json::Value::String(s) => Ok(PyString::new(py, s).into_any()),
        serde_json::Value::Array(arr) => {
            // Recursive: JSON array → Python list
            // 递归：JSON 数组 → Python 列表
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(json_value_to_py(py, item)?)?;
            }
            Ok(py_list.into_any())
        }
        serde_json::Value::Object(obj) => {
            // Recursive: JSON object → Python dict
            // 递归：JSON 对象 → Python 字典
            let py_dict = PyDict::new(py);
            for (k, v) in obj {
                py_dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(py_dict.into_any())
        }
    }
}

/// Convert a HashMap<String, TypedValue> to a PyDict.
/// 将 HashMap<String, TypedValue> 转换为 PyDict。
///
/// This is called for each result row: the HashMap keys become dict keys,
/// and TypedValue values are converted to concrete Python types.
/// 这个函数为每一行结果调用：HashMap 的键成为字典键，
/// TypedValue 的值被转换为具体的 Python 类型。
fn typed_value_map_to_pydict<'py>(
    py: Python<'py>,
    map: &HashMap<String, TypedValue>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        dict.set_item(key, typed_value_to_py(py, value)?)?;
    }
    Ok(dict)
}

/// Main function exposed to Python.
/// 暴露给 Python 的主函数。
///
/// Parameters (all JSON strings) / 参数（全部为 JSON 字符串）:
///   schema_json    - Schema describing field structure (types, prefetch, etc.)
///                    描述字段结构的 Schema（类型、预取等）
///   sql_map_json   - {"db_alias": "SELECT ..."} — main SQL to execute
///                    {"db_alias": "SELECT ..."} — 要执行的主 SQL
///   databases_json - DATABASES config for Rust connections
///                    用于 Rust 连接的 DATABASES 配置
///   settings_json  - Django datetime/timezone settings
///                    Django 日期时间/时区设置
///
/// Returns / 返回: list[dict] — Python native objects (no PyAny)
///                 list[dict] — Python 原生对象（不使用 PyAny）
///
/// Execution flow / 执行流程:
///   1. Parse 4 JSON inputs / 解析 4 个 JSON 输入
///   2. Release GIL / 释放 GIL
///   3. Execute SQL via sqlx (main + prefetch, concurrently) / 通过 sqlx 执行 SQL
///   4. Re-acquire GIL / 重新获取 GIL
///   5. Build PyList[PyDict] from results / 从结果构建 PyList[PyDict]
#[pyfunction]
#[allow(deprecated)]
fn execute_serialization(
    py: Python<'_>,
    schema_json: &str,
    sql_map_json: &str,
    databases_json: &str,
    settings_json: &str,
) -> PyResult<Py<PyList>> {
    let t0 = Instant::now();

    // Parse 4 JSON inputs / 解析 4 个 JSON 输入
    let schema = get_or_parse_schema(schema_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Schema parse error: {}", e)))?;
    let sql_map: HashMap<String, String> = serde_json::from_str(sql_map_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("SQL map parse error: {}", e)))?;
    let databases: HashMap<String, DbConfig> = serde_json::from_str(databases_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Databases parse error: {}", e)))?;
    let settings: DjangoSettings = serde_json::from_str(settings_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Settings parse error: {}", e)))?;

    // Release GIL and execute SQL in Tokio runtime
    // 释放 GIL 并在 Tokio 运行时中执行 SQL
    // This allows Python threads to continue while Rust handles I/O.
    // 这允许 Python 线程在 Rust 处理 I/O 时继续运行。
    #[allow(deprecated)]
    let results: Result<Vec<HashMap<String, TypedValue>>, String> = py.allow_threads(|| {
        get_runtime().block_on(async {
            executor::execute_all(&schema, &sql_map, &databases, &settings).await
        })
    });

    let rows = results
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

    // Build Python list[dict] with concrete types (GIL re-acquired here)
    // 使用具体类型构建 Python list[dict]（此处已重新获取 GIL）
    let py_list = PyList::empty(py);
    for row in &rows {
        let py_dict = typed_value_map_to_pydict(py, row)?;
        py_list.append(py_dict)?;
    }
    // Log execution timing / 记录执行耗时
    let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "[rust_engine] execute_serialization: total={:.2}ms rows={}",
        total_ms, rows.len()
    );

    Ok(py_list.unbind())
}

/// PyO3 module definition — registers the module and its functions with Python.
/// PyO3 模块定义 — 向 Python 注册模块及其函数。
#[pymodule]
fn rust_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Install default sqlx database drivers (postgres, mysql, sqlite)
    // 安装默认的 sqlx 数据库驱动（postgres、mysql、sqlite）
    sqlx::any::install_default_drivers();
    // Register the execute_serialization function
    // 注册 execute_serialization 函数
    m.add_function(wrap_pyfunction!(execute_serialization, m)?)?;
    Ok(())
}
