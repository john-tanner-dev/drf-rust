# drf-rust

**为 Django REST Framework 提供最高 37× 序列化性能提升**

**基于 Rust 的 Django REST Framework的 `ModelSerializer` 即插即用加速方案**

只需替换：

```python
ModelSerializer → RustModelSerializer
```

无需任何其他修改。

完全兼容：

* serializers
* views
* pagination
* filtering
* URL 配置

你的代码将 **保持完全一致地运行**。

---

## 性能测试（Benchmark）

基于生产环境 PostgreSQL 数据库，测试一个 **3 层嵌套 serializer**。

|     行数 |     DRF | drf-rust |        加速 |
| -----: | ------: | -------: | --------: |
|    100 |  0.042s |   0.019s |  **2.2x** |
|    400 |  0.888s |   0.035s | **25.4x** |
|    800 |  1.775s |   0.056s | **31.7x** |
|  1,200 |  2.541s |   0.079s | **32.2x** |
|  2,400 |  5.090s |   0.144s | **35.3x** |
|  4,800 | 10.356s |   0.278s | **37.3x** |
|  9,600 | 20.442s |   0.558s | **36.6x** |
| 19,200 | 39.753s |   1.208s | **32.9x** |
| 38,400 | 79.135s |   4.145s | **19.1x** |

**关键结论**

* **最高 37x 倍性能提升**

---

## 使用示例

### 原始写法

```python
from rest_framework import serializers

class ArticleSerializer(serializers.ModelSerializer):
    author_name = serializers.CharField(source="author.name")

    class Meta:
        model = Article
        fields = ["id", "title", "author_name"]
```

---

### 使用 drf-rust

```python
from drf_rust.serializers import RustModelSerializer

class ArticleSerializer(RustModelSerializer):
    author_name = serializers.CharField(source="author.name")

    class Meta:
        model = Article
        fields = ["id", "title", "author_name"]
```

就这样，完成替换。

---

## 为什么 DRF 序列化很慢？

DRF 在序列化过程中存在几个核心性能问题：

---

### 1. 按行处理（Python 循环）

```python
for obj in queryset:
    serializer.to_representation(obj)
```

对于大数据量，性能急剧下降。

---

### 2. 嵌套 serializer 导致 N+1 查询

例如：

```
Order
 └── user
      └── organization
           └── name
```

DRF 可能执行多次查询，而不是一次 JOIN。

---

### 3. Python 属性访问开销

每个字段都需要：

* 属性查找
* 类型转换
* serializer 处理

这些操作叠加后成本极高。

---

## drf-rust 如何解决？

`drf-rust` 将序列化过程迁移到 **Rust 编译代码**中执行：

执行流程:

```
Django View
     │
     ▼
RustModelSerializer
     │
     ▼
字段分类
     │
     ▼
SQL 生成
     │
     ▼
Rust 执行引擎
     │
     ▼
数据库
     │
     ▼
Python list[dict]
```

### 核心优化

* SQL JOIN 扁平化
* 零 N+1 查询
* 编译级数据处理
* I/O 期间释放 GIL

---

## 功能特性

### 支持加速的字段

* CharField
* IntegerField
* DecimalField
* BooleanField
* DateTimeField
* ...

### 支持的关系类型

* ForeignKey
* OneToOne
* ManyToMany
* 深层 JOIN 链

示例：

```python
source="author.department.company.name"
```

---

### 嵌套 Serializer

嵌套 序列化自动优化为 SQL JOIN。

---

## Python 处理字段

以下字段仍由 DRF 处理：

* `SerializerMethodField`
* `source='*'`
* callable source

这些字段会在 Rust 返回数据后补充。

---

## 自动回退机制

如果 Rust 出现任何异常：

```text
drf-rust → 自动回退到 DRF
```

你的 API 不会中断。

---

## 安装

### 源码安装

```bash
git clone https://github.com/john-tanner-dev/drf-rust
cd drf-rust

pip install maturin
maturin develop --release
```

---

## 环境要求

* Python ≥ 3.8
* Django ≥ 3.0
* djangorestframework ≥ 3.12
* Rust toolchain
* maturin ≥ 1.0

---

## 支持数据库

| 数据库        | 支持 |
| ---------- | -- |
| PostgreSQL | ✓  |
| MySQL      | ✓  |
| SQLite     | ✓  |

---

## Roadmap

计划中的功能：

* ManyToMany 优化
* 异步数据库支持
* 查询优化器（query planner）
* serializer 缓存
* 性能提升到 **50× DRF**

---

## License

MIT

---



