# drf-rust

**37× Faster Serialization for Django REST Framework**

**Drop-in Rust acceleration for Django REST Framework's `ModelSerializer` serialization process.**

Just replace:

```python
ModelSerializer → RustModelSerializer
```

No other changes required.

Works with:

* serializers
* views
* pagination
* filtering
* URL configs

**Everything continues to work exactly the same.**

---

## Benchmark

Tested on a production PostgreSQL database with a **3-level nested serializer**.

|   Rows |     DRF | drf-rust |   Speedup |
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

**Key takeaway**

* Up to **37x faster**

---

## Example

### Before

```python
from rest_framework import serializers

class ArticleSerializer(serializers.ModelSerializer):
    author_name = serializers.CharField(source="author.name")

    class Meta:
        model = Article
        fields = ["id", "title", "author_name"]
```

### After

```python
from drf_rust.serializers import RustModelSerializer

class ArticleSerializer(RustModelSerializer):
    author_name = serializers.CharField(source="author.name")

    class Meta:
        model = Article
        fields = ["id", "title", "author_name"]
```

That's it.

---

## Why DRF Serialization Is Slow

DRF serialization suffers from several performance issues:

### 1. Python per-row processing

DRF serializes each object individually.

```python
for obj in queryset:
    serializer.to_representation(obj)
```

For large querysets this becomes extremely expensive.

---

### 2. Nested serializer N+1 queries

Nested serializers often trigger extra database queries.

Example:

```
Order
 └── user
      └── organization
           └── name
```

DRF may execute many queries instead of one JOIN.

---

### 3. Python attribute resolution overhead

Every field requires:

* attribute lookup
* field conversion
* serializer processing

This overhead accumulates quickly.

---

## How drf-rust Works

`drf-rust` moves serialization into **compiled Rust code**.

Pipeline:

```
Django View
     │
     ▼
RustModelSerializer
     │
     ▼
Field classification
     │
     ▼
SQL generation
     │
     ▼
Rust execution engine
     │
     ▼
Database
     │
     ▼
Python list[dict]
```

Key improvements:

* SQL JOIN flattening
* zero N+1 queries
* compiled row processing
* GIL released during I/O

---

## Features

### Accelerated fields

* CharField
* IntegerField
* DecimalField
* BooleanField
* DateTimeField
* ...

### Supported relationships

* ForeignKey
* OneToOne
* ManyToMany
* deep join chains

Example:

```
source="author.department.company.name"
```

### Nested serializers

Nested serializers are automatically optimized into SQL JOINs.

---

## Python-only fields

Some fields are handled by DRF normally:

* `SerializerMethodField`
* `source='*'`
* callable sources

These are filled **after Rust returns results**.

---

## Automatic Fallback

If Rust fails for any reason:

```
drf-rust → fallback to native DRF
```

Your API will still work.

---

## Installation

### Install from pypi.org

```bash
pip install drf-rust
```

### Install from source

```bash
git clone https://github.com/john-tanner-dev/drf-rust
cd drf-rust

pip install maturin
maturin develop --release
```

---

## Requirements

* Python ≥ 3.8
* Django ≥ 3.0
* djangorestframework ≥ 3.12
* Rust toolchain
* maturin ≥ 1.0

---

## Supported Databases

| Database   | Status |
| ---------- | ------ |
| PostgreSQL | ✓      |
| MySQL      | ✓      |
| SQLite     | ✓      |

---

## Roadmap

Planned improvements:

* ManyToMany optimization
* async database execution
* query planner
* serializer caching
* Running speed increased to 50X of DRF
---

## License

MIT

---



