# mcp-trino.py — PoC MCP server for Trino (HTTP :8086)
# Deps: pip install mcp trino

import logging
import os
import sys
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP
from trino.dbapi import connect

# STDERR logging (never print to STDOUT in MCP servers)
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
log = logging.getLogger("mcp-trino-poc")

mcp = FastMCP("trino-poc")

# Defaults for your PoC
_DEFAULTS = {
    "HOST": "172.22.0.146",
    "PORT": 8086,          # your port
    "USER": "trino",
    "CATALOG": "iceberg",
    "SCHEMA": "gold",
    "SCHEME": "http",      # HTTP
}

def _get_env(name: str, fallback: Any) -> Any:
    val = os.getenv(f"TRINO_{name}")
    if val is None:
        return fallback
    if name == "PORT":
        return int(val)
    return val

def _get_trino_conn(
    *, catalog_override: Optional[str] = None, schema_override: Optional[str] = None
):
    host = _get_env("HOST", _DEFAULTS["HOST"])
    port = _get_env("PORT", _DEFAULTS["PORT"])
    user = _get_env("USER", _DEFAULTS["USER"])
    catalog = catalog_override or _get_env("CATALOG", _DEFAULTS["CATALOG"])
    schema = schema_override or _get_env("SCHEMA", _DEFAULTS["SCHEMA"])
    http_scheme = _get_env("SCHEME", _DEFAULTS["SCHEME"])  # "http"

    # Simple PoC: HTTP, no auth, no TLS verify parameters
    conn = connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme=http_scheme,
        source="mcp-trino-poc",
    )
    return conn

def _ensure_readonly(sql: str) -> None:
    s = sql.lstrip().lower()
    if not s.startswith(("select", "with", "show", "describe", "explain")):
        raise ValueError("Only read-only queries are allowed (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN).")

@mcp.tool(description="Ping Trino with SELECT 1.")
def trino_ping() -> dict:
    try:
        conn = _get_trino_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return {"ok": True, "result": cur.fetchall()}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try: conn.close()
        except Exception: pass

@mcp.tool(description="List tables in the target schema (defaults to iceberg.gold).")
def trino_tables(
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> dict:
    cat = catalog or _get_env("CATALOG", _DEFAULTS["CATALOG"])
    sch = schema or _get_env("SCHEMA", _DEFAULTS["SCHEMA"])
    sql = f"SHOW TABLES FROM {cat}.{sch}"
    _ensure_readonly(sql)

    try:
        conn = _get_trino_conn(catalog_override=cat, schema_override=sch)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall() or []
        tables = [f"{cat}.{sch}.{r[0]}" for r in rows]
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        return {"error": str(e), "catalog": cat, "schema": sch}
    finally:
        try: conn.close()
        except Exception: pass

@mcp.tool(description="Describe a table's columns and types.")
def trino_describe(table: str, catalog: str | None = None, schema: str | None = None) -> dict:
    cat = catalog or _get_env("CATALOG", _DEFAULTS["CATALOG"])
    sch = schema  or _get_env("SCHEMA",  _DEFAULTS["SCHEMA"])
    _ensure_readonly(f"describe {cat}.{sch}.{table}")
    try:
        conn = _get_trino_conn(catalog_override=cat, schema_override=sch)
        cur = conn.cursor()
        cur.execute(f"DESCRIBE {cat}.{sch}.{table}")
        rows = cur.fetchall() or []
        cols = [{"column": r[0], "type": r[1], "extra": r[2] if len(r)>2 else None} for r in rows]
        return {"catalog": cat, "schema": sch, "table": table, "columns": cols}
    except Exception as e:
        return {"error": str(e), "catalog": cat, "schema": sch, "table": table}
    finally:
        try: conn.close()
        except: pass

import re

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _id_ok(name: str) -> bool:
    return bool(_NAME_RE.match(name))

def _q(v):
    # very small PoC quoter: numbers pass through, strings get single-quoted
    return v if isinstance(v, (int, float)) else "'" + str(v).replace("'", "''") + "'"


@mcp.tool(description="Run a read-only SQL query (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN). Optionally enforce a LIMIT and a timeout.")
def trino_query(
    sql: str,
    max_rows: int = 1000,
    default_limit: int = 1000,
    timeout_s: int | None = 30,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> dict:
    _ensure_readonly(sql)

    # If user forgot a LIMIT in a large SELECT, append one
    s = sql.strip().rstrip(";")
    if s.lower().startswith(("select", "with")) and " limit " not in s.lower():
        s = f"{s} LIMIT {int(default_limit)}"
    sql = s

    try:
        conn = _get_trino_conn(catalog_override=catalog, schema_override=schema)
        cur = conn.cursor()

        # Best-effort safety: trim execution time if requested
        if timeout_s:
            cur.execute(f"SET SESSION query_max_execution_time = '{int(timeout_s)}s'")

        cur.execute(sql)
        rows = cur.fetchmany(max_rows + 1)
        columns = [d[0] for d in (cur.description or [])]
        truncated = len(rows) > max_rows
        rows = rows[:max_rows]
        data = [dict(zip(columns, r)) for r in rows]
        return {
            "sql": sql,
            "columns": columns,
            "rows": data,
            "rowcount": len(data),
            "truncated": truncated,
            "catalog": catalog or _get_env("CATALOG", _DEFAULTS["CATALOG"]),
            "schema": schema or _get_env("SCHEMA", _DEFAULTS["SCHEMA"]),
        }
    except Exception as e:
        return {"error": str(e), "sql": sql}
    finally:
        try: conn.close()
        except Exception: pass

@mcp.tool(description="List catalogs available in this Trino.")
def trino_catalogs() -> dict:
    try:
        conn = _get_trino_conn()
        cur = conn.cursor()
        cur.execute("SHOW CATALOGS")
        cats = [r[0] for r in cur.fetchall() or []]
        return {"catalogs": cats}
    except Exception as e:
        return {"error": str(e)}
    finally:
        try: conn.close()
        except: pass


@mcp.tool(description="List schemas for a given catalog (defaults to env).")
def trino_schemas(catalog: Optional[str] = None) -> dict:
    cat = catalog or _get_env("CATALOG", _DEFAULTS["CATALOG"])
    try:
        conn = _get_trino_conn(catalog_override=cat)
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS FROM {cat}")
        schemas = [r[0] for r in cur.fetchall() or []]
        return {"catalog": cat, "schemas": schemas}
    except Exception as e:
        return {"error": str(e), "catalog": cat}
    finally:
        try: conn.close()
        except: pass


@mcp.tool(description="List tables from information_schema (stable API), optionally filtered by table_name like pattern.")
def trino_list_tables(
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    like: Optional[str] = None,
) -> dict:
    cat = catalog or _get_env("CATALOG", _DEFAULTS["CATALOG"])
    sch = schema  or _get_env("SCHEMA",  _DEFAULTS["SCHEMA"])
    where = "WHERE table_schema = ?" + (" AND table_name LIKE ?" if like else "")
    params = [sch] + ([like] if like else [])
    sql = f"""
        SELECT table_name
        FROM {cat}.information_schema.tables
        {where}
        ORDER BY table_name
    """
    try:
        conn = _get_trino_conn(catalog_override=cat, schema_override=sch)
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        return {"catalog": cat, "schema": sch, "tables": [r[0] for r in rows]}
    except Exception as e:
        return {"error": str(e), "catalog": cat, "schema": sch}
    finally:
        try: conn.close()
        except: pass


@mcp.tool(description="Get column names and types via information_schema for a table in gold.")
def trino_columns(table: str, catalog: Optional[str] = None, schema: Optional[str] = None) -> dict:
    cat = catalog or _get_env("CATALOG", _DEFAULTS["CATALOG"])
    sch = schema  or _get_env("SCHEMA",  _DEFAULTS["SCHEMA"])
    sql = f"""
        SELECT column_name, data_type, ordinal_position
        FROM {cat}.information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
    """
    try:
        conn = _get_trino_conn(catalog_override=cat, schema_override=sch)
        cur = conn.cursor()
        cur.execute(sql, [sch, table])
        rows = cur.fetchall() or []
        cols = [{"column": r[0], "type": r[1], "position": r[2]} for r in rows]
        return {"catalog": cat, "schema": sch, "table": table, "columns": cols}
    except Exception as e:
        return {"error": str(e), "catalog": cat, "schema": sch, "table": table}
    finally:
        try: conn.close()
        except: pass

if __name__ == "__main__":
    mcp.run(transport="stdio")
