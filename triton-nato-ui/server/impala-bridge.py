#!/usr/bin/env python3
"""Run a single read-only SQL statement against Impala; JSON result on stdout."""
import json
import os
import sys


def connect():
    from impala.dbapi import connect

    host = os.environ.get("IMPALA_HOST", "")
    port = int(os.environ.get("IMPALA_PORT", "443"))
    database = os.environ.get("IMPALA_DATABASE", "defense")
    user = os.environ.get("IMPALA_USER", "")
    password = os.environ.get("IMPALA_PASSWORD", "")
    use_ssl = os.environ.get("IMPALA_USE_SSL", "true").lower() in ("1", "true", "yes")
    http_path = os.environ.get("IMPALA_HTTP_PATH", "cliservice")
    auth_mech = os.environ.get("IMPALA_AUTH_MECH", "LDAP").upper()

    if not host:
        raise RuntimeError("IMPALA_HOST is not set")

    kwargs = {
        "host": host,
        "port": port,
        "database": database,
        "use_ssl": use_ssl,
        "auth_mechanism": auth_mech,
    }
    if user:
        kwargs["user"] = user
    if password:
        kwargs["password"] = password

    # CDW HTTPS Impala (transportMode=http)
    kwargs["use_http_transport"] = True
    kwargs["http_path"] = http_path

    return connect(**kwargs)


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "Missing SQL argument"}))
        sys.exit(1)
    sql = sys.argv[1]
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(10000)
        conn.close()
        data = [dict(zip(columns, row)) for row in rows]
        print(json.dumps({"ok": True, "columns": columns, "rows": data, "source": "impala"}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "source": "impala"}))
        sys.exit(2)


if __name__ == "__main__":
    main()
