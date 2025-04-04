import psycopg2
import os
from Config import config as app_cfg
from App_Helpers.Logger import dbg_print


def _get_connect():
    return psycopg2.connect(
        dbname=app_cfg.DB_default_base,
        user=app_cfg.DB_User,
        password=app_cfg.DB_Password,
        host=app_cfg.DB_Server,
        port=app_cfg.DB_port
    )
# end of get_connect()


def _test_connection():
    dbg_print(f"Running at host: '{app_cfg.DB_Server}'")
    dbg_print(execute_one(sql_stmt="SELECT version();"))
# end of test_connection()


def execute(sql_stmt: str) -> None:
    assert sql_stmt, "No SQL Statement"
    with _get_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_stmt)
# end of execute_one()


def execute_one(sql_stmt: str) -> tuple or None:
    assert sql_stmt, "No SQL Statement"
    with _get_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_stmt)
            res = cur.fetchone()
    return res
# end of execute_one()


def execute_all(sql_stmt: str) -> [tuple]:
    assert sql_stmt, "No SQL Statement"
    with _get_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_stmt)
            res = cur.fetchall()
    return res
# end of execute_one()


def execute_f(sql_file: str):
    assert sql_file, "No SQL file"
    assert os.path.exists(sql_file), f"SQL file not exists '{sql_file}'"
    with open(sql_file, mode="r", encoding="utf-8") as sf:
        sql_stmt = "".join(sf.readlines())
    return execute(sql_stmt)
# end of execute_f()


def execute_f_one(sql_file: str):
    assert sql_file, "No SQL file"
    assert os.path.exists(sql_file), f"SQL file not exists '{sql_file}'"
    with open(sql_file, mode="r", encoding="utf-8") as sf:
        sql_stmt = "".join(sf.readlines())
    return execute_one(sql_stmt)
# end of execute_f_one()


def execute_f_all(sql_file: str):
    assert sql_file, "No SQL file"
    assert os.path.exists(sql_file), f"SQL file not exists '{sql_file}'"
    with open(sql_file, mode="r", encoding="utf-8") as sf:
        sql_stmt = "".join(sf.readlines())
    return execute_all(sql_stmt)
# end of execute_f_all()


# ##############################
# Local test site
if __name__ == '__main__':
    _test_connection()
