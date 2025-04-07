import psycopg2
import os
from Config import config as app_cfg
from App_Helpers.Logger import *


def _connect(db_name: str = app_cfg.DB_Default,
             db_user: str = app_cfg.DB_User,
             db_pass: str = app_cfg.DB_Password,
             db_host: str = app_cfg.DB_Server,
             db_port: str = app_cfg.DB_port
             ):
    return psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
# end of get_connect()


def touch_db(db_name: str) -> bool:
    assert db_name, "DB Name not provided"
    try:
        with _connect(db_name=db_name) as conn:
            with conn.cursor() as cur:
                trc_print(f"DB Connection success: '{db_name}'")
                cur.close()
                conn.close()
                return True
    except Exception as e:
        msg = f"Connection to database '{db_name}' failed."
        err_print(msg, ex=e)
    return False
# end of touch_db()


def _test_connection(db_name: str):
    assert db_name, "No DB name"
    dbg_print(f"Running at host: '{app_cfg.DB_Server}'")
    dbg_print(execute_one(db_name=db_name, sql_stmt="SELECT version();"))
# end of test_connection()


def execute(db_name: str, sql_stmt: str) -> None:
    assert db_name, "No DB name"
    assert sql_stmt, "No SQL Statement"
    with _connect(db_name=db_name) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_stmt)
                conn.commit()
            except Exception as e:
                conn.rollback()
                err_print("Can't execute sql statement", ex=e)
# end of execute_one()


def execute_one(db_name: str, sql_stmt: str) -> tuple or None:
    assert db_name, "No DB name"
    assert sql_stmt, "No SQL Statement"
    with _connect(db_name=db_name) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_stmt)
                res = cur.fetchone()
                conn.commit()
            except Exception as e:
                conn.rollback()
                err_print("Can't execute sql statement", ex=e)
    return res
# end of execute_one()


def execute_all(db_name: str, sql_stmt: str) -> [tuple]:
    assert db_name, "No DB name"
    assert sql_stmt, "No SQL Statement"
    with _connect(db_name=db_name) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_stmt)
                res = cur.fetchall()
                conn.commit()
            except Exception as e:
                conn.rollback()
                err_print("Can't execute sql statement", ex=e)
    return res
# end of execute_one()


def execute_f(db_name: str, sql_file: str):
    assert db_name, "No DB name"
    assert sql_file, "No SQL file"
    assert os.path.exists(sql_file), f"SQL file not exists '{sql_file}'"
    with open(sql_file, mode="r", encoding="utf-8") as sf:
        sql_stmt = "".join(sf.readlines())
    return execute(db_name=db_name, sql_stmt=sql_stmt)
# end of execute_f()


def execute_f_one(db_name: str, sql_file: str):
    assert db_name, "No DB name"
    assert sql_file, "No SQL file"
    assert os.path.exists(sql_file), f"SQL file not exists '{sql_file}'"
    sql_stmt = ""
    with open(sql_file, mode="r", encoding="utf-8") as sf:
        sql_stmt = "".join(sf.readlines())
    return execute_one(db_name=db_name, sql_stmt=sql_stmt)
# end of execute_f_one()


def execute_f_all(db_name: str, sql_file: str):
    assert db_name, "No DB name"
    assert sql_file, "No SQL file"
    assert os.path.exists(sql_file), f"SQL file not exists '{sql_file}'"
    with open(sql_file, mode="r", encoding="utf-8") as sf:
        sql_stmt = "".join(sf.readlines())
    return execute_all(db_name=db_name, sql_stmt=sql_stmt)
# end of execute_f_all()


# ##############################
# Local test site
if __name__ == '__main__':
    _test_connection(db_name="test")
