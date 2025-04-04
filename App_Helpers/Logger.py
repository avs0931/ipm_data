from typing import Any


def dbg_print(dbg_string: Any, end: str = "\n"):
    print(f"DEBUG: {dbg_string}", end=end)


def wrn_print(wrn_string: Any):
    print(f"WARNING: {wrn_string}")


def err_print(err_string: Any, ex: Exception = None):
    if ex:
        print(f"ERROR: {err_string} / Exception of '{type(ex)}' raised '{str(ex)}'")
    else:
        print(f"ERROR: {err_string}")
