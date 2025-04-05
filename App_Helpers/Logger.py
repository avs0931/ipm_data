import Config.config as Cfg
from typing import Any


def trc_print(trace_string: Any, end: str = "\n") -> None:
    """
    Output TRACE message.
    :param trace_string: message
    :param end: end of line
    """
    if Cfg.APP_Verbose_Mode:
        print(f"TRACE: {trace_string}", end=end)


def dbg_print(dbg_string: Any, end: str = "\n"):
    if Cfg.APP_Debug_Mode:
        print(f"DEBUG: {dbg_string}", end=end)


def wrn_print(wrn_string: Any):
    print(f"WARNING: {wrn_string}")


def err_print(err_string: Any, ex: Exception = None):
    if ex:
        print(f"ERROR: {err_string} / Exception of '{type(ex)}' raised '{str(ex)}'")
    else:
        print(f"ERROR: {err_string}")
