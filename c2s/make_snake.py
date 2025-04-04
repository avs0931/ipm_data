import re


def make_snake(src_name: str) -> str:
    # Name sanitizer: translate given name to 'snake_case' style
    return re.sub("([a-z0-9]{1,1})([A-Z]{1,1})", r"\1_\2", src_name).lower()
