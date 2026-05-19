# src/executions/transform.py
import copy
import jmespath


def apply_transform(config: dict, context: dict) -> dict:
    """
    config: {"<dest_dot_path>": "<src_jmespath>", ...}
    Copies context and writes extracted values to destination paths.
    """
    result = copy.deepcopy(context)
    for dest_path, src_expr in config.items():
        value = jmespath.search(src_expr, context)
        _set_nested(result, dest_path, value)
    return result


def _set_nested(data: dict, path: str, value) -> None:
    keys = path.split(".")
    current = data
    for key in keys[:-1]:
        current = current.setdefault(key, {})
    current[keys[-1]] = value
