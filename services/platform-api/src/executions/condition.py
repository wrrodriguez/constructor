# src/executions/condition.py
import operator as op
import jmespath

_OPERATORS: dict[str, object] = {
    ">":       op.gt,
    ">=":      op.ge,
    "<":       op.lt,
    "<=":      op.le,
    "==":      op.eq,
    "!=":      op.ne,
    "in":      lambda a, b: a in b,
    "not_in":  lambda a, b: a not in b,
}


def evaluate_condition(config: dict, context: dict) -> str:
    """
    config: {
        "expression": "<jmespath>",
        "operator": ">",
        "value": <any>,
        "branches": {"true": "<step_id>", "false": "<step_id>"}
    }
    Returns the next step_id based on the evaluated condition.
    """
    extracted = jmespath.search(config["expression"], context)
    compare_fn = _OPERATORS[config["operator"]]
    result = bool(compare_fn(extracted, config["value"]))
    key = "true" if result else "false"
    return config["branches"][key]
