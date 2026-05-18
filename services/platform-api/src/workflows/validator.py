# src/workflows/validator.py

VALID_STEP_TYPES = {"agent", "condition", "transform", "notify", "wait", "human_approval"}
VALID_ON_ERROR = {"stop", "skip", "retry", "fallback"}


def validate_steps(steps: list[dict]) -> list[str]:
    """Returns list of validation errors. Empty list = valid."""
    errors = []
    if not steps:
        errors.append("Process must have at least one step")
        return errors

    seen_ids: set[str] = set()
    for i, step in enumerate(steps):
        if step.get("type") not in VALID_STEP_TYPES:
            errors.append(
                f"Step {i}: invalid type '{step.get('type')}'. Must be one of {sorted(VALID_STEP_TYPES)}"
            )
        step_id = step.get("id")
        if not step_id:
            errors.append(f"Step {i}: 'id' is required")
        elif step_id in seen_ids:
            errors.append(f"Step {i}: duplicate id '{step_id}'")
        else:
            seen_ids.add(step_id)
        if step.get("timeout_seconds", 60) <= 0:
            errors.append(f"Step {i}: timeout_seconds must be positive")

    # Validate 'next' references point to declared step IDs
    if not errors:
        for i, step in enumerate(steps):
            next_val = step.get("next")
            if isinstance(next_val, str) and next_val not in seen_ids:
                errors.append(f"Step {i}: 'next' references unknown step id '{next_val}'")

    return errors
