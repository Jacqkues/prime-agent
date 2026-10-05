"""Project application data and live public Python functions after each cell."""


def _pa_project(namespace):
    import inspect
    import json
    import types

    catalog = {"entries": [], "total": 0, "truncated": False, "error": None}
    budget = 48 * 1024
    for name, value in sorted(list(namespace.items())):
        # Imported helpers, private names and methods are not application exports.
        if name.startswith("_") or not isinstance(value, types.FunctionType):
            continue
        if value.__globals__ is not namespace:
            continue
        catalog["total"] += 1
        if len(catalog["entries"]) >= 128 or len(name) > 256:
            catalog["truncated"] = True
            continue
        try:
            # Inspect a metadata-only copy, never call it. Omitting annotations
            # also avoids deferred annotation evaluation on newer Python versions.
            shape = types.FunctionType(value.__code__, namespace, value.__name__,
                                       tuple(Ellipsis for _ in (value.__defaults__ or ())),
                                       value.__closure__)
            shape.__kwdefaults__ = {key: Ellipsis for key in (value.__kwdefaults__ or {})}
            signature = str(inspect.signature(shape, follow_wrapped=False))
        except (TypeError, ValueError):
            signature = "(signature unavailable)"
        description = inspect.cleandoc(value.__doc__ or "")
        entry = {
            "name": name,
            "signature": signature[:512],
            "description": description[:512],
            "kind": "async_function" if inspect.iscoroutinefunction(value) else "function",
        }
        size = len(json.dumps(entry, ensure_ascii=False).encode("utf-8"))
        if size > budget:
            catalog["truncated"] = True
            continue
        budget -= size
        catalog["entries"].append(entry)
        catalog["truncated"] |= len(signature) > 512 or len(description) > 512

    projection = {"catalog": catalog, "state": None, "state_error": None}
    try:
        projection["state"] = json.loads(json.dumps(namespace["app"], default=repr))
    except Exception as error:
        # A broken app projection must not hide newly created/redefined functions.
        projection["state_error"] = f"Application state could not be projected: {type(error).__name__}"
    return projection


from rlm.repl import emit as _pa_emit
_pa_emit({"application/vnd.prime-agent.application-state+json": _pa_project(globals())})
