from functools import wraps
from inspect import signature
from threading import RLock


_scene_write_lock = RLock()


def serialize_scene_writes(handler):
    """Keep scene writes and their notifications in the same order across clients."""
    resource_index = list(signature(handler).parameters).index("resource")

    @wraps(handler)
    def dispatch(*args, **kwargs):
        resource = kwargs["resource"] if "resource" in kwargs else args[resource_index]
        if resource in ("scene", "scenes", "smart_scene"):
            with _scene_write_lock:
                return handler(*args, **kwargs)
        return handler(*args, **kwargs)

    return dispatch
