import functools as ft


def decorator(func=None, foo='spam'):
    if func is None:
        return ft.partial(decorator, foo=foo)

    @ft.wraps(func)
    def wrapper(*args, **kwargs):
        # do something with `func` and `foo`, if you're so inclined
        pass

    return wrapper
