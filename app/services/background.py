class InlineBackgroundTasks:
    def __init__(self) -> None:
        self._tasks: list[tuple[callable, tuple, dict]] = []

    def add_task(self, func: callable, *args, **kwargs) -> None:
        self._tasks.append((func, args, kwargs))

    def run(self) -> None:
        for func, args, kwargs in self._tasks:
            func(*args, **kwargs)
