from typing import Optional

from airflow.utils.task_group import TaskGroup


class LazyTaskGroup(TaskGroup):
    """
    Lazy execution of TaskGroup that only inits when something is added to it.
    """
    task_group: Optional[TaskGroup] = None

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @property
    def __class__(self):
        if self.task_group is None:
            super().__init__(*self.args, **self.kwargs)
        return self.task_group
