from airflow.utils.task_group import TaskGroup


class LazyTaskGroup(TaskGroup):
    """
    Lazy execution of TaskGroup that only inits when something is added to it.
    """
    args = []
    kwargs = {}

    def __init__(self):
        super().__init__(*self.args, **self.kwargs)

    @classmethod
    def initialize(cls, *args, **kwargs):
        cls.args = args
        cls.kwargs = kwargs
        return cls
