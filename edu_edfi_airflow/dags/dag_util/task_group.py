from airflow.utils.task_group import TaskGroup


class LazyTaskGroup(TaskGroup):
    """
    Lazy execution of TaskGroup that only inits when something is added to it.
    """
    __exists = False

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def initialize(self):
        if not self.__exists:
            super().__init__(*self.args, **self.kwargs)
