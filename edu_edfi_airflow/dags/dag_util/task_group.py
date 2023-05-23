from airflow.utils.task_group import TaskGroup


class LazyTaskGroup(TaskGroup):
    """
    Lazy execution of TaskGroup that only inits when something is added to it.
    """
    __exists = False

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __bool__(self) -> bool:
        return self.__exists

    def initialize(self):
        """
        Method to initialize the TaskGroup after init.
        :return:
        """
        if not self.__exists:
            super().__init__(*self.args, **self.kwargs)
            self.__exists = True
