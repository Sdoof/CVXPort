
import abc


class Schedule(abc.ABC):
    @abc.abstractmethod
    def __call__(self, time):
        pass


class DefaultSchedule(Schedule):
    """
    Always true
    """
    def __call__(self, time):
        return True
