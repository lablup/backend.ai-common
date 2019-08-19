from abc import ABC, abstractmethod


class AbstractHook(ABC):

    @abstractmethod
    async def init(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def get_handlers(self):
        raise NotImplementedError

    @abstractmethod
    def get_hook_event_types(self):
        raise NotImplementedError
