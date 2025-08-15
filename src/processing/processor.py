import abc

class Processor(abc.ABC):
    @abc.abstractmethod
    def preprocess(self):
        pass
    @abc.abstractmethod
    def transform(self):
        pass
