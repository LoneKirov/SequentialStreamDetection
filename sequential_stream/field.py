from pytrace.command_statistics import Field

class Stream(Field):
    def __init__(self, name='stream'):
        super(Stream, self).__init__(name)
    def __call__(self, prev, cur, next):
        return getattr(cur, self.name(), -1)
