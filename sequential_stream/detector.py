from itertools import takewhile, dropwhile
from collections import defaultdict

class Detector(object):
    def __init__(self, cmds, attrName = 'stream', tDelta = 1000):
        self.__attrName = attrName
        self.__tDelta = tDelta
        self.__cmds = cmds
        self.__nextStreamID = 0

        self.__lbaXCmds = defaultdict(list)
        self.__window = []
        pass

    def assignStream(self, cmd):
        self.__window.append(cmd)
        if hasattr(cmd.start(), 'sectorCount'):
            lba = cmd.start().lba
            if lba in self.__lbaXCmds:
                prev = self.__lbaXCmds[lba][0]
                if not hasattr(prev, self.__attrName):
                    setattr(prev, self.__attrName, self.__nextStreamID)
                    self.__nextStreamID += 1
                setattr(cmd, self.__attrName, getattr(prev, self.__attrName))
                self.removeLBAXCmd(prev)
            nLBA = lba + cmd.start().sectorCount
            self.__lbaXCmds[nLBA].append(cmd)

    def removeLBAXCmd(self, c):
        if hasattr(c.start(), 'sectorCount'):
            nLBA = c.start().lba + c.start().sectorCount
            l = self.__lbaXCmds[nLBA]
            if c in l:
                l.remove(c)
            if len(l) == 0:
                del self.__lbaXCmds[nLBA]

    def retire(self, cmd):
        def isExpired(x):
            return (cmd.sTime() - x.eTime() > self.__tDelta * 1000)
        for c in takewhile(isExpired, self.__window):
            self.removeLBAXCmd(c)
            yield c
        self.__window[:] = list(dropwhile(isExpired, self.__window))

    def __iter__(self):
        for cmd in self.__cmds:
            for r in self.retire(cmd):
                yield r
            self.assignStream(cmd)
        for c in self.__window:
                yield c
