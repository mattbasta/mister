import tempfile
import cPickle
import time
import os
import sys
from router import Router
from constants import MAP, FILTER, REDUCE

class ForkingRouter(Router):
    "A baseline object that routes mister data within a single thread"

    def __init__(self, max_forks=0):
        "max_forks: The maximum number of forks (default: 0 - unlimited"

        Router.__init__(self)
        self.max_forks = max_forks
        self.fork_count = 0
        self.forks = []

    def feed(self, pattern, datum):
        "Adds a new item to the data set"

        # We filter beforehand to prevent unnecessary disk writes/pickles
        if pattern in self.hooks[FILTER]:
            if any(filter_(datum) for filter_ in self.hooks[FILTER][pattern]):
                return

        if pattern not in self.data:
            self.data[pattern] = \
                tempfile.SpooledTemporaryFile(max_size=self.spool_size)
        cPickle.dump(datum,
                     self.data[pattern],
                     protocol=cPickle.HIGHEST_PROTOCOL)

    def _gather_data(self, pattern):
        "Yields iterables of processable data"

        # We want to be the only ones with a reference ot the current data
        spool = self.data[pattern]
        del self.data[pattern]

        spool.seek(0)

        data = []
        size = 0
        try:
            while True:
                datum = cPickle.load(spool)
                data.append(datum)
                if spool.tell() > self.spool_size:
                    yield data
                    data = []
        except EOFError:
            pass

        spool.close()
        if data:
            yield data

    def process(self):
        "Processes the data and yields the result"
        while self.data:
            for pattern in self.data.keys():
                data_generator = self._gather_data(pattern)
                for data in data_generator:
                    if pattern in self.hooks[MAP]:
                        for datum in data:
                            for pattern, new_datum in self.hooks[MAP][pattern](datum):
                                self.feed(pattern, new_datum)
                    elif pattern in self.hooks[REDUCE]:
                        for reducer in self.hooks[REDUCE]:
                            for pattern, datum in reducer(data):
                                self.feed(pattern, datum)
                    else:
                        yield pattern, data

class ForkTask(object):
    "Describes a task that a fork should be performing"

    def __init__(self, pattern):
        self.pattern = pattern

class Forklet(object):
    "A class to help with the division of work among forks"
    
    def __init__(self, hooks):
        self.hooks = hooks
        self.pid = 0
        self.is_child = False
        self._rpipe, self._wpipe = os.pipe()
        self._rpipe, self._wpipe = os.fdopen(self._rpipe, 'r', 0), \
                                   os.fdopen(self._wpipe, 'w', 0)
        self.pattern = ""

        self.queue = []

    def fork(self):
        self.pid = os.fork()
        self.is_child = self.pid == 0

        if not self.is_child:
            self._wpipe.close()
            return True

        self._rpipe.close()

        while True:
            incoming_object = cPickle.load(self._rpipe)
            if isinstance(incoming_object, ForkTask):
                self.pattern = incoming_object.pattern
                continue
            else:
                self.process(self.pattern, incoming_object)

            time.sleep(0.001) # Read somewhere that this is what you should do

        sys.exit(1)

    def process(self, pattern, datum):
        "Processes a datum"
        if pattern in self.hooks[MAP]:
            for new_pattern, new_datum in self.hooks[MAP][pattern](datum):
                if new_pattern == self.pattern:
                    self.queue.append(new_datum)
                else:
                    self.submit(new_pattern, new_datum)
        elif pattern in self.hooks[REDUCE]:
            
        
    def submit(self, pattern, datum):
        "Sends a completed datum back to the router"
        packet = (pattern, datum)
        cPickle.dump(packet, self._wpipe, protocol=cPickle.HIGHEST_PROTOCOL)

