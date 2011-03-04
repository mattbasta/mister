import tempfile
import cPickle
from router import Router
from constants import MAP, FILTER, REDUCE

class DiskRouter(Router):
    "A baseline object that routes mister data within a single thread"

    def __init__(self, spool_size=4096):
        Router.__init__(self)
        self.spool_size = spool_size

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
                    if pattern in self.hooks[REDUCE]:
                        gen = self.hooks[REDUCE][pattern](data)
                        if gen is None:
                            continue
                        for pattern, datum in gen:
                            self.feed(pattern, datum)
                    elif pattern in self.hooks[MAP]:
                        for datum in data:
                            for pattern, new_datum in self.hooks[MAP][pattern](datum):
                                self.feed(pattern, new_datum)
                    else:
                        yield pattern, data

