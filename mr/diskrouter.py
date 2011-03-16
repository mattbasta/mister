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

        # Add the pattern if it doesn't exist
        if pattern not in self.data:
            self.data[pattern] = \
                tempfile.SpooledTemporaryFile(max_size=self.spool_size)

        # Pickle the datum and save it to the file buffer
        cPickle.dump(datum,
                     self.data[pattern],
                     protocol=cPickle.HIGHEST_PROTOCOL)

    def _gather_data(self, pattern):
        "Yields iterables of processable data"

        # We want to be the only ones with a reference ot the current data
        spool = self.data[pattern]
        del self.data[pattern]

        spool.seek(0)

        try:
            # Unpickle until we get an end-of-file exception
            while True:
                yield cPickle.load(spool)
        except EOFError:
            pass

        # Close the temporary file when we're done
        spool.close()

    def process(self):
        "Processes the data and yields the result"

        while self.data:

            skip_reduction = False

            # Apply processing to mappers
            for pattern in filter(lambda p: p in self.hooks[MAP] and
                                            p not in self.hooks[REDUCE],
                                  self.data.keys()):

                data = self._gather_data(pattern)

                # Mappers must be fed individual datums
                mapper = self.hooks[MAP][pattern]
                for datum in data:
                    for new_pattern, new_datum in mapper(datum):
                        skip_reduction = True
                        self.feed(new_pattern, new_datum)

            # Skip the reduction if we got new datums
            if skip_reduction:
                continue

            # Flag to determine whether everything left is an orphan
            all_orphans = True

            # Apply processing for reducers and yieldables
            for pattern in self.data.keys():

                data = list(self._gather_data(pattern))

                # If the pattern has no reducer, then yield it
                if pattern not in self.hooks[REDUCE]:
                    yield pattern, data
                    continue

                if len(data) > 1:
                    gen = self.hooks[REDUCE][pattern](data)
                    # Skip reducers that cancel out
                    if gen is None:
                        continue
                    for new_pattern, datum in gen:
                        all_orphans = False # We have more to process!
                        self.feed(new_pattern, datum)
                else:
                    self.feed(pattern, data[0])

            if all_orphans:
                for pattern in self.data.keys():
                    yield pattern, list(self._gather_data(pattern))

