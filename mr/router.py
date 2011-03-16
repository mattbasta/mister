from constants import MAP, FILTER, REDUCE

class Router(object):
    "A baseline object that routes mister data within a single thread"

    def __init__(self):
        self.data = {}
        self.hooks = {}
        self.reset_hooks()

    def reset_hooks(self):
        self.hooks = {MAP:{},
                      FILTER:{},
                      REDUCE:{}}

    def feed(self, pattern, datum):
        "Adds a new item to the data set"

        if pattern in self.hooks[FILTER]:
            if any(filter_(datum) for
                   filter_ in
                   self.hooks[FILTER][pattern]):
                return

        if pattern not in self.data:
            self.data[pattern] = []
        self.data[pattern].append(datum)

    def _gather_data(self, pattern):
        "Returns a list of data to be processed"

        data = self.data[pattern]
        del self.data[pattern]
        return data

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

                data = self._gather_data(pattern)

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
                    yield pattern, self._gather_data(pattern)

