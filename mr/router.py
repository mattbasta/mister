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
        if pattern not in self.data:
            self.data[pattern] = []
        self.data[pattern].append(datum)

    def process(self):
        "Processes the data and yields the result"
        
        while self.data:
            for pattern, orig_data in self.data.items():
                data = []
                if pattern in self.hooks["filter"]:
                    filters = self.hooks["filter"][pattern]
                    for datum in orig_data:
                        if any(filter_(datum) for
                               filter_ in
                               filters):
                            continue
                        data.append(datum)
                else:
                    data = orig_data[:]

                # The original data is now useless
                del self.data[pattern]

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

