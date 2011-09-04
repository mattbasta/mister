import uuid

from datum import Wrapper


class Router(object):
    """A baseline object that routes mister data within a single thread."""

    def __init__(self):
        self.data = {}
        self.reduced_data = {}

        # self.<hook>s should be in the form of:
        # {"pattern": <lambda>}
        self.mappers = {}
        self.reducers = {}
        # self.filters should be in the form of:
        # {"pattern": [<lambda>, ...]}
        self.filters = {}

        self.job_id = uuid.uuid4().hex

    def reset_hooks(self):
        self.mappers = {}
        self.reducers = {}
        self.filters = {}

    def feed(self, pattern, body, id_=None, reduced=False):
        """Add a new item to the data set."""

        if id_ is None:
            id_ = uuid.uuid4().hex
        item = Wrapper(self.job_id, pattern, id_, body, reduced=reduced)

        # Immediately test whether the item is eligible for inclusion.
        if pattern in self.filters:
            if any(filter_(item) for filter_ in self.filters[pattern]):
                return

        self._save_fed_data(pattern, item, reduced)

    def _save_fed_data(self, pattern, item, reduced):
        """Save an item to the appropriate location."""
        repo = self.data if not reduced else self.reduced_data
        if pattern not in repo:
            repo[pattern] = []
        repo[pattern].append(item)

    def _gather_data(self, pattern, reduced=False):
        """Return a list of data to be processed."""

        repo = self.data if not reduced else self.reduced_data

        if pattern in self.filters:
            data = []
            f = self.filters[pattern]
            for item in repo[pattern]:
                if any(filter_(item) for filter_ in f):
                    continue
                data.append(item)
        else:
            data = repo[pattern]

        del repo[pattern]
        return data

    def process(self):
        """Process fed data and yield the results."""

        while self.data or self.reduced_data:

            # First, we're going to process all of the raw data. Reduced data
            # will get processed after all unreduced data is processed.

            for pattern in self.data.keys():
                data = self._gather_data(pattern)

                # If there's nothing left to do to a blob of data, it should
                # be yielded.
                if (pattern not in self.mappers and
                    pattern not in self.reducers):
                    for item in data:
                        yield item
                    continue

                if pattern in self.reducers:
                    output = self.reducers[pattern](data, rereduce=False)
                    if not output:
                        continue
                    new_pattern, id_, value = output
                    self.feed(new_pattern, value, id_=id_, reduced=True)

                elif pattern in self.mappers:
                    for item in data:
                        for new_pattern, id_, new_datum in \
                            self.mappers[pattern](item):
                            # TODO: send along additional metadata.
                            self.feed(new_pattern, new_datum, id_=id_)

            # Continue processing the raw data until we're left with only
            # reduced data.
            if self.data:
                continue

            # Process all of the reduced data.
            for pattern in list(self.reduced_data.keys()):
                data = self._gather_data(pattern, reduced=True)

                if pattern in self.reducers:
                    # Rereduce operations on a non-forking router can be
                    # treated as a "final" reduce operation for a pattern.
                    output = self.reducers[pattern](data, rereduce=True)
                    if not output:
                        continue

                    if pattern not in self.mappers:
                        new_pattern, id_, value = output
                        yield Wrapper(self.job_id, pattern, id_, value)
                        continue
                    else:
                        # Reassign pattern so it gets mapped.
                        pattern, id_, output = output
                        data = [output]

                if pattern in self.mappers:
                    for item in data:
                        for new_pattern, id_, value in \
                            self.mappers[pattern](item):
                            self.feed(new_pattern, value, id_=id_)
                else:
                    for item in data:
                        yield item

