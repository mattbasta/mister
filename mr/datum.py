import time


class Wrapper(object):
    """A wrapper which enables easy distribution of data."""

    def __init__(self, job, pattern, id_, value, timestamp=None, origin=None,
                 reduced=False):
        """origin should be in the following format:

        {
            "address": "127.0.0.1:7422",
            "remote_job_id": "...",
        }
        """
        self.job_id = job
        self.pattern = pattern
        self.id_ = id_
        self.value = value

        # Assign a timestamp if one isn't already available.
        if timestamp is None:
            timestamp = time.time()
        self.timestamp = timestamp

        self.origin = origin
        self.reduced = reduced

    def __eq__(self, other):
        if isinstance(other, Wrapper):
            return other.value == self.value
        else:
            return False

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.__str__()

