import cPickle
import tempfile
import uuid
from router import Router


class DiskRouter(Router):
    "A baseline object that routes mister data within a single thread"

    def __init__(self, spool_size=4096):
        super(DiskRouter, self).__init__()
        self.spool_size = spool_size

    def _save_fed_data(self, pattern, item, reduced):
        """Save an item to a disk buffer."""

        repo = self.data if not reduced else self.reduced_data
        # Add the pattern if it doesn't exist
        if pattern not in repo:
            repo[pattern] = \
                tempfile.SpooledTemporaryFile(max_size=self.spool_size)

        # Pickle the datum and save it to the file buffer
        cPickle.dump(item,
                     repo[pattern],
                     protocol=cPickle.HIGHEST_PROTOCOL)

    def _gather_data(self, pattern, reduced=False):
        """Return the data found in a disk buffer."""

        repo = self.data if not reduced else self.reduced_data

        # We want to be the only ones with a reference to the current data,
        # so delete the reference to the buffer immediately after reading.
        spool = repo[pattern]
        del repo[pattern]

        def gen():
            spool.seek(0)

            try:
                # Unpickle until we get an end-of-file exception.
                while True:
                    yield cPickle.load(spool)
            except EOFError:
                pass

            # Close the temporary file when we're done
            spool.close()

        return gen()

