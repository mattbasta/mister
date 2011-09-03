import tempfile
import cPickle
import fcntl
import signal
import os, sys, time
from router import Router

PERIOD = 0.05

class ForkingRouter(Router):
    "A baseline object that routes mister data within a single thread"

    def __init__(self, max_forks=0, prespawn=False, homogeneous=True):
        """
        max_forks: The maximum number of forks (default: 0 - unlimited)
        prespawn: Spawns and feeds forks before processing begins.
        homogeneous: Attempt to keep forks limited to one pattern
        """

        Router.__init__(self)

        self.max_forks = max_forks
        self.prespawn = prespawn
        self.homogeneous = homogeneous

        self.pattern_assignments = {}
        self.forks = []
        self.data = {}

        self.processing = False

    def feed(self, pattern, datum):
        "Adds a new item to the data set"

        if not self.processing:
            if not self.prespawn:
                self._queue(pattern, datum)
                return
        else:
            # We filter beforehand to prevent unnecessary disk writes/pickles
            if pattern in self.hooks[FILTER]:
                if any(filter_(datum) for
                       filter_ in
                       self.hooks[FILTER][pattern]):
                    return

        f = self._get_pattern_forklet(pattern)
        f.feed(pattern, datum)

    def _queue(self, pattern, datum):
        "Push the pattern and datum into the queue."
        if pattern not in self.data:
            self.data[pattern] = []
        self.data[pattern].append(datum)

    def _get_pattern_forklet(self, pattern):
        "Returns a forklet object for a pattern"

        if self.homogeneous and pattern in self.pattern_assignments:
            # If it's homogenous and an assignment already exists,
            # keep rolling with that pattern
            return self.pattern_assignments[pattern]

        if self.max_forks == 0 or len(self.forks) < self.max_forks:
            # If we can have unlimited forks or we haven't hit our
            # maximum forks, create a new fork
            f = Forklet(router=self,
                        pattern=pattern,
                        prespawn=self.prespawn)
            if not self.prespawn and self.processing:
                f.fork()
            self.forks.append(f)
        else:
            # We've hit our fork limit

            if self.homogeneous:
                # If we're supposed to be homogeneous, we should pick the fork
                # with the fewest assigned patterns.

                # We'll assume that forks exist
                f = reduce(lambda x,y: x if
                                       x[0] <= y[0] else
                                       y,
                           map(lambda x: (len(x.patterns), x),
                               self.forks))[1]
            else:
                # For now, we just do a round-robin to decide which fork to
                # use. This is round-robin by PATTERN, not on a per-datum
                # basis.
                # We do this on a pattern-by-pattern basis because it prevents
                # this function from being run for each datum of a pattern.
                index = len(self.pattern_assignments) % self.max_forks
                f = self.forks[index]

        # Make a note of where we're putting the pattern and return it
        self.pattern_assignments[pattern] = f
        return f

    def process(self):
        "Processes the data and yields the result"
        self.processing = True

        for f in self.forks:
            f._write_submission(ForkStart(), outbound=False)

        while self.data or any(f.working for f in self.forks):
            # Write lots
            for pattern in self.data.keys():
                fork = self._get_pattern_forklet(pattern)

                slice = self.data[pattern]
                del self.data[pattern]

                for datum in slice:
                    fork.feed(pattern, datum)

            # Read lots
            for fork in self.forks:
                # Note that we need to do this because if no patterns
                # are fed to a fork that has completed data, we'll never get
                # our data back.
                for pattern, datum in fork.collect():
                    self._queue(pattern, datum)

            # Yield data with no reducers or mappers
            for pattern in self.data.keys():
                if pattern not in self.hooks[MAP] and \
                   pattern not in self.hooks[REDUCE]:
                    yield pattern, self.data[pattern]
                    del self.data[pattern]

            time.sleep(PERIOD) # Give some time back to the forks

        for f in self.forks:
            f._write_submission(ForkPoison(), outbound=False)
        self.processing = False

class ForkTimestamp(object):
    "Just a timestamp object"

    def __init__(self):
        self.timestamp = time.time()

class ForkComplete(ForkTimestamp):
    """Notification that the fork has completed its assigned work up to a
    certain timestamp"""
    pass

class ForkStart(ForkTimestamp):
    "Begins the processing process"
    pass

class ForkPoison(object):
    "Instructs a fork to die."
    pass

class Forklet(object):
    "A class to help with the division of work among forks"

    def __init__(self, router, pattern=None, prespawn=False):
        self.router = router

        self.pid = 0
        self.is_child = False
        self.working = False
        self.startwork = not prespawn
        self.die = False

        #print "Creating fork"

        # Initialize input/output pipes

        self.output_r, self.output_w = os.pipe()
        self.input_r, self.input_w = os.pipe()

        out_rl = fcntl.fcntl(self.output_r, fcntl.F_GETFL)
        fcntl.fcntl(self.output_r, fcntl.F_SETFL, out_rl | os.O_NONBLOCK)

        in_rl = fcntl.fcntl(self.input_r, fcntl.F_GETFL)
        fcntl.fcntl(self.input_r, fcntl.F_SETFL, in_rl | os.O_NONBLOCK)

        self.output_r, self.output_w = os.fdopen(self.output_r, 'r', 0), \
                                       os.fdopen(self.output_w, 'w', 0)
        self.input_r, self.input_w = os.fdopen(self.input_r, 'r', 0), \
                                     os.fdopen(self.input_w, 'w', 0)

        # Set up patterns

        self.patterns = set()
        if pattern is not None:
            self.patterns.add(pattern)

        self.queue = {}

        if prespawn:
            self.fork()

    def fork(self):
        "Forks the process and handles where processing should go"
        self.pid = os.fork()
        self.is_child = self.pid == 0

        if not self.is_child:
            self.output_w.close()
            self.input_r.close()
            # Relinquish control back to the router
            return True


        self.output_r.close()
        self.input_w.close()

        while not self.die:
            # Read in new datum
            for pattern, datum in self.collect():
                self.queue[pattern].append(datum)

            # Process and output
            if self.startwork and self.queue:
                for pattern in self.queue.keys():
                    slice = self.queue[pattern][:]
                    del self.queue[pattern]

                    if pattern in self.router.hooks[REDUCE]:
                        gen = self.router.hooks[REDUCE][pattern](slice)
                        if gen is None:
                            continue
                        for new_pattern, datum in gen:
                            self.submit(new_pattern, datum)
                    elif pattern in self.router.hooks[MAP]:
                        for datum in slice:
                            for new_pattern, new_datum in self.router.hooks[MAP][pattern](datum):
                                self.submit(new_pattern, new_datum)
                    else:
                        self.patterns.discard(pattern)
                        for datum in slice:
                            self.submit(pattern, datum)

                if not self.queue:
                    self._write_submission(ForkComplete())

            time.sleep(PERIOD) # Read somewhere that this is what you should do

        os._exit(0)

    def feed(self, pattern, datum):
        "Sends an unprocessed datum to the fork"

        self.working = time.time()

        if pattern not in self.patterns:
            self.patterns.add(pattern)

        #print ">>", self.pid, pattern, datum

        cPickle.dump((pattern, datum),
                     self.input_w,
                     protocol=cPickle.HIGHEST_PROTOCOL)
        self.input_w.flush()

    def submit(self, pattern, datum):
        "Sends a completed datum back to the router"

        if pattern in self.router.hooks[FILTER] and \
           any(filter_(datum) for
               filter_ in
               self.router.hooks[FILTER][pattern]):
            return

        #print "<", pattern, datum
        if pattern in self.queue or not self.router.homogeneous:
            self._queue(pattern, datum)
            return

        self._write_submission((pattern, datum))

    def _write_submission(self, blob, outbound=True):
        "Submits a picklable blob to the router"
        #print "<" if outbound else ">>", blob
        p = self.output_w if outbound else self.input_w
        cPickle.dump(blob,
                     p,
                     protocol=cPickle.HIGHEST_PROTOCOL)
        p.flush()

    def collect(self):
        "Yields completed pattern-datum pairs."
        if self.is_child:
            while True:
                try:
                    inbound = cPickle.load(self.input_r)
                    #print ">", inbound
                except:
                    # Nothing to read, carry on
                    break

                # Handle "start work" signals
                if isinstance(inbound, ForkStart):
                    self.startwork = True
                    continue
                elif isinstance(inbound, ForkPoison):
                    self.die = True
                    return

                self.working = True

                pattern, datum = inbound
                self.patterns.add(pattern)

                if pattern not in self.queue:
                    self.queue[pattern] = []

                yield pattern, datum

        else:
            while True:
                try:
                    outbound = cPickle.load(self.output_r)
                    #print "<<", self.pid, outbound
                except:
                    break

                if isinstance(outbound, ForkComplete):
                    if outbound.timestamp > self.working:
                        self.working = False
                        break
                    else:
                        continue
                else:
                    yield outbound # We'll assume it's a 2-tuple

