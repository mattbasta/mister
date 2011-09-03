import re
from router import Router


class Processor(object):
    "Sets up the correct router"

    def __init__(self):
        self.router = Router()

    def set_router(self, router):
        self.router = router

_processor = Processor()
set_router = _processor.set_router


def process(as_lists=False):
    "Initiates the Router's processor"
    output = _processor.router.process()
    if as_lists:
        # Wrap the router in a generator to yield as lists
        def list_wrap(gen):
            for pattern, datums in gen:
                yield pattern, list(datums)
        return list_wrap(output)

    # Otherwise, just output the standard generator
    return output


def feed(pattern, datum):
    "Feeds a raw blob of data into the queue for type 'pattern'"
    _processor.router.feed(pattern, datum)


def reset_hooks():
    "Removes all hooks from the router"
    _processor.router.reset_hooks()


def map(pattern):
    """Register a mapping function for a pattern."""
    def wrap(function):
        r = _processor.router
        r.mappers[pattern] = function
        return function
    return wrap


def reduce(pattern):
    """Register a reducing function for a pattern."""
    def wrap(function):
        r = _processor.router
        r.reducers[pattern] = function
        return function
    return wrap


def filter(pattern):
    """Register a mapping function for a pattern."""
    def wrap(function):
        r = _processor.router
        if pattern not in r.filters:
            r.filters[pattern] = []
        r.filters[pattern].append(function)
        return function
    return wrap

