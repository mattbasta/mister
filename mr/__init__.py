import re
from router import Router
from constants import *

class Processor(object):
    "Sets up the correct router"

    def __init__(self):
        self.router = Router()

    def set_router(self, router):
        self.router = router

_processor = Processor()

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

def set_router(instance):
    "Sets a new router"
    _processor.set_router(instance)

def feed(pattern, datum):
    "Feeds a raw blob of data into the queue for type 'pattern'"
    _processor.router.feed(pattern, datum)

def reset_hooks():
    "Removes all hooks from the router"
    _processor.router.reset_hooks()

def hook(stage, pattern):
    "Registers mister hooks for stage with pattern"
    def wrap(function):
        r = _processor.router
        if stage not in r.hooks:
            raise Exception("Could not find hook stage '%s'" % stage)
        if stage == FILTER:
            if pattern not in r.hooks[stage]:
                r.hooks[stage][pattern] = []
            r.hooks[stage][pattern].append(function)
        else:
            r.hooks[stage][pattern] = function
        return function
    return wrap

