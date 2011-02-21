import re
from router import Router
from constants import *

router_instance = Router()
process = router_instance.process

def feed(pattern, datum):
    "Feeds a raw blob of data into the queue for type 'pattern'"
    if not router_instance:
        return
    router_instance.feed(pattern, datum)

def reset_hooks():
    "Removes all hooks from the router"
    router_instance.reset_hooks()

def hook(stage, pattern):
    "Registers mister hooks for stage with pattern"
    def wrap(function):
        if stage not in router_instance.hooks:
            raise Exception("Could not find hook stage '%s'" % stage)
        if stage == FILTER:
            if pattern not in router_instance.hooks[stage]:
                router_instance.hooks[stage][pattern] = []
            router_instance.hooks[stage][pattern].append(function)
        else:
            router_instance.hooks[stage][pattern] = function
        return function
    return wrap



