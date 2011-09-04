from time import time

import mr
from mr.router import Router
from mr.diskrouter import DiskRouter
from mr.forkingrouter import ForkingRouter


ENABLE_ROUTER = True
ENABLE_DISKROUTER = True
ENABLE_FORKINGROUTER = False


def performance(func):
    t0 = time()
    func()
    tend = time()
    print tend - t0


def run_test(router, test):
    print "Test: %s" % test.__name__
    print "Running: %s" % router.__name__
    mr.reset_hooks()
    mr.set_router(router())
    performance(test)


def test_multi(test):
    "Runs a test on multiple routers"

    def test_wrap():
        print "Testing..."
        if ENABLE_ROUTER:
            run_test(Router, test)

        if ENABLE_DISKROUTER:
            run_test(DiskRouter, test)

        if ENABLE_FORKINGROUTER:
            run_test(ForkingRouter, test)

    return test_wrap

    # Nerf the rest of this until nose fixes its shit.
    #def test_wrap():
    #    if ENABLE_ROUTER:
    #        yield run_test, Router, test
    #
    #    if ENABLE_DISKROUTER:
    #        yield run_test, DiskRouter, test
    #
    #    if ENABLE_FORKINGROUTER:
    #        yield run_test, ForkingRouter, test
    #
    #return test_wrap

