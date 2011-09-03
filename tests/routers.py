import mr
from time import time
from mr.router import Router
from mr.diskrouter import DiskRouter
from mr.forkingrouter import ForkingRouter


ENABLE_ROUTER = True
ENABLE_DISKROUTER = False
ENABLE_FORKINGROUTER = False


def performance(func):
    t0 = time()
    func()
    tend = time()
    print tend - t0

def test_multi(test):
    "Runs a test on multiple routers"
    def test_wrap():
        print "\n", test.__name__

        if ENABLE_ROUTER:
            # Router
            print "Running: Router"
            mr.reset_hooks()
            mr.set_router(Router())
            performance(test)

        if ENABLE_DISKROUTER:
            # DiskRouter
            print "Running: DiskRouter"
            mr.reset_hooks()
            mr.set_router(DiskRouter())
            performance(test)

        if ENABLE_FORKINGROUTER:
            # ForkingRouter
            print "Running: ForkingRouter"
            mr.reset_hooks()
            mr.set_router(ForkingRouter())
            performance(test)

    return test_wrap

