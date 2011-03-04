import mr
from time import time
from mr.router import Router
from mr.diskrouter import DiskRouter
from mr.forkingrouter import ForkingRouter


def performance(func):
    t0 = time()
    func()
    tend = time()
    print tend - t0

def test_multi(test):
    "Runs a test on multiple routers"
    def test_wrap():
        print "\n", test.__name__
        
        # Router
        print "Running: Router"
        mr.reset_hooks()
        mr.set_router(Router())
        performance(test)

        # DiskRouter
        print "Running: DiskRouter"
        mr.reset_hooks()
        mr.set_router(DiskRouter())
        performance(test)

        # ForkingRouter
        print "Running: ForkingRouter"
        mr.reset_hooks()
        mr.set_router(ForkingRouter())
        performance(test)

    return test_wrap

