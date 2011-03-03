import mr
from mr.router import Router
from mr.diskrouter import DiskRouter
from mr.forkingrouter import ForkingRouter

def test_multi(test):
    "Runs a test on multiple routers"
    def test_wrap():
        # Router
        print "Running: Router"
        mr.reset_hooks()
        mr.set_router(Router())
        test()

        # DiskRouter
        print "Running: DiskRouter"
        mr.reset_hooks()
        mr.set_router(DiskRouter())
        test()

        # ForkingRouter
        print "Running: ForkingRouter"
        mr.reset_hooks()
        mr.set_router(ForkingRouter())
        test()

    return test_wrap

