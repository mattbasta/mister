import mr

def test_exists():
    "Makes sure that a registered mapper exists as a hook"
    
    mr.reset_hooks()
    @mr.hook(mr.MAP, "foo")
    def _null(data):
        pass

    assert mr.router_instance.hooks["map"]

def test_filter_list():
    "Tests that filters are placed in a list"

    mr.reset_hooks()
    @mr.hook(mr.FILTER, "foo")
    def test(data):
        pass

    assert isinstance(mr.router_instance.hooks["filter"]["foo"], list)

