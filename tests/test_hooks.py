import mr
import routers

@routers.test_multi
def test_exists():
    "Makes sure that a registered mapper exists as a hook"
    
    @mr.hook(mr.MAP, "foo")
    def _null(data):
        pass

    assert mr._processor.router.hooks["map"]

@routers.test_multi
def test_filter_list():
    "Tests that filters are placed in a list"

    @mr.hook(mr.FILTER, "foo")
    def test(data):
        pass

    assert isinstance(mr._processor.router.hooks["filter"]["foo"], list)

@routers.test_multi
def test_reducer_precedence():
    "Tests that reducers take precedence over mappers"

    @mr.hook(mr.REDUCE, "foo")
    def red(data):
        return

    @mr.hook(mr.MAP, "foo")
    def m(datum):
        yield "bar", True

    mr.feed("foo", 1)
    assert not list(mr.process())

