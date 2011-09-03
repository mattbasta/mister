import mr
import routers

@routers.test_multi
def test_exists():
    "Makes sure that a registered mapper exists as a hook"

    @mr.map("foo")
    def _null(data):
        pass

    assert mr._processor.router.mappers
    assert "foo" in mr._processor.router.mappers

@routers.test_multi
def test_filter_list():
    "Tests that filters are placed in a list"

    @mr.filter("foo")
    def test(data):
        pass

    assert mr._processor.router.filters
    assert "foo" in mr._processor.router.filters
    assert isinstance(mr._processor.router.filters["foo"], list)

@routers.test_multi
def test_reducer_precedence():
    "Tests that reducers take precedence over mappers"

    @mr.reduce("foo")
    def red(data, rereduce):
        return

    @mr.map("foo")
    def m(datum):
        yield "bar", True

    mr.feed("foo", 1)
    assert not list(mr.process())

