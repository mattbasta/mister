import mr
import routers

@routers.test_multi
def test_reduce():
    """Test that data is reduced properly."""

    @mr.map("first")
    def first(item):
        for i in range(5):
            yield "second", str(i), 1
            yield "second_alt", str(i), 1

    @mr.reduce("second")
    def second(items, rereduce):
        output = 0
        for item in items:
            output += item.value
        return "third", "reduced_sec", output

    @mr.reduce("second_alt")
    def second_alt(items, rereduce):
        output = 0
        for item in items:
            output += item.value
        return "third", "reduced_sec_alt", output

    @mr.reduce("third")
    def third(items, rereduce):
        # This should only fire during a rereduction.
        assert rereduce

        output = 0
        for item in items:
            output += item.value
        return "third", "rereduced", output

    mr.feed("first", None)
    results = list(mr.process())
    print results
    assert len(results) == 1
    assert results[0].value == 10

