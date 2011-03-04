import mr
import routers

@routers.test_multi
def test_simple_filter():
    "Tests that simple filters work"

    @mr.hook(mr.FILTER, "filterable")
    def delete_even(input):
        return input % 2 == 0

    for i in range(500):
        mr.feed("filterable", i)

    results = list(mr.process())
    assert len(results) == 1
    pattern, odds = results[0]
    assert all(x % 2 == 1 for x in odds)

