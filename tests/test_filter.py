import mr
import routers

@routers.test_multi
def test_simple_filter():
    "Tests that simple filters work"

    @mr.filter("filterable")
    def delete_even(input):
        return input.value % 2 == 0

    for i in range(500):
        mr.feed("filterable", i)

    results = list(mr.process())
    assert all(x.value % 2 == 1 for x in results)

