import mr
import routers

@routers.test_multi
def test_foobar():
    mr.reset_hooks()

    @mr.map("foo")
    def process_foo(query):
        yield "bar", "from_foo", query.value + 1

    @mr.map("bar")
    def process_bar(query):
        yield "abc", "from_bar", query.value + 1

    mr.feed("foo", 0)

    results = list(mr.process())
    assert len(results) == 1
    print results
    assert results[0].value == 2

@routers.test_multi
def test_recursive():
    mr.reset_hooks()

    @mr.map("ill")
    def hospital(input):
        input = input.value
        if input == "pregger":
            yield "healthy", "from_hospital", "babby"
        elif input == "chicken pox":
            yield "healthy", "from_hospital", "itchy"

    @mr.map("healthy")
    def apartment(input):
        input = input.value
        if input == "tummy":
            yield "ill", "from_home", "pregger"
        elif input == "babby":
            yield "ill", "from_home", "chicken pox"
        elif input == "itchy":
            yield "medicine", "from_home", "ointment"

    mr.feed("healthy", "tummy")
    assert list(mr.process())[0].value == "ointment"

