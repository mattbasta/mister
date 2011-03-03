import mr
import routers

@routers.test_multi
def test_foobar():
    mr.reset_hooks()
    
    @mr.hook(mr.MAP, "foo")
    def process_foo(query):
        yield "bar", query + 1
    
    @mr.hook(mr.MAP, "bar")
    def process_bar(query):
        yield "abc", query + 1

    mr.feed("foo", 0)
    
    results = list(mr.process())
    assert len(results) == 1
    print results
    assert results[0] == ("abc", [2])

@routers.test_multi
def test_recursive():
    mr.reset_hooks()

    @mr.hook(mr.MAP, "ill")
    def hospital(input):
        if input == "pregger":
            yield "healthy", "babby"
        elif input == "chicken pox":
            yield "healthy", "itchy"

    @mr.hook(mr.MAP, "healthy")
    def apartment(input):
        if input == "tummy":
            yield "ill", "pregger"
        elif input == "babby":
            yield "ill", "chicken pox"
        elif input == "itchy":
            yield "medicine", "ointment"

    mr.feed("healthy", "tummy")
    assert list(mr.process())[0] == ("medicine", ["ointment"])

