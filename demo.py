import mr

@mr.hook(mr.MAP, "query")
def parse_query(query):
    "Takes an integer and yields a prime number and another query."
    if query % 2 == 0:
        yield "prime", 2
        yield "query", query / 2
    i = 3
    while i < query / 2:
        if query % i == 0:
            yield "prime", i
            yield "query", query / i
            return
        i += 2
    yield "prime", query

mr.feed("query", 7)

print list(mr.process())

