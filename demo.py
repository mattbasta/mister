import mr
from mr.diskrouter import DiskRouter
mr.set_router(DiskRouter())

@mr.hook(mr.MAP, "query")
def parse_query(query):
    "Takes an integer and yields a prime number and another query."
    if query % 2 == 0:
        yield "prime", 2
        yield "query", query / 2
        return
    i = 3
    while i < query / 2:
        if query % i == 0:
            yield "prime", i
            yield "query", query / i
            return
        i += 2
    yield "prime", query

mr.feed("query", 124846525121166472890127769845656706959834701767553316679575342375728606681436245953703527478773456698735316531921607496638484885416740029028542605893861455745313937474271661656548230159065196413238268640890)

print list(mr.process())

