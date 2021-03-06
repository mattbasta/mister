Mister v0.6 BETA
================

A simple, friendly map/filter/reduce library.

Mister makes it easy to process lists of data. With a super-simple API based
around Python generators, simple tasks that might otherwise require custom
scripts or Hadoop now take just a few lines of code.

Features
--------

- Mapping, reducing, filtering
- Beautiful Python decorators
- Disk buffering
- Process forking for multi-core support

Sample Code
-----------

::

    import mr

    # Add a mapper
    @mr.hook(mr.MAP, "log entry")
    def parse_entry(entry):
        if entry.startswith("Warning!"):
            yield "warning", entry
        elif entry.startswith("ERROR"):
            yield "error", entry
        else:
            yield "notice", entry

    # Add a filter
    @mr.hook(mr.FILTER, "notice")
    def delete_noise(entry):
        if entry.contains("spawned new thread"):
            return True # Delete the entry

    # Add a reducer
    @mr.hook(mr.REDUCE, "error")
    def group_errors(errors): # errors is a list
        yield "error block", "\n".join(entry)

    for line in open("/var/log/foo.log").lines():
        mr.feed("log entry", line)

    for tag, result in mr.process():
        for item in result:
            if tag == "warning":
                open("warnings.txt", mode="a").write(item)
            elif tag == "error block":
                email_errors_to_somebody(item)
            elif tag == "notice":
                print "Simple notice: %s" % item

Future Enhancements
-------------------

- Support for multiple servers ("bean server")
- Support for drop-in hooks ("rogers")


"It's a beautiful day in the neighborhood,
 a beautiful day in the neighborhood.
 Would you be mine? Could you be mine?"

