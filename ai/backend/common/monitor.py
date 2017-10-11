'''
A collection of dummy monitoring daemon classes with no-op stubs.
Useful to avoid excessive repetition of "if app['datadog']:
app['datadog'].statsd.increment(...)" snippets all scattered around the
codebase.
'''


class DummyStatsd:
    def increment(self, *args, **kwargs): pass  # noqa
    def event(self, *args, **kwargs): pass      # noqa
    def gauge(self, *args, **kwargs): pass      # noqa
    def __enter__(self): return self                     # noqa
    def __exit__(self, exc_type, exc_val, exc_tb): pass  # noqa


class DummyDatadog:
    statsd = DummyStatsd()


class DummySentry:
    def captureException(self, *args, **kwargs): pass  # noqa
    def captureMessage(self, *args, **kwargs): pass    # noqa
    def __enter__(self): return self                     # noqa
    def __exit__(self, exc_type, exc_val, exc_tb): pass  # noqa
