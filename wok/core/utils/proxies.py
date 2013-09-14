# from http://www.voidspace.org.uk/python/weblog/arch_d7_2007_03_17.shtml#e664

def ReadOnlyProxy(obj):
    class _ReadOnlyProxy(object):
        def __getattr__(self, name):
            return getattr(obj, name)

        def __setattr__(self, name, value):
            raise AttributeError("Attributes can't be set on this object")

    for name in dir(obj):
        if not (name[:2] == '__' == name[-2:]):
            continue
        if name in ('__new__', '__init__', '__class__', '__bases__'):
            continue
        if not callable(getattr(obj, name)):
            continue

        def get_proxy_method(name):
            def proxy_method(self, *args, **keywargs):
                return getattr(obj, name)(*args, **keywargs)
            return proxy_method

        setattr(_ReadOnlyProxy, name, get_proxy_method(name))

    return _ReadOnlyProxy()