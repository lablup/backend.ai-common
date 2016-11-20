#! /usr/bin/env python3

from collections import OrderedDict, UserDict
# simplejson serializes namedtuples into objects and offers C-based acceleration. (Yay!)
import simplejson as json

from ..utils import odict


class Message(UserDict):
    '''
    A dictionary-like container for API messages.
    '''

    def __init__(self, *args, **kwargs):
        self.data = odict(*args)
        self.data.update(kwargs)

    @staticmethod
    def _from_odict(od):
        assert isinstance(od, OrderedDict)
        m = Message()
        m.data = od
        return m

    def encode(self):
        s = json.dumps(self.data, ensure_ascii=False)
        return s.encode('utf8', errors='replace')

    @staticmethod
    def decode(s):
        if isinstance(s, bytes):
            s = s.decode('utf8')
        root_od = json.loads(s, object_pairs_hook=OrderedDict)
        return Message._from_odict(root_od)
