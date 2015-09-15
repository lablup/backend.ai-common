#! /usr/bin/env python3

from collections import OrderedDict, UserDict
import base64
# simplejson serializes namedtuples into objects and offers C-based acceleration. (Yay!)
import simplejson as json
import uuid


def odict(*args):
    '''
    A short-hand for the constructor of OrderedDict.
    :code:`odict(('a':1), ('b':2))` is equivalent to :code:`OrderedDict([('a':1), ('b':2)])`.
    '''
    return OrderedDict(args)

def generate_uuid():
    u = uuid.uuid4()
    # Strip the last two padding characters because u always has fixed length.
    return base64.urlsafe_b64encode(u.bytes)[:-2].decode('ascii')


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
        return bytes(json.dumps(self.data, ensure_ascii=False),
                     encoding='utf8')

    @staticmethod
    def decode(s):
        if isinstance(s, bytes):
            s = s.decode('utf8')
        root_od = json.loads(s, object_pairs_hook=OrderedDict)
        return Message._from_odict(root_od)

