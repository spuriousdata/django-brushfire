from django.conf import settings
from brushfire.core.exceptions import BrushfireConfigException

conf = settings.get('BRUSHFIRE', None)
if conf is None:
    raise BrushfireConfigException("`BRUSHFIRE` dict must be present in settings module")

BRUSHFIRE = {
    'host': 'http://localhost:8080/solr',
    'cache': {
        'method': 'file', # or django
        'path': '/tmp/.cache', # if file
        'prefix': 'solrcache', # if django
    },
    'cores': {
        'query': 'collection1',
        'index': 'collection2',
        'admin': '/admin/cores',
    },
    'handlers': {
        'default': 'edismax',
        'custom': {
            'typename': 'handlername',
            'typename2': 'handlername2',
            'mlt': 'morelikethis',
        },
    },
    'index': {
        'method': 'dih', # or 'document add' -- not sure about this name
        'dih': {
            'handler': '/dataimport',
            'command': 'full-import',
            'clear': True,
            'swap_cores_on_complete': ['collection1', 'collection2'],
        },
    },
}

class Configuration(object):
    def __init__(self, c):
        self.c = c
        self.__set('host')

    def __set(self, property, dict_key=None, required=True):
        if dict_key is None:
            dict_key = property

        value = self.c
        try:
            keypath = dict_key.split('.')
            for k in keypath:
                value = value[k]
        except KeyError:
            if required:
                setting_path = "BRUSHFIRE['" + "']['".join([x for x in dict_key.split('.')]) + "']"
                raise BrushfireConfigException("%s is a required setting" % setting_path)
        else:
            setattr(self, property, value)

    def configure_solr(self, solr):
        s = solr(conf)
        return s

if __name__ == '__main__':
    c = Configuration(BRUSHFIRE)
    import pdb; pdb.set_trace() 
