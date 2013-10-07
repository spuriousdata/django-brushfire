from django.conf import settings
from brushfire.core.exceptions import BrushfireConfigException
from django.utils.importlib import import_module

conf = getattr(settings, 'BRUSHFIRE', None)
if conf is None:
    raise BrushfireConfigException("`BRUSHFIRE` dict must be present in settings module")

"""
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
    'query': {
        'fields': '*,score',
        'lparams': "{!edismax qf='text^2 name^100' bf='name'}",
        'rows': 20,
    },
}
"""

class Configuration(object):
    def __init__(self, c):
        self.c = c
        self.__set('host')
        self.__set('query_core', 'cores.query', False, '') 
        self.__set('default_handler', 'handlers.default', False, 'select') 
        self.__set('default_lparams', 'query.lparams', False, '') 
        self.set_query_cache()

    def set_query_cache(self):
        c = self.get('cache.method', False)
        if c is None:
            self.query_cache = None
        else:
            if c not in ('file', 'django'):
                self.query_cache = import_module(c)()
            elif c == 'file':
                self.query_cache = self.get('cache.path')
            elif c == 'django':
                from django.core.cache import get_cache
                self.query_cache = get_cache(self.get('cache.which', False, 'default'))

    def __set(self, property, dict_key=None, required=True, default=None):
        if dict_key is None:
            dict_key = property
        value = None
        try:
            value = self.get(dict_key, required, default)
        except BrushfireConfigException as e:
            if default is not None:
                value = default
            else:
                raise
        setattr(self, property, value)

    def get(self, dict_key, required=True, default=None):
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
                return default
        else:
            return value

    def set_solr(self, solr):
        setattr(self, 'solr_connection', solr)


    def configure_solr(self, solr):
        self.solr_conn = solr(
            server=self.host,
            core=self.query_core,
            query_handler=self.default_handler,
            lparams=self.default_lparams,
            cache=self.query_cache,
	        fields=self.get('query.fields', False, '*,score'),
            rows=self.get('query.rows', False, 20),
        )
        return self.solr_conn

configuration = Configuration(conf)

if __name__ == '__main__':
    c = Configuration(BRUSHFIRE)
    import pdb; pdb.set_trace() 
