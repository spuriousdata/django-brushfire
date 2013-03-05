from brushfire import solr
from brushfire.core.settings import configuration as conf
from django.db.models import Q

import logging
logger = logging.getLogger('brushfire.core.query')

class BrushfireQuerySet(object):
    def __init__(self, model):
        self.model = model
        self.results = []
        self.querystring = "*:*"
        self._filters = []
        self._sort = ""

    def __len__(self):
        self.__run()
        return len(self.results)

    def __getitem__(self, key):
        self.__run()
        return self.results[key]

    def filter(self, *args, **kwargs):
        logger.debug("Adding filter %s, %s", str(args), str(kwargs))
        self._filters.append(Q(*args, **kwargs))
        return self

    def all(self):
        return self

    def get(self, *args, **kwargs):
        r = self.filter(*args, **kwargs)
        if len(r) > 1:
            raise self.model.MultipleResultsFound
        elif len(r) == 0:
            raise self.model.DoesNotExist
        else:
            return r[0]

    def order_by(self, *args):
        for i in args:
            i = "%s ASC" % i if not i.startswith('-') else "%s DESC" % i
            self._sort += i
        return self

    def __run(self):
        results = solr.search(self.querystring).get('response', {})
        for x in results['docs']:
            obj = self.model()
            for k,v in x.items():
                setattr(obj, k, v)
            self.results.append(obj)
