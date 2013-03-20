from brushfire import solr
from brushfire.core.driver import SolrQuery, SQ
from brushfire.core.settings import configuration as conf
from django.db.models.query import QuerySet
from django.utils.tree import Node

import logging
logger = logging.getLogger('brushfire.core.query')

class BrushfireQuerySet(QuerySet):
    def __init__(self, model=None, query=None, using=None):
        super(BrushfireQuerySet, self).__init__(model, query, using)
        self.query = query or SolrQuery("*:*")

    def order_by(self, *fields):
        assert self.query.can_filter(), \
                "Cannot filter a query once a slice has been taken."
        clone = self._clone()
        """
        clone.query.clear_ordering(force_empty=False)
        clone.query.add_ordering(*fields)
        """
        return clone

    def iterator(self):
        results = solr.search(self.query.get_querystring()).get('response', {})
        for x in results['docs']:
            obj = self.model()
            for k,v in x.items():
                setattr(obj, k, v)
                if k == self.model()._meta.pk.name:
                    setattr(obj, 'pk', v)
            yield obj

    def _filter_or_exclude(self, negate, *args, **kwargs):
        if args or kwargs:
            assert self.query.can_filter(), \
                    "Cannot filter a query once a slice has been taken."
        clone = self._clone()
        if negate:
            clone.query.add_q(~SQ(*args, **kwargs))
        else:
            clone.query.add_q(SQ(*args, **kwargs))
        return clone
