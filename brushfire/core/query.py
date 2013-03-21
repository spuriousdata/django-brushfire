from brushfire.core.driver import SolrQuery, SQ
from django.db.models.query import QuerySet

import logging
logger = logging.getLogger('brushfire.core.query')

class BrushfireQuerySet(QuerySet):
    def __init__(self, model=None, query=None, using=None):
        super(BrushfireQuerySet, self).__init__(model, query, using)
        self.query = query or SolrQuery("*:*")

    def sort(self, *fields):
        return self.order_by(*fields)

    def order_by(self, *fields):
        assert self.query.can_filter(), \
                "Cannot filter a query once a slice has been taken."
        clone = self._clone()
        clone.query.clear_ordering().add_ordering(*fields)
        return clone

    def _clone(self, klass=None, setup=False, **kwargs):
        try:
            kwargs.update({'return_fields':self.return_fields})
        except AttributeError:
            pass
        return super(BrushfireQuerySet, self)._clone(klass, setup, **kwargs)

    def values(self, *fields):
        clone = self._clone(BrushfireValuesQuerySet)
        clone.query.set_fields(*fields)
        setattr(clone, 'return_fields', fields)
        return clone

    def iterator(self):
        results = self.query.run().get('response', {})
        for x in results['docs']:
            obj = self.model()
            for k,v in x.items():
                setattr(obj, k, v)
                if k == self.model()._meta.pk.name:
                    setattr(obj, 'pk', v)
            yield obj


    def _filter_or_exclude(self, negate, *args, **kwargs):
        logger.debug("Called filter or exclude with (%s, %r, %r)", negate, args, kwargs)
        if args or kwargs:
            assert self.query.can_filter(), \
                    "Cannot filter a query once a slice has been taken."
        clone = self._clone()
        if negate:
            clone.query.add_q(~SQ(*args, **kwargs))
        else:
            clone.query.add_q(SQ(*args, **kwargs))
        return clone

class BrushfireValuesQuerySet(BrushfireQuerySet):
    def iterator(self):
        results = self.query.run().get('response', {})
        for x in results['docs']:
            obj = {}
            for k,v in x.items():
                if k in self.return_fields:
                    obj[k] = v
            yield obj
