from brushfire.core.driver import SolrQuery, SQ
from django.db.models.query import QuerySet
from django.utils.datastructures import SortedDict

import logging
logger = logging.getLogger('brushfire.core.query')

class ModelLookalikeObject(object):
    def __getattr__(self, a):
        return None

class BrushfireQuerySet(QuerySet):
    def __init__(self, model, query=None, using=None, allow_non_model_fields=False):
        super(BrushfireQuerySet, self).__init__(model, query, using)
        self.query = query or SolrQuery(model)
        self.facet_counts = None
        self.allow_non_model_fields = allow_non_model_fields

    def sort(self, *fields):
        return self.order_by(*fields)

    def order_by(self, *fields):
        assert self.query.can_filter(), \
                "Cannot filter a query once a slice has been taken."
        clone = self._clone()
        clone.query.clear_ordering().add_ordering(*fields)
        return clone

    def count(self):
        return self.query.get_count()

    def __len__(self):
        return self.count()

    def _clone(self, klass=None, setup=False, **kwargs):
        try:
            kwargs.update({'return_fields':self.return_fields})
        except AttributeError:
            pass
        clone = super(BrushfireQuerySet, self)._clone(klass, setup, **kwargs)
        clone.allow_non_model_fields = self.allow_non_model_fields
        return clone

    def search(self, q, **extra):
        clone = self._clone()
        if len(extra):
            clone.query.add_extra_params(extra)
        clone.query.default_search(q)
        return clone

    def facet(self, *fields):
        clone = self._clone()
        clone.query.add_facets(*fields)
        return clone

    def values(self, *fields):
        clone = self._clone(BrushfireValuesQuerySet)
        clone.query.set_fields(*fields)
        return clone

    def values_obj(self, *fields):
        clone = self._clone(BrushfireValuesObjectQuerySet)
        if len(fields) == 0:
            fields = ('*',)
        clone.query.set_fields(*fields)
        return clone

    def values_list(self, *fields):
        clone = self._clone(BrushfireValuesListQuerySet)
        clone.query.set_fields(*fields)
        return clone

    def iterator(self):
        results = self.query.run()
        self.docs = results.get('response', {})
        self.facet_counts = results.get('facet_counts', {})

        for x in self.docs.get('docs', []):
            yield self.postprocess_result(x)

    def get_facet_counts(self):
        if not self.facet_counts:
            results = self.query.run()
            self.docs = results.get('response', {})
            self.facet_counts = results.get('facet_counts', {})
        ret = {}
        ff = self.facet_counts.get('facet_fields', {})
        for key in ff.keys():
            ret[key] = dict(zip(ff[key][::2], ff[key][1::2]))
        return ret

    def postprocess_result(self, result):
        pk = self.query.get_meta().pk.column
        keys = set(['foo'] + [x.name for x in self.query.get_meta().fields]) & set(result.keys())
        r = {k:result[k] for k in keys}
        r['pk'] = r[pk]
        model = self.model(**r)
        if self.allow_non_model_fields:
            m = ModelLookalikeObject()
            m.__dict__.update(model.__dict__)
            nmf = set(result.keys()) - set(['foo'] + [x.name for x in self.query.get_meta().fields])
            r = {k:result[k] for k in nmf}
            m.__dict__.update(r)
            m.pk = model.pk
            model = m
        return model

    def narrow_group(self, key, values, connector='OR'):
        qs = None
        for v in values:
            if qs is None:
                qs = SQ(**{key:v})
            else:
                if connector == 'OR':
                    qs = qs | SQ(**{key:v})
                else:
                    qs = qs & SQ(**{key:v})
        clone = self._clone()
        clone.query.add_q(qs, property='fq')
        return clone

    def narrow(self, *args, **kwargs):
        logger.debug("Called narrow with (%r, %r)", args, kwargs)
        if args or kwargs:
            assert self.query.can_filter(), \
                    "Cannot filter a query once a slice has been taken."
        clone = self._clone()

        if kwargs.pop('__brushfire_connector__', 'AND') != 'AND':
            clone.query.add_q(SQ(*args, **kwargs), connector=SQ.OR, property='fq')
        else:
            clone.query.add_q(SQ(*args, **kwargs), property='fq')
        return clone

    def get(self, *args, **kwargs):
        return self.filter(*args, **kwargs)[0]

    def _filter_or_exclude(self, negate, *args, **kwargs):
        logger.debug("Called filter or exclude with (negate:%s, %r, %r)", negate, args, kwargs)
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
    def postprocess_result(self, result):
        return SortedDict(filter(lambda x: x[0] in self.query.fields and x[0] != 'score', result.items()))

class BrushfireValuesListQuerySet(BrushfireValuesQuerySet):
    def postprocess_result(self, result):
        return super(BrushfireValuesListQuerySet, self)\
                .postprocess_result(result).values()

class BrushfireValuesObjectQuerySet(BrushfireValuesQuerySet):
    def postprocess_result(self, result):
        return self.dict_to_object(result)

    def dict_to_object(self, d):
        class DictObject(object):
            def __init__(self, dct):
                self.__dict__.update(dct)
        return DictObject(d)
