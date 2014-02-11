import json

from brushfire.core.driver import SolrQuery, SQ
from django.db.models.query import QuerySet
from django.utils.datastructures import SortedDict
from django.utils.importlib import import_module

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
        self.term_vectors = None
        self.stats = None
        self.allow_non_model_fields = allow_non_model_fields

    def sort(self, *fields):
        return self.order_by(*fields)

    def use_handler(self, handler):
        self.query.set_handler(handler)
        return self

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

    def tf(self, *fields):
        """shortcut for term_frequency()"""
        return self.term_frequency(*fields)

    def term_frequency(self, *fields):
        """Adds tv.tf to query to return term frequency counts for listed fields"""
        clone = self._clone()
        clone.query.add_extra_params({"tv.tf":True,"tv.fl":','.join(fields)})
        return clone

    def stat(self, *fields, **kwargs):
        """Adds stats.field and optionally stats.facet to query to return term
        frequency counts for listed fields"""
        clone = self._clone()
        clone.query.add_stats(*fields)
        if kwargs.get('facet', None):
            clone.query.add_stats_facets(*kwargs.get('facet'))
        return clone

    def annotate(self, **kwargs):
        clone = self._clone()
        clone.allow_non_model_fields = True
        clone.query.add_annotations(**kwargs)
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

    def _cache_response(self, results, updateonly=[]):
        if updateonly:
            if type(updateonly) not in (list, tuple, set):
                updateonly = [updateonly]
            for f in updateonly:
                setattr(self, f, results.get(f))
        else:
            self.docs = results.get('response', {})
            self.facet_counts = results.get('facet_counts', {})
            self.term_vectors = results.get('termVectors', [])
            self.stats = results.get('stats', {})

    def iterator(self):
        results = self.query.run()
        self._cache_response(self.query.run())

        for x in self.docs.get('docs', []):
            yield self.postprocess_result(x)

    def get_facet_counts(self):
        if not self.facet_counts:
            q = self.query.clone()
            q.set_limits(high=1)
            self._cache_response(q.run())
        ret = {}
        ff = self.facet_counts.get('facet_fields', {})
        for key in ff.keys():
            ret[key] = dict(zip(ff[key][::2], ff[key][1::2]))
        return ret

    def get_term_vectors(self):
        """
        extract term vector information
        this format will (hopefully) change as per [SOLR-2420]
        """
        if not self.term_vectors or len(self.term_vectors) != self.query.high_mark:
            q = self.query.clone()
            self._cache_response(q.run())
        ret = {}
        tvs = self.term_vectors[3::2]
        for tv in tvs:
            unique = tv[1]
            ret[unique] = {}
            for kvp in zip(tv[2::2], tv[3::2]):
                fieldname = kvp[0]
                ret[unique][fieldname] = dict(zip(kvp[1][::2], kvp[1][1::2]))
                for k in ret[unique][fieldname].keys():
                    x = ret[unique][fieldname][k]
                    ret[unique][fieldname][k] = dict(zip(x[::2], x[1::2]))
                    x = ret[unique][fieldname][k]
                    if x.get('offsets'):
                        x['offsets'] = dict(zip(x['offsets'][::2], x['offsets'][1::2]))
                    if x.get('positions'):
                        x['positions'] = dict(zip(x['positions'][::2], x['positions'][1::2]))
                    ret[unique][fieldname][k] = x
        return ret

    def get_stats(self, force=False):
        if not self.stats or force:
            q = self.query.clone()
            self._cache_response(q.run(), updateonly='stats')
        stats = {}
        for field in self.stats['stats_fields'].keys():
            try:
                stats[field] = Stats(field, **self.stats['stats_fields'][field])
            except:
                # there were no stats for the field
                pass
        return stats

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

    def _serialize(self):
        """
        def __init__(self, model, query=None, using=None, allow_non_model_fields=False):
        """
        sr = {
            'model': (self.model.__module__, self.model.__name__),
            'allow_non_model_fields': self.allow_non_model_fields,
            'query': self.query._serialize(),
        }
        return json.dumps(sr)

    @staticmethod
    def _from_serial(data):
        sr = json.loads(data)
        modulestring, modelstring = sr['model']
        model = getattr(import_module(modulestring), modelstring)
        query = SolrQuery._from_serial(sr['query'])
        anmf = sr['allow_non_model_fields']
        return BrushfireQuerySet(model, query=query, allow_non_model_fields=anmf)

################################################################################
#
#                              QuerySet subtypes
#
################################################################################
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

################################################################################
#
#                                   Helpers
#
################################################################################
class Stats(object):
    def __init__(self, name, min, max, count, missing, sum, sumOfSquares, mean, stddev, facets={}):
        self.name = name
        self.min = min
        self.max = max
        self.count = count
        self.missing = missing
        self.sum = sum
        self.sumsq = sumOfSquares
        self.mean = mean
        self.stddev = stddev
        self.facets = {}
        for facet_field in facets.keys():
            for facet in facets[facet_field].keys():
                if self.facets.get(facet_field, None):
                    self.facets[facet_field][facet] = Stats(facet, **facets[facet_field][facet])
                else:
                    self.facets[facet_field] = {}
                    self.facets[facet_field][facet] = Stats(facet, **facets[facet_field][facet])

