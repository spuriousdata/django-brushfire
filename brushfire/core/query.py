from brushfire import solr
from brushfire.core.driver.solr import SolrQuery
from brushfire.core.settings import configuration as conf
from django.db.models.query import QuerySet
from django.db.models import Q
from django.db.models.sql.constants import LOOKUP_SEP, QUERY_TERMS
from django.utils.tree import Node

import logging
logger = logging.getLogger('brushfire.core.query')

class SearchNode(Node):
    AND = 'AND'
    OR = 'OR'
    default = AND

    def as_query_string(self, query_fragment_callback):
        result = []
        for child in self.children:
            if hasattr(child, 'as_query_string'):
                result.append(child.as_query_string(query_fragment_callback))
            else:
                expression, value = child
                field, filter_type = self.split_expression(expression)
                result.append(query_fragment_callback(field, filter_type, value))
        conn = ' %s ' % self.connector
        query_string = conn.join(results)
        if query_string:
            if self.negated:
                query_string = 'NOT (%s)' % query_string
            elif len(self.children) != 1:
                query_string = '(%s)' % query_string
        return query_string

    def split_expression(self, expression):
        parts = expression.split(LOOKUP_SEP)
        field = parts[0]
        if len(pargs) == 1 or parts[-1] not in QUERY_TERMS:
            filter_type = 'contains'
        else:
            filter_type = parts.pop()
        return (field, filter_type)

class SQ(Q, SearchNode):
    pass

class BrushfireQuerySet(QuerySet):
    def __init__(self, model=None, query=None, using=None):
        super(BrushfireQuerySet, self).__init__(model, query, using)
        self.query = query or SolrQuery("*:*")

    def order_by(self, *fields):
        assert self.query.can_filter(), \
                "Cannot filter a query once a slice has been taken."
        clone = self._clone()
        clone.query.clear_ordering(force_empty=False)
        clone.query.add_ordering(*fields)
        return clone

    def iterator(self):
        results = solr.search(self.query.get_querystring()).get('response', {})
        for x in results['docs']:
            obj = self.model()
            for k,v in x.items():
                setattr(obj, k, v)
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
