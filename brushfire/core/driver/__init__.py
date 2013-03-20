import logging
from django.utils.tree import Node
from django.db.models import Q
from django.db.models.sql.constants import LOOKUP_SEP
from brushfire.core.driver.solr import *

QUERY_TERMS = set([
    'exact', 'contains', 'gt', 'gte', 'lt', 'lte', 'in',
    'startswith', 'endswith', 'range'])

logger = logging.getLogger('brushfire.driver.query')

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
        query_string = conn.join(result)
        if query_string:
            if self.negated:
                query_string = 'NOT (%s)' % query_string
            elif len(self.children) != 1:
                query_string = '(%s)' % query_string
        return query_string

    def split_expression(self, expression):
        parts = expression.split(LOOKUP_SEP)
        field = parts[0]
        if len(parts) == 1 or parts[-1] not in QUERY_TERMS:
            filter_type = 'contains'
        else:
            filter_type = parts.pop()
        return (field, filter_type)

class SQ(Q, SearchNode):
    pass

class SolrQuery(object):
    def __init__(self, model=None):
        self.model = model
        self.query_terms = []
        self.start = None
        self.stop = None
        self.where = SearchNode()

    def clone(self):
        q = SolrQuery(self.model)
        q.query_terms = self.query_terms
        q.start = self.start
        q.stop = self.stop
        q.where = self.where
        return q

    def set_limits(self, start, stop):
        logger.debug("Called Query.set_limits(%r, %r)", start, stop)
        self.start = start
        self.stop = stop

    def get_querystring(self):
        qs = self.where.as_query_string(self.build_query_fragment)
        if not qs:
            qs = "*:*"
        return qs

    def build_query_fragment(self, field, filter_type, value):
        fragment = ''

        filters = {
            'exact': u'%s',
            'contains': u'%s',
            'gt': u'{%s TO *}',
            'lt': u'{* TO %s}',
            'gte': u'[%s TO *]',
            'lte': u'[* TO %s]',
            'startswith': u'%s*',
            'endswith': u'*%s',
        }
        
        if type(value) in (set, list, tuple) and len(value) == 1:
            value = value[0]

        if filter_type not in ('in', 'range'):
            fragment = "%s:%s" % (field, filters[filter_type] % value)
        elif filter_type == 'in':
            fragment = "%s:(%s)" % (field, " OR ".join(value))
        elif filter_type == 'range':
            fragment = '%s:["%s" TO "%s"]' % (field, value[0], value[1])
        return fragment

    def can_filter(self):
        return True

    def add_q(self, q, connector=SQ.AND):

        if self.where and q.connector != connector and len(q) > 1:
            self.where.start_subtree(connector)
            subtree = True
        else:
            subtree = False

        for child in q.children:
            if isinstance(child, Node):
                self.where.start_subtree(connector)
                self.add_q(child)
                self.where.end_subtree()
            else:
                expression, value = child
                self.where.add((expression, value), connector)
            connector = q.connector

        if q.negated:
            self.where.negate()

        if subtree:
            self.where.end_subtree()
