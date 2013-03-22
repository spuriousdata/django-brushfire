import logging
import copy
from django.utils.tree import Node
from django.db.models import Q
from django.db.models.sql.constants import LOOKUP_SEP
from brushfire.core.driver.solr import *
from brushfire.core.settings import configuration as conf

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
                query_string = '-(%s)' % query_string # this doesn't work right (QS.exclude())
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
        self.low_mark = 0
        self.high_mark = None
        self.where = SearchNode()
        self.ordering = []
        self.fields = ['*', 'score']

    def set_fields(self, *fields):
        if len(fields) != 0:
            self.fields = list(fields) + ['score']
        else:
            self.fields = [x.name for x in self.model._meta.fields] + ['score']

    def clear_ordering(self):
        self.ordering = []
        return self

    def add_ordering(self, *order):
        self.ordering += order
        return self

    def clone(self):
        q = SolrQuery(self.model)
        q.low_mark = self.low_mark
        q.high_mark = self.high_mark
        q.where = copy.deepcopy(self.where)
        q.ordering = self.ordering[:]
        q.fields = self.fields[:]
        return q

    def set_limits(self, low=None, high=None):
        """
        Adjusts the limits on the rows retrieved. We use low/high to set these,
        as it makes it more Pythonic to read and write. When the SQL query is
        created, they are converted to the appropriate offset and limit values.

        Any limits passed in here are applied relative to the existing
        constraints. So low is added to the current low value and both will be
        clamped to any existing high value.
        """
        logger.debug("Called set_limits(%r, %r)", low, high)
        if high is not None:
            if self.high_mark is not None:
                self.high_mark = min(self.high_mark, self.low_mark + high)
            else:
                self.high_mark = self.low_mark + high
        if low is not None:
            if self.high_mark is not None:
                self.low_mark = min(self.high_mark, self.low_mark + low)
            else:
                self.low_mark = self.low_mark + low


    def get_querystring(self):
        qs = self.where.as_query_string(self.build_query_fragment)
        if not qs:
            qs = "*:*"
        return qs

    def get_full_query(self):
        """
        debug use only
        """
        return "q=" + self.get_querystring() + '&' + '&'.join(
                ["%s=%s" % (k,v) for k,v in self.get_query_params().items()])

    def get_query_params(self):
        return {
            'start':self.start(), 
            'rows':self.rows(), 
            'fields':','.join(self.fields), 
            'sort':self.ordering,
        }

    def start(self):
        return self.low_mark or 0

    def rows(self):
        return (self.high_mark or 10) - self.start()

    def run(self):
        logging.debug("running")
        return conf.solr_connection.search(
            self.get_querystring(),
            **self.get_query_params()
        )

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
        return not self.low_mark and self.high_mark is None

    def clear_limits(self):
        self.low_mark, self.high_mark = 0, None

    def prepare(self):
        return self

    def get_meta(self):
        return self.model._meta

    def get_count(self, *args, **kwargs):
        clone = self.clone()
        try:
            count = int(clone.run()['response']['numFound'])
        except:
            count = 0
        return count

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
