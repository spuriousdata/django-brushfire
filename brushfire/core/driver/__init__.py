import logging
import copy
from django.utils.tree import Node
from django.db.models import Q
try:
    from django.db.models.sql.constants import LOOKUP_SEP
except ImportError:
    from django.db.models.constants import LOOKUP_SEP
from brushfire.core.driver.solr import *
from brushfire.core.settings import configuration as conf

QUERY_TERMS = set([
    'exact', 'contains', 'gt', 'gte', 'lt', 'lte', 'in',
    'startswith', 'endswith', 'like', 'range'])

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
        self.fq = SearchNode()
        self.ordering = []
        self.facets = []
        self.fields = ['*', 'score']
        self.extra_params = {}

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

    def clear_facets(self):
        self.facets = []
        return self

    def add_facets(self, *fields):
        self.facets += fields
        return self

    def clear_extra_params(self):
        self.extra_params = {}
        return self

    def add_extra_params(self, d):
        self.extra_params.update(d)

    def clone(self):
        q = SolrQuery(self.model)
        q.low_mark = self.low_mark
        q.high_mark = self.high_mark
        q.where = copy.deepcopy(self.where)
        q.fq = copy.deepcopy(self.fq)
        q.ordering = self.ordering[:]
        q.facets = self.facets[:]
        q.fields = self.fields[:]
        q.extra_params = copy.deepcopy(self.extra_params)
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

    def default_search(self, q):
        self.where = q

    def get_querystring(self, property='where'):
        where = getattr(self, property)
        if isinstance(where, SearchNode):
            qs = where.as_query_string(self.build_query_fragment)
        elif isinstance(where, basestring):
            qs = where
        if not qs:
            qs = "*:*"
        return qs

    def get_full_query(self):
        """
        debug use only
        """
        return "q=" + self.get_querystring() + '&fq=' + \
                    self.get_querystring(property='fq') + '&' + '&'.join(
                        ["%s=%s" % (k,v) for k,v in self.get_query_params().items()])

    def get_query_params(self):
        p = {
            'start':self.start(), 
            'rows':self.rows(), 
            'fields':','.join(self.fields), 
            'sort':self.ordering,
            'facet':self.facets,
        }
        p.update(self.extra_params)
        return p

    def start(self):
        return self.low_mark or 0

    def rows(self):
        if self.high_mark:
            minrows = self.high_mark
        elif self.get_querystring() == "*:*":
            minrows = 10
        else:
            minrows = self.get_count()
        return minrows - self.start()

    def run(self):
        logging.debug("running")
        return conf.solr_connection.search(
            self.get_querystring(),
            fq=self.get_querystring(property='fq'),
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
            'like': u'*%s*',
        }

        if type(value) in (set, list, tuple) and len(value) == 1:
            value = value[0]

        if type(value) in (set, list, tuple):
            if value[0].find(' ') != -1:
                value[0] = '"%s"' % value[0]

            if value[1].find(' ') != -1:
                value[1] = '"%s"' % value[1]
        else:
            if value.find(' ') != -1:
                value = '"%s"' % value

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
        clone.set_limits(high=1) # This must be > 0 -- or self.rows() will infinite loop
        try:
            count = int(clone.run()['response']['numFound'])
        except:
            count = 0
        return count

    def add_q(self, q, connector=SQ.AND, property='where'):

        where = getattr(self, property)

        if not isinstance(where, SearchNode):
            where = SearchNode()

        if where and q.connector != connector and len(q) > 1:
            where.start_subtree(connector)
            subtree = True
        else:
            subtree = False

        for child in q.children:
            if isinstance(child, Node):
                where.start_subtree(connector)
                self.add_q(child, property=property)
                where.end_subtree()
            else:
                expression, value = child
                where.add((expression, value), connector)
            connector = q.connector

        if q.negated:
            where.negate()

        if subtree:
            where.end_subtree()
