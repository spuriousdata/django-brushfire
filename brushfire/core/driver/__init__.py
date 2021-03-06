import logging
import json
import copy

import six

from django.utils.tree import Node
from django.db.models import Q
from django.utils.importlib import import_module
try:
    from django.db.models.sql.constants import LOOKUP_SEP
except ImportError:
    from django.db.models.constants import LOOKUP_SEP

from brushfire.core.driver.solr import *
from brushfire.core.settings import configuration as conf
from brushfire.utils import smart_quote_string
from brushfire.core.types import FRange, GroupedFRange
from brushfire.core.exceptions import BrushfireException

QUERY_TERMS = set([
    'exact', 'contains', 'gt', 'gte', 'lt', 'lte', 'in',
    'startswith', 'endswith', 'like', 'range'])

logger = logging.getLogger('brushfire.driver.query')

class SearchNode(Node):
    AND = 'AND'
    OR = 'OR'
    default = AND
    
    """
    Like 90% of this class is stolen directly from the haystack project.
    
    Thanks!
    """
    
    # Start compat. Django 1.6 changed how ``tree.Node`` works, so we're going
    # to patch back in the original implementation until time to rewrite this
    # presents itself.
    # See https://github.com/django/django/commit/d3f00bd.

    def __init__(self, children=None, connector=None, negated=False):
        """
        Constructs a new Node. If no connector is given, the default will be
        used.

        Warning: You probably don't want to pass in the 'negated' parameter. It
        is NOT the same as constructing a node and calling negate() on the
        result.
        """
        self.children = children and children[:] or []
        self.connector = connector or self.default
        self.subtree_parents = []
        self.negated = negated
        self._optimized = True

    # We need this because of django.db.models.query_utils.Q. Q. __init__() is
    # problematic, but it is a natural Node subclass in all other respects.
    def _new_instance(cls, children=None, connector=None, negated=False):
        """
        This is called to create a new instance of this class when we need new
        Nodes (or subclasses) in the internal code in this class. Normally, it
        just shadows __init__(). However, subclasses with an __init__ signature
        that is not an extension of Node.__init__ might need to implement this
        method to allow a Node to create a new instance of them (if they have
        any extra setting up to do).
        """
        obj = SearchNode(children, connector, negated)
        obj.__class__ = cls
        return obj
    _new_instance = classmethod(_new_instance)

    def __str__(self):
        if self.negated:
            return '(NOT (%s: %s))' % (self.connector, ', '.join([str(c) for c
                    in self.children]))
        return '(%s: %s)' % (self.connector, ', '.join([str(c) for c in
                self.children]))

    def __deepcopy__(self, memodict):
        """
        Utility method used by copy.deepcopy().
        """
        obj = SearchNode(connector=self.connector, negated=self.negated)
        obj.__class__ = self.__class__
        obj.children = copy.deepcopy(self.children, memodict)
        obj.subtree_parents = copy.deepcopy(self.subtree_parents, memodict)
        return obj

    def __len__(self):
        """
        The size of a node if the number of children it has.
        """
        return len(self.children)

    def __bool__(self):
        """
        For truth value testing.
        """
        return bool(self.children)

    def __nonzero__(self):      # Python 2 compatibility
        return type(self).__bool__(self)

    def __contains__(self, other):
        """
        Returns True is 'other' is a direct child of this instance.
        """
        return other in self.children

    def add(self, node, conn_type):
        """
        Adds a new node to the tree. If the conn_type is the same as the root's
        current connector type, the node is added to the first level.
        Otherwise, the whole tree is pushed down one level and a new root
        connector is created, connecting the existing tree and the new node.
        """
        if node in self.children and conn_type == self.connector:
            return
        self._optimized = False
        if len(self.children) < 2:
            self.connector = conn_type
        if self.connector == conn_type:
            if isinstance(node, SearchNode) and (node.connector == conn_type or
                    len(node) == 1):
                self.children.extend(node.children)
            else:
                self.children.append(node)
        else:
            obj = self._new_instance(self.children, self.connector,
                    self.negated)
            self.connector = conn_type
            self.children = [obj, node]

    def negate(self):
        """
        Negate the sense of the root connector. This reorganises the children
        so that the current node has a single child: a negated node containing
        all the previous children. This slightly odd construction makes adding
        new children behave more intuitively.

        Interpreting the meaning of this negate is up to client code. This
        method is useful for implementing "not" arrangements.
        """
        self.children = [self._new_instance(self.children, self.connector,
                not self.negated)]
        self.connector = self.default

    def start_subtree(self, conn_type):
        """
        Sets up internal state so that new nodes are added to a subtree of the
        current node. The conn_type specifies how the sub-tree is joined to the
        existing children.
        """
        if len(self.children) == 1:
            self.connector = conn_type
        elif self.connector != conn_type:
            self.children = [self._new_instance(self.children, self.connector,
                    self.negated)]
            self.connector = conn_type
            self.negated = False

        self.subtree_parents.append(self.__class__(self.children,
                self.connector, self.negated))
        self.connector = self.default
        self.negated = False
        self.children = []

    def end_subtree(self):
        """
        Closes off the most recently unmatched start_subtree() call.

        This puts the current state into a node of the parent tree and returns
        the current instances state to be the parent.
        """
        obj = self.subtree_parents.pop()
        node = self.__class__(self.children, self.connector)
        self.connector = obj.connector
        self.negated = obj.negated
        self.children = obj.children
        self.children.append(node)
        
    def optimize(self):
        """
        This is a basic query optimizer. It's only current ability is to detect
        multiple filters or excludes acting on the same field at the same level
        in the node tree and combine them into an __in query. 
        eg. .exclude(foo='bar').exclude(foo='baz').exclude(foo='bak') becomes
            .exclude(foo__in=('bar', 'baz', 'bak')
        """
        if self._optimized:
            return
        new_fields = {}
        for c in self.children:
            if not isinstance(c, SearchNode):
                continue
            if len(c.children) == 1:
                k,v = c.children[0]
                if k.find("__") == -1:
                    new_field = k+"__in"
                    if new_fields.get(new_field, None) is None:
                        new_fields[new_field] = {
                            "negated": c.negated, 
                            "values": [v],
                            "maybe_remove": [c],
                        }
                    elif new_fields[new_field]['negated'] == c.negated:
                        new_fields[new_field]["values"].append(v)
                        new_fields[new_field]["maybe_remove"].append(c)
                    else:
                        """
                        This really shouldn't happen because you shouldn't be
                        making a query that says foo is xxx and foo is not yyy,
                        but just in case i'm crazy, we'll support it.
                        """
                        new_fields[new_field] = {
                            "negated": c.negated, 
                            "values": [v],
                            "maybe_remove": [c],
                        }
                elif k.endswith("__in"):
                    if new_fields.get(k, None) is None:
                       new_fields[k] = {
                           "negated": c.negated, 
                           "values": list(v),
                           "maybe_remove": [c],
                       }
                    elif new_fields[k]['negated'] == c.negated:
                        new_fields[k]['values'].extend(v)
                        new_fields[k]['maybe_remove'].append(c)
                    else:
                       """
                       see note for same case above.
                       """
                       new_fields[k] = {
                           "negated": c.negated, 
                           "values": list(v),
                           "maybe_remove": [c],
                       }
                    
            else:
                c._optimized = False
                c.optimize()
        
        for k,v in new_fields.items():
            if v['maybe_remove'] > 1:
                for c in v['maybe_remove']:
                    self.children.remove(c)
                if v['negated']:
                    self.add(~SQ(**{k:v['values']}), self.connector)
                else:
                    self.add(SQ(**{k:v['values']}), self.connector)
        
        self._optimized = True

    def as_query_string(self, query_fragment_callback):
        self.optimize()
        result = []
        for child in self.children:
            if hasattr(child, 'as_query_string'):
                result.append(child.as_query_string(query_fragment_callback))
            elif isinstance(child, six.string_types):
                result.append(query_fragment_callback(None, None, child))
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
    
    def _serialize(self):
        s = {
            'connector': self.connector,
            'negated': self.negated,
        }
        children = []
        for c in self.children:
            if isinstance(c, SearchNode):
                children.append(c._serialize())
            else:
                children.append(c)
        s['children'] = children
        return s

    @staticmethod
    def _from_serial(data):
        if isinstance(data, basestring):
            data = json.loads(data)
        s = SearchNode()
        s.connector = data['connector']
        s.negated = data['negated']
        for c in data['children']:
            if isinstance(c, dict):
                s.children.append(SearchNode._from_serial(c))
            else:
                s.children.append(c)
        return s

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
        self.stats = []
        self.stats_facets = []
        self.fields = ['*', 'score']
        self.extra_params = {}
        self.annotations = {}
        self.frange = []
        self.handler = conf.get('handlers.default')

    def _serialize(self):
        return {
            'model': (self.model.__module__, self.model.__name__),
            'low_mark': self.low_mark,
            'high_mark': self.high_mark,
            'where': self.where._serialize(),
            'fq': self.fq._serialize(),
            'ordering': self.ordering,
            'stats': self.stats,
            'stats_facets': self.stats_facets,
            'fields': self.fields,
            'extra_params': self.extra_params,
            'annotations': self.annotations,
            'frange': [x._serialize() for x in self.frange],
            'handler': self.handler,
        }

    @staticmethod
    def _from_serial(dct):
        if isinstance(dct, basestring):
            dct = json.loads(dct)
        s = SolrQuery()
        modulestring, modelstring = dct.pop('model')
        model = getattr(import_module(modulestring), modelstring)
        s.model = model
        s.where = SearchNode._from_serial(dct.pop('where'))
        s.fq = SearchNode._from_serial(dct.pop('fq'))
        if dct.get('frange', False):
            s.frange = []
            for x in dct.pop('frange'):
                if x.get('func', None) is not None:
                    s.frange.append(FRange(func=x['func'], l=x['l'], u=x['u']))
                elif x.get('franges', None) is not None:
                    s.frange.append(GroupedFRange._from_serial(x))
                else:
                    # This shouldn't happen
                    pass
        for k, v in dct.items():
            setattr(s, k, v)
        return s

    def set_fields(self, *fields):
        if len(fields) != 0:
            self.fields = list(fields) + ['score']
        else:
            self.fields = [x.name for x in self.model._meta.fields] + ['score']
            
    def add_fields(self, *fields):
        self.fields += list(fields)
            
    def set_handler(self, handler):
        self.handler = handler

    def clear_ordering(self):
        self.ordering = []
        return self

    def add_ordering(self, *order):
        self.ordering += order
        return self

    def clear_annotations(self):
        self.annotations = {}
        return self

    def add_annotations(self, **kwargs):
        for k,v in kwargs.items():
            self.annotations[k] = v
        return self
    
    def clear_frange(self):
        self.frange = []
        return self

    def add_frange(self, l, u, func):
        self.frange.append(FRange(l=l, u=u, func=func))
        return self
    
    def add_frange_group(self, frs):
        if not isinstance(frs, GroupedFRange):
            raise BrushfireException("Expected type(GroupedFRange) got type(%s)" % type(frs))
        self.frange.append(frs)
        return self

    def clear_facets(self):
        self.facets = []
        return self

    def add_facets(self, *fields):
        self.facets += fields
        return self

    def clear_stats(self):
        self.stats = []
        return self

    def add_stats(self, *fields):
        self.stats += fields
        return self

    def clear_stats_facets(self):
        self.stats_facets = []
        return self

    def add_stats_facets(self, *fields):
        self.stats_facets += fields
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
        q.stats = self.stats[:]
        q.stats_facets = self.stats_facets[:]
        q.fields = self.fields[:]
        q.extra_params = copy.deepcopy(self.extra_params)
        q.annotations = copy.deepcopy(self.annotations)
        q.frange = self.frange[:]
        q.handler = self.handler
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

        # XXX:
        # This is a shitty hack. Setting high=0 causes an infinite loop, but I
        # don't know why. You _should_ be able to use &rows=0 in solr to get
        # back metadata about the result set (like result count)
        if high < 1:
            high = 1
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
        if not qs and property != 'fq': # Don't add fq=*:*
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
        """
        Return the solr query params, excluding ?q= and ?fq=
        """
        p = {
            'start':self.start(), 
            'rows':self.rows(), 
            'fields':','.join(self.fields), 
            'sort':self.ordering,
            'facet':self.facets,
            'stats':self.stats,
            'stats_facets':self.stats_facets,
            'annotations':self.annotations,
            'frange':self.frange,
            'handler':self.handler,
        }
        p.update(self.extra_params)
        return p

    def start(self):
        return self.low_mark or 0

    def rows(self):
        if self.high_mark:
            minrows = self.high_mark
        # If you only use narrow() and no filter(), this will set the rows to
        # 10, which most likely is not what we want
        #elif self.get_querystring() == "*:*":
        #    minrows = 10
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

        # This is what was causing the multiple quotes around the first two
        # items of a list issue
        """
        if type(value) in (set, list, tuple):
            if value[0].find(' ') != -1:
                value[0] = '"%s"' % value[0]

            if value[1].find(' ') != -1:
                value[1] = '"%s"' % value[1]
        """
        if type(value) not in (set, list, tuple):
            if type(value) not in (unicode, str):
                value = str(value)
            # quote a single string value
            if value.find(' ') != -1:
                value = smart_quote_string(value)

        if field is None and filter_type is None:
            fragment = value
        elif filter_type not in ('in', 'range'):
            fragment = "%s:%s" % (field, filters[filter_type] % value)
        elif filter_type == 'in':
            if type(value) not in (list, tuple):
                value = [value]
            # quote a multi-string value
            fragment = "%s:(%s)" % (field, " OR ".join([smart_quote_string(x) for x in value]))
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

        where.add(q, connector)
        """
        if where and ((q.connector != connector and len(q) > 1) or where.negated != q.negated ):
            where.start_subtree(connector)
            subtree = True
        else:
            subtree = False

        for child in q.children:
            if isinstance(child, Node):
                where.start_subtree(connector)
                self.add_q(child, property=property)
                where.end_subtree()
            elif isinstance(child, six.string_types):
                where.add(child, connector)
            else:
                expression, value = child
                where.add((expression, value), connector)
            connector = q.connector

        if q.negated:
            where.negate()

        if subtree:
            where.end_subtree()
        """
