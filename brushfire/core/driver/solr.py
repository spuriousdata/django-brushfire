import requests
import logging
import re
from urllib import  urlencode as e
from urlparse import parse_qsl as d

URL_LENGTH_MAX = 1024
DEFAULT = 0xDEFA17

logger = logging.getLogger('brushfire.driver.solr')

class Url(object):
    def __init__(self, start, path, params=()):
        self.start = start
        self.path = path
        self.params = params
        self.qs = e(params) if len(params) else ''
    
    @property
    def fullurl(self):
        qs = ""
        if self.qs:
            qs = "?%s" % self.qs
        return self.start + self.path + qs

    @property
    def urlpart(self):
        return self.start + self.path

    @property
    def hostpart(self):
        return self.start

    @property
    def pathpart(self):
        return self.path

    @property
    def qspart(self):
        return self.qs
    
    @property
    def query_params(self):
        return self.params

    @property
    def rightside(self):
        qs = ""
        if self.qs:
            qs = "?%s" % self.qs
        return self.path + qs

    @property
    def pretty_qspart(self):
        return "&".join(["%s=%s" % (k,v) for k,v in d(self.qspart)])

    def humanize(self):
        return self.urlpart + "?" + self.pretty_qspart

    def __repr__(self):
        return self.humanize()

    def __str__(self):
        return self.__repr__()

    def __unicode__(self):
        return self.__repr__()

class SolrException(Exception):
    def __init__(self, msg, type="Unknown"):
        self.type = type
        super(SolrException, self).__init__(msg)

class SolrResponseException(SolrException):
    def __init__(self, msg):
        super(SolrResponseException, self).__init__(msg, "Response Error")

class Solr(object):
    sort_regex = re.compile('(\+|-)?(.*)')
    def __init__(self, server, core='', query_handler='select', lparams='',
            cache=None, fields='*,score', rows=20):
        self.solr = server
        self.default_core = core
        self.cache = cache
        self.query_handler = query_handler
        self.lparams = lparams
        self.fields = fields
        self.rows = rows

    def _url(self, path, query):
        path = path if path.startswith('/') else "/%s" % path
        query_params = []
        if query.get('stats'):
            sfl = query.pop('stats.fields', [])
            sft = query.pop('stats.facets', [])
            query_params += [('stats.field',x) for x in sfl]
            query_params += [('stats.facet',x) for x in sft]
        if query.get('facet'):
            ff = query.pop('facet.fields')
            query_params += [('facet.field',x) for x in ff]
        if query.get('frange') or type(query.get('frange')) in (list, tuple):
            fr = query.pop('frange')
            query_params += [('fq',str(x)) for x in fr]
        if isinstance(query.get('annotations'), dict):
            ann = query.pop('annotations')
            if len(ann):
                if query.get('fl'):
                    query['fl'] += ',' + \
                            ','.join(
                                ["%s:%s" % (x[0],x[1]) for x in ann.items()])
                else:
                    query['fl'] = self.fields + \
                            ',' + \
                            ','.join(
                                ["%s:%s" % (x[0],x[1]) for x in ann.items()])
        # convert True/False to strings
        tmp = {}
        for k,v in query.items():
            if type(v) is bool:
                v = "true" if v else "false"
            if v != '' and v is not None:
                tmp[k] = v
        query_params += list(tmp.items())
        # End convert True/False
        return Url(self.solr, path, query_params)


    def _raw(self, path, **kwargs):
        url = self._url(path, kwargs)

        if len(url.rightside) > URL_LENGTH_MAX:
            logger.debug("Requesting[POST] %s with body: %s", url.urlpart, url.pretty_qspart)
            resp = requests.post(url.urlpart, data=url.query_params, headers={'content-type': 'application/x-www-form-urlencoded'})
            if resp.status_code != 200:
                logger.debug("Method: POST")
                logger.debug("url: %s", resp.url)
                logger.error("Error[%d]: url: %s", resp.status_code, resp.url)
                try:
                    raise SolrException("Request returned status[%d]: %r" % (resp.status_code, resp.json()))
                except ValueError:
                    raise SolrException("Request returned status[%d]: %s" % (resp.status_code, resp.content))
        else:
            logger.debug("Requesting[GET] %s", url)
            resp = requests.get(url.urlpart, params=url.query_params)

        if resp.status_code != 200:
            e = SolrException("Request returned status[%d]: %s" % (resp.status_code, resp.content))
            logger.error("Error[%d]: url: %s", resp.status_code, resp.url)
            logger.exception(e)
            raise e

        return resp

    def search(self, query, fields=DEFAULT, lparams=DEFAULT, 
               handler=DEFAULT, core=DEFAULT, start=0, rows=DEFAULT, raw=False, 
               sort=[], facet=[], fq=None, frange=[], stats=[], stats_facets=[], **kwargs):
        if handler == DEFAULT:
            handler = self.query_handler
        if core == DEFAULT:
            core = self.default_core
        if lparams == DEFAULT:
            lparams = self.lparams
        if fields == DEFAULT:
            fields = self.fields
        if rows == DEFAULT:
            rows = self.rows

        # turn ['+foo', '-bar', 'baz'] into "foo asc,bar desc,baz asc"
        sort = ','.join(
                ["%s asc" % field if direction in (None, '+') else "%s desc" % field 
                    for direction, field in [
                        Solr.sort_regex.search(x).groups() for x in sort]])

        path = "%s/%s" % (core, handler)
        f_query = ""
        if lparams.find("$$QUERY$$") != -1:
            f_query = lparams.replace("$$QUERY$$", query)
        else:
            f_query = lparams+query
        q = {
            'q': f_query,
            'wt': 'json', # Should this be overridable?
            'fl': fields,
            'rows': rows,
            'start': start,
            'sort': sort,
            'frange': frange,
        }
        if facet:
            q.update({
                'facet':'on',
                'facet.fields':facet,
            })
        if stats:
            q.update({
                'stats':'on',
                'stats.fields':stats,
            })
            if stats_facets: # this wont' do anything without stats.fields
                q.update({
                    'stats.facets':stats_facets,
                })
        if fq:
            q['fq'] = fq

        q.update(kwargs)
        try:
            response = self._raw(path, **q)
        except Exception as e:
            logger.exception(e)
            raise 
        
        if raw:
            return response.content
        try:
            return response.json()
        except ValueError as e:
            raise SolrResponseException("Error decoding JSON response from Solr, "\
                    "possible misconfiguration. Content: %s" % response.content)

if __name__ == '__main__':
    l = logging.getLogger('brushfire')
    l.setLevel(logging.DEBUG)
    l.addHandler(logging.StreamHandler())
    solr = Solr("http://localhost/solr")
    import pdb; pdb.set_trace() 
