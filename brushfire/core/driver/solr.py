import httplib2
import json
import logging
from urllib import  urlencode as e
from urlparse import parse_qsl as d

URL_LENGTH_MAX = 1024
DEFAULT = 0xDEFA17

logger = logging.getLogger('brushfire.driver.solr')

class Url(object):
    def __init__(self, start, path, qs):
        self.start = start
        self.path = path
        self.qs = qs
    
    @property
    def fullurl(self):
        return self.start + self.path + self.qs

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
    def rightside(self):
        return self.path + self.qs

class SolrException(Exception):
    def __init__(self, msg, type="Unknown"):
        self.type = type
        super(SolrException, self).__init__(msg)

class SolrResponseException(SolrException):
    def __init__(self, msg):
        super(SolrResponseException, self).__init__(msg, "Response Error")

class Solr(object):
    def __init__(self, server, core='', 
                 query_handler='select', lparams='', cache=None):
        self.solr = server
        self.default_core = core
        self.conn = httplib2.Http(cache)
        self.query_handler = query_handler
        self.lparams = lparams

    def _url(self, path, query):
        path = path if path.startswith('/') else "/%s" % path
        qs = "?%s" % e(query) if len(query) else ''
        return Url(self.solr, path, qs)

    def _raw(self, path, **kwargs):
        url = self._url(path, kwargs)

        if len(url.rightside) > URL_LENGTH_MAX:
            logger.debug("Requesting %s with body: %s", url.urlpart, url.qspart)
            resp, content = self.conn.request(
                url.urlpart, method='POST', body=url.qspart)
        else:
            human_url = "&".join(["%s=%s" % (k,v) for k,v in d(url.fullurl)])
            logger.debug("Requesting %s", human_url)
            resp, content = self.conn.request(url.fullurl)

        if resp['status'] != '200':
            e = SolrException("Request returned status %d" % int(resp['status']))
            e.errorbody = content
            e.url = url.fullurl
            raise e

        return content

    def search(self, query, fields=DEFAULT, lparams=DEFAULT, 
               handler=DEFAULT, core=DEFAULT, raw=False, **kwargs):
        if handler == DEFAULT:
            handler = self.query_handler
        if core == DEFAULT:
            core = self.default_core
        if lparams == DEFAULT:
            lparams = self.lparams
        if fields == DEFAULT:
            fields = '*,score'

        path = "%s/%s" % (core, handler)
        q = {
            'q': lparams+query,
            'wt': 'json',
            'fl': fields,
        }
        q.update(kwargs)
        response = self._raw(path, **q)
        if raw:
            return response
        try:
            return json.loads(response)
        except ValueError as e:
            raise SolrResponseException("Error decoding JSON response from Solr, possible misconfiguration. Error: %s" % e.message)

if __name__ == '__main__':
    l = logging.getLogger('brushfire')
    l.setLevel(logging.DEBUG)
    l.addHandler(logging.StreamHandler())
    solr = Solr("http://localhost/solr")
    import pdb; pdb.set_trace() 
