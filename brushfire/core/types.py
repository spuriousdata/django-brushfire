import json

class FRange(object):
    def __init__(self, func, l=None, u=None):
        assert l is not None or u is not None, \
                "At least one of l or u is required"
        self.l = l
        self.u = u
        self.func = func
        
    def __unicode__(self):
        s = u"{!frange "
        s += "l=%s " % str(self.l) if self.l else ""
        s += "u=%s" % str(self.u) if self.u else ""
        s += "}%s" % self.func
        return s
    
    def __str__(self):
        return unicode(self).encode('utf-8')
    
    def _serialize(self):
        return self.__dict__
    
class GroupedFRange(object):
    def __init__(self, franges=[], connector='OR'):
        self.connector = connector
        self.franges = franges
        
    def __unicode__(self):
        connector = u" %s " % self.connector
        s = u"("
        s += connector.join([unicode(x) for x in self.franges])
        s += u")"
        return s
    
    def __str__(self):
        return unicode(self).encode('utf-8')
    
    def _serialize(self):
        return {
            'connector': self.connector,
            'franges': [x._serialize() for x in self.franges],
        }
        
    def add_frange(self, func, l=None, u=None):
        self.franges.append(FRange(func, l=l, u=u))
    
    @staticmethod
    def _from_serial(data):
        if isinstance(data, basestring):
            data = json.loads(data)
        s = GroupedFRange(connector=data['connector'])
        for f in data['franges']:
            s.add_frange(f['func'], l=f['l'], u=f['u'])
        return s
