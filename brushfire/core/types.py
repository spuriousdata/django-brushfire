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
