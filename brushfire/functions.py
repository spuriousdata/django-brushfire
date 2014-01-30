def field(x):
    return "field(%s)" % str(x)

def ord(x):
    return "ord(%s)" % str(x)

def rord(x):
    return "rord(%s)" % str(x)

def sum(*args):
    return "sum(%s)" % ','.join([str(x) for x in args])

def sub(x, y):
    return "sub(%s,%s)" % (str(x), str(y))

def div(x, y):
    return "div(%s,%s)" % (str(x), str(y))

def mul(x, y):
    return "mul(%s,%s)" % (str(x), str(y))

def mod(x, y):
    return "mod(%s,%s)" % (str(x), str(y))

def pow(x, y):
    return "pow(%s,%s)" % (str(x), str(y))

def abs(x):
    return "abs(%s)" % str(x)

def log(x):
    return "log(%s)" % str(x)

def sqrt(x):
    return "sqrt(%s)" % str(x)

def map(*args):
    return "map(%s)" % ','.join([str(x) for x in args])

def currency(*args):
    return "currency(%s)" % ','.join([str(x) for x in args])

def linear(x, m, c):
    return "linear(%s,%s,%s)" % (str(x), str(m), str(c))

def recip(x, m, a, b):
    return "recip(%s,%s,%s)" % (str(x), str(m), str(a), str(b))

def max(x, c):
    return "max(%s,%s)" % (str(x), str(c))

def min(x, y):
    return "min(%s,%s)" % (str(x), str(y))

def ms(*args):
    if len(args) == 0:
        args = "",
    return "ms(%s)" % ','.join([str(x) for x in args])

def rad(x):
    return "rad(%s)" % str(x)

def deg(x):
    return "deg(%s)" % str(x)

def cbrt(x):
    return "cbrt(%s)" % str(x)

def ln(x):
    return "ln(%s)" % str(x)

def exp(x):
    return "exp(%s)" % str(x)

def sin(x):
    return "sin(%s)" % str(x)

def cos(x):
    return "cos(%s)" % str(x)

def tan(x):
    return "tan(%s)" % str(x)

def asin(x):
    return "asin(%s)" % str(x)

def acos(x):
    return "acos(%s)" % str(x)

def atan(x):
    return "atan(%s)" % str(x)

def sinh(x):
    return "sinh(%s)" % str(x)

def cosh(x):
    return "cosh(%s)" % str(x)

def tanh(x):
    return "tanh(%s)" % str(x)

def ceil(x):
    return "ceil(%s)" % str(x)

def floor(x):
    return "floor(%s)" % str(x)

def rint(x):
    return "rint(%s)" % str(x)

def hypo(x,y):
    return "hypo(%s,%s)" % (str(x), str(y))

def atan2(x,y):
    return "atan2(%s,%s)" % (str(x), str(y))

def pi():
    return "pi()"

def e():
    return "e()"

def docfreq(field, word):
    return 'docfreq(%s, %s)' % (field, repr(word))

def termfreq(field, word):
    return 'termfreq(%s, %s)' % (field, repr(word)) 

def totaltermfreq(field, word):
    return 'totaltermfreq(%s, %s)' % (field, repr(word))

def ttf(*args):
    return 'ttf(%s, %s)' % (field, repr(word))

def sumtotaltermfreq(field, word):
    return 'sumtotaltermfreq(%s, %s)' % (field, repr(word))

def sttf(*args):
    return 'sttf(%s, %s)' % (field, repr(word))

def idf(field, word):
    return 'idf(%s, %s)' % (field, repr(word))

def tf(field, word):
    return 'tf(%s, %s)' % (field, repr(word))

def norm(field):
    return 'norm(%s)' % field

def maxdoc():
    return 'maxdoc()'

def numdocs():
    return 'numdocs()'

def true():
    return 'true()'

def false():
    return 'false()'

def exists(x):
    return 'exists(%s)' % x

def _if(exp, tv, fv):
    return 'if(%s,%s,%s)' % (exp, tv, fv)

def _def(f, dv):
    return 'def(%s,%s)' % (f, dv)

def _not(f):
    return 'not(%s)' % f

def _and(x, y):
    return 'and(%s,%s)' % (x, y)

def _or(x, y):
    return 'or(%s,%s)' % (x, y)
