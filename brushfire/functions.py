def field(x):
    return "field(%s)" % str(x).encode('utf-8')

def ord(x):
    return "ord(%s)" % str(x).encode('utf-8')

def rord(x):
    return "rord(%s)" % str(x).encode('utf-8')

def sum(*args):
    return "sum(%s)" % ','.join([str(x).encode('utf-8') for x in args])

def sub(x, y):
    return "sub(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def div(x, y):
    return "div(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def mul(x, y):
    return "mul(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def mod(x, y):
    return "mod(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def pow(x, y):
    return "pow(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def abs(x):
    return "abs(%s)" % str(x).encode('utf-8')

def log(x):
    return "log(%s)" % str(x).encode('utf-8')

def sqrt(x):
    return "sqrt(%s)" % str(x).encode('utf-8')

def map(*args):
    return "map(%s)" % ','.join([str(x).encode('utf-8') for x in args])

def currency(*args):
    return "currency(%s)" % ','.join([str(x).encode('utf-8') for x in args])

def linear(x, m, c):
    return "linear(%s,%s,%s)" % (str(x).encode('utf-8'), str(m).encode('utf-8'), str(c).encode('utf-8'))

def recip(x, m, a, b):
    return "recip(%s,%s,%s)" % (str(x).encode('utf-8'), str(m).encode('utf-8'), str(a).encode('utf-8'), str(b).encode('utf-8'))

def max(*args):
    return "max(%s)" % ','.join([str(x).encode('utf-8') for x in args])

def min(x, y):
    return "min(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def ms(*args):
    if len(args) == 0:
        args = "",
    return "ms(%s)" % ','.join([str(x).encode('utf-8') for x in args])

def rad(x):
    return "rad(%s)" % str(x).encode('utf-8')

def deg(x):
    return "deg(%s)" % str(x).encode('utf-8')

def cbrt(x):
    return "cbrt(%s)" % str(x).encode('utf-8')

def ln(x):
    return "ln(%s)" % str(x).encode('utf-8')

def exp(x):
    return "exp(%s)" % str(x).encode('utf-8')

def sin(x):
    return "sin(%s)" % str(x).encode('utf-8')

def cos(x):
    return "cos(%s)" % str(x).encode('utf-8')

def tan(x):
    return "tan(%s)" % str(x).encode('utf-8')

def asin(x):
    return "asin(%s)" % str(x).encode('utf-8')

def acos(x):
    return "acos(%s)" % str(x).encode('utf-8')

def atan(x):
    return "atan(%s)" % str(x).encode('utf-8')

def sinh(x):
    return "sinh(%s)" % str(x).encode('utf-8')

def cosh(x):
    return "cosh(%s)" % str(x).encode('utf-8')

def tanh(x):
    return "tanh(%s)" % str(x).encode('utf-8')

def ceil(x):
    return "ceil(%s)" % str(x).encode('utf-8')

def floor(x):
    return "floor(%s)" % str(x).encode('utf-8')

def rint(x):
    return "rint(%s)" % str(x).encode('utf-8')

def hypo(x,y):
    return "hypo(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def atan2(x,y):
    return "atan2(%s,%s)" % (str(x).encode('utf-8'), str(y).encode('utf-8'))

def pi():
    return "pi()"

def e():
    return "e()"

def docfreq(field, word):
    w = '"%s"' % word.replace('"', r'\"')
    return 'docfreq(%s, %s)' % (field, w)

def termfreq(field, word):
    w = '"%s"' % word.replace('"', r'\"')
    return 'termfreq(%s, %s)' % (field, w) 

def totaltermfreq(field, word):
    w = '"%s"' % word.replace('"', r'\"')
    return 'totaltermfreq(%s, %s)' % (field, w)

def ttf(*args):
    w = '"%s"' % word.replace('"', r'\"')
    return 'ttf(%s, %s)' % (field, w)

def sumtotaltermfreq(field, word):
    w = '"%s"' % word.replace('"', r'\"')
    return 'sumtotaltermfreq(%s, %s)' % (field, w)

def sttf(*args):
    w = '"%s"' % word.replace('"', r'\"')
    return 'sttf(%s, %s)' % (field, w)

def idf(field, word):
    w = '"%s"' % word.replace('"', r'\"')
    return 'idf(%s, %s)' % (field, w)

def tf(field, word):
    w = '"%s"' % word.replace('"', r'\"')
    return 'tf(%s, %s)' % (field, w)

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
