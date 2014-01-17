from django.core.exceptions import ImproperlyConfigured

class BrushfireException(Exception):
    pass

class BrushfireConfigException(ImproperlyConfigured):
    pass
