from django.core.exceptions import ImproperlyConfigured

class BrushfireExcpetion(Exception):
    pass

class BrushfireConfigException(ImproperlyConfigured):
    pass
