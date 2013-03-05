from brushfire.core.query import BrushfireQuerySet
from django.db.models.base import ModelBase
from django.utils import six

import logging
logger = logging.getLogger('brushfire.core')

class BrushfireManager(object):
    use_for_related_fields = True
    def __init__(self, model):
        logging.debug("Configuring BrushfireManager for model %s", model)
        super(BrushfireManager, self).__init__()
        self.model = model
        setattr(model, 'objects', self)

    def get_query_set(self):
        logger.debug("Called get_query_set(), returning BrushfireQuerySet(self.model)")
        return BrushfireQuerySet(self.model)

    def filter(self, *args, **kwargs):
        return self.get_query_set().filter(*args, **kwargs)

    def all(self, *args, **kwargs):
        return self.get_query_set().all(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.get_query_set().get(*args, **kwargs)

    def values(self, *args, **kwargs):
        return self.get_query_set().values(*args, **kwargs)

    def values_list(self, *args, **kwargs):
        return self.get_query_set().values_list(*args, **kwargs)

class BrushfireModelBase(ModelBase):
    def __new__(cls, name, bases, attrs):
        new_class = super(BrushfireModelBase, cls).__new__(cls, name, bases, attrs)
        setattr(new_class, '_default_manager', BrushfireManager(new_class))
        setattr(new_class, '_base_manager', BrushfireManager(new_class))
        return new_class

class BrushfireModel(six.with_metaclass(BrushfireModelBase)):
    _deferred = False
    class Meta:
        abstract = True

class BrushfireField(object):
    def __init__(self, search_name=None):
        self.search_name = search_name

class TextField(BrushfireField):pass
class DateTimeField(BrushfireField):pass
class IntegerField(BrushfireField):pass
class FloatField(BrushfireField):pass
class BooleanField(BrushfireField):pass
