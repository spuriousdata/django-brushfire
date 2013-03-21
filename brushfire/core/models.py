from brushfire.core.query import BrushfireQuerySet
from django.db import models
from django.db.models.base import ModelBase, Model
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

    def __getattr__(self, name):
        """
        Automatically proxy all method calls to the queryset.

        Allows Model.objects.{filter,get,all,none,order_by,etc}() without explicitly
        defining them all
        """
        try:
            return self.__dict__[name]
        except KeyError:
            return getattr(self.get_query_set(), name)

class BrushfireModelBase(ModelBase):
    def __new__(cls, name, bases, attrs):
        new_class = super(BrushfireModelBase, cls).__new__(cls, name, bases, attrs)
        setattr(new_class, '_default_manager', BrushfireManager(new_class))
        setattr(new_class, '_base_manager', BrushfireManager(new_class))
        return new_class

class BrushfireModel(six.with_metaclass(BrushfireModelBase), Model):
    _deferred = False
    class Meta:
        abstract = True # Stop Django from creating a BrushfireModel table
        managed = False # Stop Django from creating tables for subclasses


################################################################################
#
#                                    Fields
#
################################################################################

class BooleanField(models.BooleanField):
    pass

# BinaryField

# Numeric Fields
class IntegerField(models.IntegerField):
    pass

class FloatField(models.FloatField):
    pass

class LongField(models.IntegerField):
    pass

class DoubleField(models.FloatField):
    pass

class TrieIntegerField(models.IntegerField):
    pass

class TrieFloatField(models.FloatField):
    pass

class TrieLongField(models.IntegerField):
    pass

class TrieDoubleField(models.FloatField):
    pass

# Date Fields
class DateField(models.DateTimeField):
    pass

class TrieDateField(models.DateTimeField):
    pass

# Character Fields
class TextField(models.TextField):
    pass

class CharField(models.CharField):
    def __init__(self, *args, **kwargs):
        kwargs['max_length'] = 0xFFFFFFFF if kwargs.get('max_length', None) is None else kwargs['max_length']
        super(CharField, self).__init__(*args, **kwargs)

class StringField(CharField):
    pass

class TextGeneralField(TextField):pass
class TextEnField(TextField):pass
class TextWsField(TextField):pass
class NgramField(TextField):pass
class EdgeNgramField(TextField):pass
