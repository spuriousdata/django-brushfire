from itertools import chain
import inspect
from brushfire.core.query import BrushfireQuerySet
from brushfire.core.exceptions import *
from django.apps import apps
from django.db import models
from django.db.models.base import subclass_exception
from django.db.models.options import Options
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.utils import six

import logging
logger = logging.getLogger('brushfire.core')

################################################################################
#
#                                    Fields
#
################################################################################

class BooleanField(models.BooleanField):
    pass

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

################################################################################
#
#                                   Classes
#
################################################################################
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

class BrushfireModelBase(type):
    def __new__(cls, name, bases, attrs):
        new_class = super(BrushfireModelBase, cls).__new__(cls, name, bases, attrs)
        
        parents = [b for b in bases if isinstance(b, BrushfireModelBase)]

        module = attrs.pop('__module__')
        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta
            
        app_config = apps.get_containing_app_config(module)
        setattr(new_class, 'objects', BrushfireManager(new_class))
        
        if getattr(meta, 'app_label', None):
            label = meta.app_label
        elif app_config:
            label = app_config.label
        else:
            label = '__NONE__'
            
        new_class.add_to_class('_meta', Options(meta, 
                **{'app_label':label}))
            
        new_class.add_to_class(
                'DoesNotExist',
                subclass_exception(
                    str('DoesNotExist'),
                    (ObjectDoesNotExist,),
                    module,
                    attached_to=new_class))
        new_class.add_to_class(
                'MultipleObjectsReturned',
                subclass_exception(
                    str('MultipleObjectsReturned'),
                    (MultipleObjectsReturned,),
                    module,
                    attached_to=new_class))

        if new_class._meta.proxy:
            raise BrushfireException, "BrushfireModels proxies not allowed."
                
        # add attributes to class
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)
                
        new_fields = chain(
                new_class._meta.local_fields,
                new_class._meta.local_many_to_many,
                new_class._meta.virtual_fields
        )
        field_names =  {f.name for f in new_fields}
        
        new_class._meta.concrete_model = new_class
        
        # Do the appropriate setup for any model parents.
        for base in parents:
            if not hasattr(base, '_meta'):
                # Things without _meta aren't functional models, so they're
                # uninteresting parents.
                continue

            parent_fields = base._meta.local_fields + base._meta.local_many_to_many
            # Check for clashes between locally declared fields and those
            # on the base classes (we cannot handle shadowed fields at the
            # moment).
            for field in parent_fields:
                if field.name in field_names:
                    raise FieldError(
                        'Local field %r in class %r clashes '
                        'with field of similar name from '
                        'base class %r' % (field.name, name, base.__name__)
                    )
            # Inherit virtual fields (like GenericForeignKey) from the parent
            # class
            for field in base._meta.virtual_fields:
                if base._meta.abstract and field.name in field_names:
                    raise FieldError(
                        'Local field %r in class %r clashes '
                        'with field of similar name from '
                        'abstract base class %r' % (field.name, name, base.__name__)
                    )
                new_class.add_to_class(field.name, copy.deepcopy(field))

        # Keep this stuff last
        new_class._prepare()
        # ModelBase calls this, not sure what it does or if we need it here. Need to investigate further.
        #new_class._meta.apps.register_model(new_class._meta.app_label, new_class)
        return new_class

    def add_to_class(cls, name, value):
        if not inspect.isclass(value) and hasattr(value, 'contribute_to_class'):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)
            
    def _prepare(cls):
        cls._meta._prepare(cls)
            
class BrushfireModel(six.with_metaclass(BrushfireModelBase)):
    _deferred = False
    score = IntegerField()
    
    # These aren't inherited for some reason? It breaks the new migration commands
    class Meta:
        abstract = True # Stop Django from creating a BrushfireModel table
        managed = False # Stop Django from creating tables for subclasses
        
    @classmethod
    def check(cls, **kwargs):
        return []

    def _get_pk_val(self, meta=None):
        if not meta:
            meta = self._meta
        return getattr(self, meta.pk.attname)
    
    def _set_pk_val(self, value):
        return setattr(self, self._meta.pk.attname, value)
    
    pk = property(_get_pk_val, _set_pk_val)
    
    def __init__(self, *args, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)
