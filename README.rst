=========
Brushfire
=========

:author: Mike O'Malley
:date: 2014/03/24

Brushfire is an app/plugin for Django designed to provide full integration into
Solr_ with all of it's advanced features. While brushfire shares some
similarities with Haystack_, the project goals and feature support are
different. Where the goal of the Haystack_ project is to provide a modular
search infrastructure with a configurable set of search backends, the goal of
Brushfire is just the opposite. Brushfire is designed to allow access to all of
Solr_'s advanced featues at the expense of search server portability.

------------
Installation
------------

Grab the source from github::

    git clone https://github.com/spuriousdata/brushfire.git
    
-----
Usage
-----

Generally, you want to start by creating a model instance that maps fields to
your solr schema. Set the primary_key attribute on whatever field you have
defined as your unique id in your solr schema.::

    # mainapp/models.py
    
    from brushfire import models as bfm
    
    class People(bfm.Model):
        ssn           = bfm.CharField(primary_key=True)
        name          = bfm.CharField()
        age           = bfm.IntegerField()
        height_inches = bfm.IntegerField()
        weight_lbs    = bfm.FloatField()
        location      = bfm.CharField()


Now you can query the data using normal django ORM syntax::

    >>> p = People.objects.filter(name__startswith='Bob', height_inches__gte=60)
    >>> p.count()
    32
    >>> p[0].__dict__
    {
        'ssn': '123-45-6789',
        'name': 'Bob Margolin',
        'age': 64,
        'height_inches': None,
        'weight_lbs': None,
        'location': 'Brookline, Mass.'
    }
    
    >>> for person in p:
    ...  # do something with person
    
Most of the normal django ORM methods work (sort, filter, get, etc)::

    >>> p.sort('-age')[0].__dict__
    {
        'ssn': '987-65-4321',
        'name': 'Bob William White',
        'age': 81,
        'height_inches': None,
        'weight_lbs': None,
        'location': 'Los Angeles, California'
    }

###########################
Solr-Specific Features
###########################

Now that you know how to interact with Brushfire as a normal django model,
let's see what else we can do.

****************
Function Queries
****************

Say we were searching health data and needed to get the BMI of each
person in the result set.::

    >>> from brushfire import functions as f
    >>> p = People.objects.all().annotate(bmi=f.mul(f.div('weight_lbs', f.pow('height_inches', 2)), 703))
    >>> p[0].__dict__
    {
        'ssn': 445-43-0399',
        'name': 'Jane Smith',
        'age': 32,
        'height_inches': 65,
        'weight_lbs': 150,
        'bmi': 24.958579881656807,
    }
    
Now, let's filter out anyone with a healthy BMI using a function query and
Solr's {!frange}::
    
    >>> from brushfire import functions as f
    >>> p = People.objects.all().frange(l=25, bmi=f.mul(f.div('weight_lbs', f.pow('height_inches', 2)), 703))
    >>> p[0].__dict__
    {
        'ssn': 445-43-5555',
        'name': 'Jimmy Jones',
        'age': 32,
        'height_inches': 65,
        'weight_lbs': 340,
        'bmi': 56.572781065088755,
    }

The frange() method takes three parameters: l, u, and the key=value pair
representing the function query and the pseudo-field to name the result. The
parameters meanings match the parameters of frange in solr.


.. _Solr: http://lucene.apache.org/solr/
.. _Haystack: http://haystacksearch.org/
