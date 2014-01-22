from django.conf import settings
from django.utils.importlib import import_module

from brushfire.core.driver import Solr
from brushfire.core.settings import configuration as conf
from brushfire.core.query import *
from brushfire.core import models

driver = conf.get('driver', False, None)
if driver:
    driver = import_module(driver)
else:
    driver = Solr

solr = conf.configure_solr(driver)
conf.set_solr(solr)
