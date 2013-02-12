from django.conf import settings
from django.utils.importlib import import_module

from brushfire.core.driver import Solr
from brushfire.core.settings import configuration as conf

driver = conf.get('driver', False, None)
if driver:
    driver = import_module(driver)
else:
    driver = Solr

class SolrEngine(object):
    def __init__(self):
        self.solr = conf.configure_solr(driver)

