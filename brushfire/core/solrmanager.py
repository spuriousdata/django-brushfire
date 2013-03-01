from django.conf import settings
from django.utils.importlib import import_module

from brushfire.core.driver import Solr
from brushfire.core.settings import configuration as conf

driver = settings.get("BRUSHFIRE_SOLR_DRIVER", '')
if driver:
    driver = import_module(driver)
else:
    driver = Solr


class SolrManager(object):
    def __init__(self):
        self.solr = conf.configure_solr(driver)



