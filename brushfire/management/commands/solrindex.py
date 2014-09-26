from optparse import make_option
from django.core.management.base import NoArgsCommand, CommandError
from brushfire.management.utils import reindex

class Command(NoArgsCommand):
    can_import_settings = True
    option_list = NoArgsCommand.option_list + (
        make_option('-H', '--host', action='store'),
        make_option('-r', '--handler', action='store'),
        make_option('-c', '--core', action='store'),
        make_option('-s', '--swap_core', action='store'),
    )
    
    def handle_noargs(self, host=None, handler=None, core=None,
            swap_core=None, **kwargs):
        if (None, None, None, None) == (host, handler, core, swap_core):
            from brushfire.core.settings import configuration as conf
            
            method = conf.get('index.method', 'dih')
            if method != 'dih':
                raise CommandError, "This command only works with the " \
                                    "dataimporthandler (dih) index method"
            
            host = conf.get('host')
            handler = conf.get('index.dih.handler', '/dataimport')
            core = conf.get('cores.index')
            swap_core = conf.get('index.dih.swap_cores_on_complete', False)
        import pdb; pdb.set_trace() 
        reindex.reindex(host, handler, core, swap_core)
