from packet import *
from job import *
from workers import *
from callbacks import LocalTag
from scheduler import Queue, Scheduler
from context import Context
from common import CheckEmailAddress, traced_rpc_method
from xmlrpc import AsyncXMLRPCServer


def DefaultContext(exec_mode=None):
    import optparse

    parser = optparse.OptionParser()
    parser.set_defaults(config="rem.cfg")
    parser.add_option("-c", dest="config", metavar="FILE", help="use configuration FILE")
    opt, args = parser.parse_args()
    return Context(opt.config, exec_mode=exec_mode or args[0])
