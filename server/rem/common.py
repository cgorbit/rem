from __future__ import with_statement
import bisect
import copy
import hashlib
import os
import shutil
import tempfile
import time
import types
import re
import sys
import xmlrpclib
from Queue import Queue as StdQueue
from Queue import PriorityQueue as StdPriorityQueue
import heapq
import atexit

import fork_locking
from heap import PriorityQueue
import osspec
from rem_logging import logger as logging
from subprocess import CalledProcessError, MAXFD
import subprocess
from xmlrpc import RpcUserError, as_rpc_user_error, traced_rpc_method

def logged(log_args=False, level="debug", skip_arg_count=0):
    log_func = getattr(logging, level)
    assert callable(log_func)

    def inner(func):
        def inner(*args, **kwargs):
            if log_args:
                args_ = args[skip_arg_count:] \
                    if skip_arg_count else args

                if args_ and kwargs:
                    args_to_log = (args_, kwargs)
                elif kwargs:
                    args_to_log = kwargs
                else:
                    args_to_log = args_

                prefix = 'function "%s(%s)" ' % (func.__name__, str(args_to_log)[:4096])
            else:
                prefix = 'function "%s" ' % func.__name__

            def log(str):
                log_func(prefix + str)

            log('started')
            try:
                ret = func(*args, **kwargs)
                log('finished')
                return ret
            except:
                logging.exception('%s call failed', prefix)
                raise
        inner.__name__ = func.__name__
        return inner
    return inner


class FakeObjectRegistrator(object):
    def register(self, obj, sdict):
        pass

    def LogStats(self):
        pass


class ObjectRegistrator(object):
    TOP_SZ = 10

    def __init__(self):
        self.sum_size = 0
        self.max_objects = []
        self.count = 0
        self.szCache = {}
        self.tpCache = {}

    def register(self, obj, sdict):
        self.szCache[id(obj)] = fullSz = sum(
            object.__sizeof__(k) + self.szCache.get(id(obj), object.__sizeof__(obj)) for k, obj in sdict.iteritems())
        smallSz = object.__sizeof__(sdict)
        self.sum_size += smallSz
        self.count += 1
        bisect.insort(self.max_objects, (fullSz, repr(obj)))
        if len(self.max_objects) >= self.TOP_SZ:
            self.max_objects.pop(0)
        tp = type(obj).__name__
        self.tpCache.setdefault(tp, [0, 0])
        self.tpCache[tp][0] += 1
        self.tpCache[tp][1] += smallSz

    def LogStats(self):
        logging.debug("summary deserialization objects size: %s(%s objects)\nmore huge objects: %s\nby types: %s",
                      self.sum_size, self.count, self.max_objects, self.tpCache)


class ObjectRegistratorsChain(object):
    __slots__ = ['registrators']

    def __init__(self, registrators):
        self.registrators = registrators

    def register(self, obj, sdict):
        for reg in self.registrators:
            reg.register(obj, sdict)

    def LogStats(self):
        for reg in self.registrators:
            reg.LogStats()

ObjectRegistrator_ = FakeObjectRegistrator()
#ObjectRegistrator_ = ObjectRegistrator()


def Unpickable(**kws):
    class ObjBuilder(object):
        def __init__(self, desc):
            if callable(desc):
                self.fn = desc
                self.defargs = ()
            elif isinstance(desc, (tuple, list)):
                self.fn = desc[0]
                self.defargs = desc[1] if len(desc) == 2 and isinstance(desc[1], tuple) else desc[1:]
            else:
                raise RuntimeError("incorrect unpickle plan: %r" % desc)

        def __call__(self, *args):
            if args:
                return self.fn(*args)
            return self.fn(*self.defargs)

    class ObjUnpickler(object):
        def __setstate__(self, sdict):
            for attr, builder in scheme.iteritems():
                try:
                    if attr in sdict:
                        sdict[attr] = builder(sdict[attr])
                    else:
                        sdict[attr] = builder()
                except:
                    logging.exception("unpickable\tcan't deserialize attribute %s with builder %r", attr, builder)
                    raise
            setter = getattr(super(ObjUnpickler, self), "__setstate__", self.__dict__.update)
            setter(sdict)
            ObjectRegistrator_.register(self, sdict)

        def __init__(self, obj=None):
            if obj is not None:
                self.__dict__ = obj.__dict__.copy()
            else:
                for attr, builder in scheme.iteritems():
                    setattr(self, attr, builder())
            getattr(super(ObjUnpickler, self), "__init__")()

    scheme = dict((attr, ObjBuilder(desc)) for attr, desc in kws.iteritems())
    return ObjUnpickler


def Pickable(fields_to_copy):
    class PickableClass(object):
        def __getstate__(self):
            sdict = self.__dict__.copy()
            for k in fields_to_copy:
                if isinstance(sdict[k], (dict, set)):
                    sdict[k] = sdict[k].copy()
                elif isinstance(sdict[k], list):
                    sdict[k] = sdict[k][:]
            return sdict

    return PickableClass


"""set of unpickable helpers"""


def runtime_object(init_value):
    """object with value equal to init_value after each deserialization (for living at run-time objects)"""

    def _constructor(*args):
        return copy.deepcopy(init_value)

    return _constructor


def emptyset(*args):
    return set()

def emptydict(*args):
    return {}


def zeroint(*args):
    return int()

def safeint(oth=None):
    if not isinstance(oth, int):
        return int()
    return int(oth)

def value_or_None(*args):
    return args[0] if args else None


class nullobject(object):
    __instance = None

    def __new__(cls, *args):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls, *args)
        return cls.__instance

    def __init__(self, *args):
        pass


class PickableLock(object):
    def __init__(self, rhs=None):
        self._object = fork_locking.Lock()

    def __getattr__(self, attrname):
        return getattr(self._object, attrname)

    def __enter__(self):
        return self._object.__enter__()

    def __exit__(self, *args):
        return self._object.__exit__(*args)

    def __getstate__(self):
        return True

    def __setstate__(self, state):
        self._object = fork_locking.Lock()


class PickableRLock(object):
    def __init__(self, rhs=None):
        self._object = fork_locking.RLock()

    def __getattr__(self, attrname):
        return getattr(self._object, attrname)

    def __enter__(self):
        return self._object.__enter__()

    def __exit__(self, *args):
        return self._object.__exit__(*args)

    def __getstate__(self):
        return True

    def __setstate__(self, state):
        self._object = fork_locking.RLock()


"""Legacy structs for correct deserialization from old backups"""


class PickableLocker(object): pass


NullObject = nullobject

"""Usefull sets based on PriorityQueue"""


class TimedSet(PriorityQueue, Unpickable(lock=PickableLock)):
    def __init__(self):
        super(TimedSet, self).__init__()

    @classmethod
    def create(cls, list=None):
        if isinstance(list, cls):
            return list
        obj = cls()
        map(obj.add, list or [])
        return obj

    def add(self, obj, tm=None):
        if isinstance(obj, tuple):
            obj, tm = obj
        if obj not in self:
            PriorityQueue.add(self, obj, tm or time.time())

    def remove(self, obj):
        return self.pop(obj)

    def lockedAdd(self, *args):
        with self.lock:
            self.add(*args)

    def lockedPop(self, *args):
        with self.lock:
            self.remove(*args)

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["objects"] = self.objects[:]
        sdict["values"] = self.values[:]
        sdict["revIndex"] = self.revIndex.copy()
        return sdict


class TimedMap(PriorityQueue):
    @classmethod
    def create(cls, dct=None):
        if not dct: dct = {}
        if isinstance(dct, cls):
            return dct
        obj = cls()
        for key, value in dct.iteritems():
            obj.add(key, value)
        return obj

    def add(self, key, value, t=None):
        if key not in self:
            PriorityQueue.add(self, key, (t or time.time(), value))

    def remove(self, key):
        return self.pop(key)


class PickableStdPriorityQueue(Unpickable(_object=StdPriorityQueue)):
    @classmethod
    def create(cls, dct=None):
        if not dct: dct = {}
        obj = cls()
        if isinstance(dct, cls):
            for key, value in dct.__dict__.iteritems():
                if key != "_object":
                    obj.put((key, value))
            return obj

        if isinstance(dct, PriorityQueue):
            for value, key in zip(dct.__dict__['objects'], dct.__dict__['values']):
                obj.put((key, value))
            return obj
        for key, value in dct.iteritems():
            obj.put((key, value))
        return obj

    def __getattr__(self, attrname):
        return getattr(self._object, attrname)

    def __getstate__(self):
        return dict(copy.copy(self.queue))

    def peak(self):
        return self._object.queue[0]


class PickableStdQueue(Unpickable(_object=StdQueue)):
    @classmethod
    def create(cls, dct=None):
        if not dct: dct = {}
        obj = cls()
        if isinstance(dct, list):
            for item in dct:
                obj.put(item)
            return obj
        if isinstance(dct, cls):
            for item in getattr(dct, 'queue', ()):
                obj.put(item)
            return obj
        return obj

    def __getattr__(self, attrname):
        return getattr(self._object, attrname)

    def __getstate__(self):
        sdict = dict()
        sdict['queue'] = copy.copy(self._object.__dict__['queue'])
        return sdict



def GeneralizedSet(priorAttr):
    class _packset(PriorityQueue):
        @classmethod
        def create(cls, list=None):
            if isinstance(list, cls):
                return list
            obj = cls()
            map(obj.add, list or [])
            return obj

        def add(self, pck):
            if pck not in self:
                PriorityQueue.add(self, pck, getattr(pck, priorAttr, 0))

        def remove(self, pck):
            return self.pop(pck)

    return _packset


class PackSet(GeneralizedSet("priority")): pass


class SerializableFunction(object):
    """simple function running object with cPickle support
    WARNING: this class works only with pure function and nondynamic class methods"""

    def __init__(self, fn, args, kws):
        self.callable = fn
        self.args = args
        self.kws = kws

    def __call__(self):
        return self.callable(*self.args, **self.kws)

    def __getstate__(self):
        callable = self.callable

        if isinstance(callable, types.MethodType):
            callable = (
                callable.im_self or callable.im_class,
                callable.im_func.func_name
            )

        return (callable, self.args, self.kws)

    def __setstate__(self, state):
        (callable, args, kws) = state

        if isinstance(callable, tuple):
            self.callable = getattr(callable[0], callable[1])
        else:
            self.callable = callable

        self.args = args
        self.kws = kws

    def __str__(self):
        return str(self.callable)

    def get_function_wo_args(self):
        if self.args or self.kws:
            raise ValueError()

        return self.callable

class BinaryFile(Unpickable(
    links=dict,
    lock=PickableRLock)):
    BUF_SIZE = 256 * 1024

    @classmethod
    def createFile(cls, directory, data):
        checksum = hashlib.md5(data).hexdigest()
        filename = os.path.join(directory, checksum)
        #if os.path.isfile(filename):
        #    raise RuntimeError("can't create file %s" % filename)
        fd, tmpfile = tempfile.mkstemp(dir=directory)
        with os.fdopen(fd, "w") as binWriter:
            binWriter.write(data)
            binWriter.flush()
        shutil.move(tmpfile, filename)
        return cls(filename, checksum, True)

    @classmethod
    def calcFileChecksum(cls, path):
        with open(path, "r") as reader:
            cs_calc = hashlib.md5()
            while True:
                buff = reader.read(BinaryFile.BUF_SIZE)
                if not buff:
                    break
                cs_calc.update(buff)
            return cs_calc.hexdigest()

    def __init__(self, path, checksum=None, set_rx_flag=False):
        assert os.path.isfile(path)
        if set_rx_flag:
            osspec.set_common_readable(path)
            osspec.set_common_executable(path)
        getattr(super(BinaryFile, self), "__init__")()
        self.path = path
        self.checksum = checksum if checksum else BinaryFile.calcFileChecksum(self.path)
        self.accessTime = time.time()

    def release(self):
        if os.path.isfile(self.path):
            os.unlink(self.path)

    def FixLinks(self):
        bad_links = set()
        for (pck_id, name), dest in self.links.iteritems():
            if not os.path.islink(dest):
                logging.warning("%s link item not found for packet %s", dest, pck_id)
                bad_links.add((pck_id, name))
        for i in bad_links:
            self.links.pop(i)

    def LinksCount(self):
        return len(self.links)

    def Link(self, pck, name):
        dstname = os.path.join(pck.directory, name)
        if (pck.id, name) in self.links:
            self.Unlink(pck, name)
        with self.lock:
            self.links[(pck.id, name)] = dstname
            osspec.create_symlink(self.path, dstname, reallocate=False)
            self.accessTime = time.time()

    def Unlink(self, pck, name):
        with self.lock:
            dstname = self.links.get((pck.id, name), None)
            if dstname is not None:
                self.links.pop((pck.id, name))
                self.accessTime = time.time()
                if os.path.islink(dstname):
                    os.unlink(dstname)

    def Relink(self, estimated_path):
        with self.lock:
            self.path = estimated_path
            for dstname in self.links.itervalues():
                dstdir = os.path.split(dstname)[0]
                if not os.path.isdir(dstdir):
                    logging.warning("binfile\tcan't relink nonexisted packet data %s", dstdir)
                elif os.path._resolve_link(dstname) != self.path:
                    osspec.create_symlink(self.path, dstname, reallocate=True)


def safeStringEncode(str):
    return xmlrpclib.Binary(str)


CheckEmailRe = re.compile("[\w._-]+@[\w_-]+\.[\w._-]+$")


def CheckEmailAddress(email):
    if isinstance(email, str) and CheckEmailRe.match(email):
        return True
    return False

def DiscardKey(d, key):
    if key in d:
        del d[key]

def get_None(*args):
    return None

def get_False(*args):
    return False

def should_execute_maker(max_tries=20, penalty_factor=5, *exception_list):
    exception_list = exception_list or []

    def should_execute(f):
        tries = max_tries

        def func(*args, **kwargs):
            penalty = 0.01
            _tries = tries
            while _tries:
                try:
                    return f(*args, **kwargs)
                    break
                except tuple(exception_list), e:
                    time.sleep(penalty)
                    penalty = min(penalty * penalty_factor, 5)
                    _tries -= 1
                    logging.error('Exception in %s, exception message: %s, attempts left:  %s', f.func_name, e.message, _tries)

        return func
    return should_execute

@should_execute_maker(20, 5, Exception)
def send_email(emails, subject, message):
    global proc_runner
    body = \
        """Subject: %(subject)s
To: %(email-list)s

%(message)s
.""" % {"subject": subject, "email-list": ", ".join(emails), "message": message}
    sender = proc_runner.Popen(["sendmail"] + map(str, emails), stdin_content=body)
    return sender.wait()

def parse_network_address(addr):
    try:
        host, port = addr.rsplit(':', 1)
    except ValueError:
        raise ValueError("No port in addr")

    try:
        port = int(port)
    except ValueError as e:
        raise ValueError("Bad port number '%s'" % port)

    if host.startswith('['):
        if not host.endswith(']'):
            raise ValueError("No matching right brace in host '%s'" % host)
        host = host[1:-1]

    if not host:
        raise ValueError("Empty host")

    return host, port


def cleanup_directory(directory, to_keep, max_removed_items_to_output=100):
    removed = []

    files = os.listdir(directory)

    for basename in files:
        if basename in to_keep:
            continue

        filename = directory + '/' + basename

        remove = shutil.rmtree \
            if os.path.isdir(filename) and not os.path.islink(filename) \
            else os.unlink

        try:
            remove(filename)
        except Exception as e:
            logging.error("Can't remove %s: %s" % (filename, e))
        else:
            removed.append(basename)

    if removed:
        logging.info('%d files removed from %s: %s' \
            % (len(removed), directory, ', '.join(removed[:max_removed_items_to_output])))


def split_in_groups(iterable, size):
    if isinstance(iterable, list):
        items = iterable
    else:
        items = list(iterable)

    if not items:
        return

    if len(items) <= size:
        yield items
        return

    group_idx = 0
    while True:
        start_idx = group_idx * size

        if start_idx > len(items) - 1:
            return

        yield items[start_idx:start_idx + size]

        group_idx += 1


def check_process_retcode(retcode, cmd):
    if retcode:
        if isinstance(cmd, list):
            cmd = ' '.join(cmd)
        raise CalledProcessError(retcode, cmd)


def check_process_call(call, args, kwargs):
    retcode = call(*args, **kwargs)
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = args[0]
        check_process_retcode(retcode, cmd)
    return 0


proc_runner = None

def set_proc_runner(runner):
    global proc_runner
    proc_runner = runner

def _unset_proc_runner():
    global proc_runner
    proc_runner = None

atexit.register(_unset_proc_runner)


_inf = float('inf')
_MAX_WAIT_DELAY = 2.0

def wait(f, timeout=None, deadline=None):
    if timeout is not None:
        deadline = time.time() + timeout

    delay = 0.005

    while True:
        res = f()
        if res is not None:
            return res

        if deadline is not None:
            remaining = deadline - time.time()
            if remaining <= 0.0:
                break
        else:
            remaining = _inf

        delay = min(delay * 2, remaining, _MAX_WAIT_DELAY)
        time.sleep(delay)

    return None


class NamedTemporaryDir(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __enter__(self):
        self.name = tempfile.mkdtemp(*self._args, **self._kwargs)
        return self.name

    def __exit__(self, e, t, bt):
        shutil.rmtree(self.name)
        self.name = None


def list_all_user_processes():
    p = subprocess.Popen(["ps", "x", "-o", "command"], stdout=subprocess.PIPE)
    return [l.rstrip('\n') for l in p.stdout]


def copy_ctor_or_none(ctor):
    def create(*args):
        if args and args[0] is not None:
            rhs = args[0]
            return ctor(rhs)
        else:
            return None
    return create
