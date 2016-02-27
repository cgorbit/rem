#if defined(_win_)
#error Windows is not supported
#endif

#include <pthread.h>
#include <sys/time.h>

#include <Python.h>

#if defined(__clang__)
  #define POD_THREAD(T) __thread T
  #define POD_STATIC_THREAD(T) static __thread T
#elif defined __GNUC__ && (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ > 2)) && !(defined __FreeBSD__ && __FreeBSD__ < 5) && !defined(_cygwin_) && !defined(_arm_) && !defined(__IOS_SIMULATOR__)
  #define POD_THREAD(T) __thread T
  #define POD_STATIC_THREAD(T) static __thread T
#elif defined(_arm_)
  #define POD_THREAD(T) __declspec(thread) T
  #define POD_STATIC_THREAD(T) __declspec(thread) static T
#else
  #error No tls for your architecture
#endif

#if defined(__linux__)
  #include <unistd.h>
  #include <sys/syscall.h>
#endif

typedef unsigned char bool;
#define false 0
#define true 1

#define NO_TIMEOUT -1

#define DEBUG_FORK_LOCKS 0

#if DEBUG_FORK_LOCKS
extern void fld_write_debug(const char* str, size_t len);
extern void fld_set_debug_fd(int fd);
extern void fld_add_acquire_time_point(size_t locked_thread_count);
extern void fld_write_int(size_t);
extern void fld_thread_release_all_locks();
extern void fld_thread_has_acquired_locks();
#endif

#if DEBUG_FORK_LOCKS
    #define WRITE_DEBUG(s) fld_write_debug("+ "s"\n", sizeof("+ "s"\n"))
#else
    #define WRITE_DEBUG(s)
#endif

POD_STATIC_THREAD(size_t) thread_lock_count = 0;

enum {
    FS_NONE = 0,
    FS_ACQUIRING,
    FS_LOCKS_PARTIALLY_DISABLED, // TODO Rename
    FS_FORKING,
};

static const char* fork_state_name[] = {
    "NONE",
    "ACQUIRING",
    "LOCKS_PARTIALLY_DISABLED",
    "FORKING",
};

static volatile size_t fork_state = FS_NONE;
static volatile size_t locked_thread_count = 0;

static pthread_mutex_t lock;

static pthread_cond_t all_locks_released;
static pthread_cond_t fork_state_changed;

static int fork_friendly_acquire_timeout = NO_TIMEOUT;


static inline void
_wait_for(pthread_mutex_t* lock, pthread_cond_t* cond, volatile size_t* value, size_t expected, int timeout) {
    if (*value == expected) {
        return;
    }

    struct timespec deadline;
    bool with_timeout = timeout != NO_TIMEOUT;

    if (with_timeout) {
        struct timeval now;
        gettimeofday(&now, 0);
        deadline.tv_sec = now.tv_sec + timeout;
        deadline.tv_nsec = now.tv_usec * 1000;
    }

    pthread_mutex_lock(lock);
    while (*value != expected) {
        PyThreadState *_save;
        Py_UNBLOCK_THREADS

        int ret = with_timeout
            ? pthread_cond_timedwait(cond, lock, &deadline)
            : pthread_cond_wait(cond, lock);
        pthread_mutex_unlock(lock);

        Py_BLOCK_THREADS

        if (with_timeout && ret == ETIMEDOUT) {
            return;
        }

        pthread_mutex_lock(lock);
    }
    pthread_mutex_unlock(lock);
}

static inline bool
_acquire_fork(void)
{
    if (fork_state != FS_NONE) {
        return false;
    }

    fork_state = FS_ACQUIRING;

    _wait_for(&lock, &all_locks_released, &locked_thread_count, 0, fork_friendly_acquire_timeout);

    if (locked_thread_count) {
        fork_state = FS_LOCKS_PARTIALLY_DISABLED;
        _wait_for(&lock, &all_locks_released, &locked_thread_count, 0, NO_TIMEOUT);
    }

    fork_state = FS_FORKING;

    return true;
}

static inline bool
_release_fork(void)
{
    if (fork_state != FS_FORKING) {
        return false;
    }

    pthread_mutex_lock(&lock);
    fork_state = FS_NONE;
    pthread_mutex_unlock(&lock);

    pthread_cond_broadcast(&fork_state_changed);

    return true;
}

static inline void
_acquire_lock(void)
{
    #if DEBUG_FORK_LOCKS
    /*fld_add_acquire_time_point(locked_thread_count);*/
    #endif

    if (!thread_lock_count) {
        if (fork_state != FS_NONE && fork_state != FS_ACQUIRING) {
            _wait_for(&lock, &fork_state_changed, &fork_state, FS_NONE, NO_TIMEOUT);
        }
    }

    if (++thread_lock_count == 1) {
        ++locked_thread_count;
        #if DEBUG_FORK_LOCKS
        fld_thread_has_acquired_locks();
        #endif
    }
/*fld_write_int(thread_lock_count);*/
}

static inline bool
_release_lock(void)
{
    if (!thread_lock_count) {
        return false;
    }

    if (!--thread_lock_count) {
        if (locked_thread_count == 1) {
            pthread_mutex_lock(&lock);
            locked_thread_count = 0;
            pthread_mutex_unlock(&lock);

            pthread_cond_signal(&all_locks_released);
        } else {
            --locked_thread_count;
        }
        #if DEBUG_FORK_LOCKS
        fld_thread_release_all_locks();
        #endif
    }

/*fld_write_int(thread_lock_count);*/

    return true;
}

static PyObject *
acquire_fork(PyObject *self)
{
    (void)self;

    WRITE_DEBUG("acquire_fork before");
    if (!_acquire_fork()) {
        PyErr_SetString(PyExc_RuntimeError, "Acquire fork already in progress");
        return NULL;
    }
    WRITE_DEBUG("acquire_fork acquired");

    Py_RETURN_NONE;
}

static PyObject *
release_fork(PyObject *self)
{
    (void)self;

    WRITE_DEBUG("release_fork");
    if (!_release_fork()) {
        PyErr_SetString(PyExc_RuntimeError, "Fork is not locked");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
acquire_lock(PyObject *self)
{
    (void)self;
    _acquire_lock();
    Py_RETURN_NONE;
}

static PyObject *
release_lock(PyObject *self)
{
    (void)self;
    if (!_release_lock()) {
        PyErr_SetString(PyExc_RuntimeError, "Thread lock count is zero");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
set_fork_friendly_acquire_timeout(PyObject *self, PyObject *args)
{
    (void)self;

    if (PySequence_Size(args) == 0) {
        if (fork_friendly_acquire_timeout == NO_TIMEOUT) {
            Py_RETURN_NONE;
        }
        else {
            return PyInt_FromLong(fork_friendly_acquire_timeout);
        }
    }

    int timeout = NO_TIMEOUT;

    {
        if (PySequence_Size(args) != 1) {
            PyErr_SetString(PyExc_TypeError, "Takes 0 or 1 argument");
            return NULL;
        }

        PyObject *arg = PySequence_GetItem(args, 0);
        const bool is_none = arg == Py_None;
        Py_XDECREF(arg);

        if (!is_none) {
            if (!PyArg_ParseTuple(args, "i:set_fork_friendly_acquire_timeout", &timeout)) {
                return NULL;
            }

            if (timeout < 0) {
                PyErr_SetString(PyExc_TypeError, "Non-negative value expected");
                return NULL;
            }
        }
    }

    fork_friendly_acquire_timeout = timeout;

    Py_RETURN_NONE;
}

static PyObject *
get_locked_thread_count(PyObject *self)
{
    (void)self;
    return PyInt_FromSize_t(locked_thread_count);
}

static PyObject *
get_thread_lock_count(PyObject *self)
{
    (void)self;
    return PyInt_FromSize_t(thread_lock_count);
}

static PyObject *
get_fork_state(PyObject *self)
{
    (void)self;
    return PyString_FromString(fork_state_name[fork_state]);
}

#if DEBUG_FORK_LOCKS
static PyObject *
set_debug_fd(PyObject *self, PyObject *args)
{
    (void)self;
    int fd;

    if (!PyArg_ParseTuple(args, "i:set_debug_fd", &fd))
        return NULL;

    fld_set_debug_fd(fd);

    Py_RETURN_NONE;
}

static PyObject *
write_debug(PyObject *self, PyObject *args)
{
    (void)self;
    const char* string = NULL;

    if (!PyArg_ParseTuple(args, "s:write", &string))
        return NULL;

    fld_write_debug(string, strlen(string));

    Py_RETURN_NONE;
}
#endif

#if defined(__linux__)
static inline pid_t gettid() {
    return syscall(SYS_gettid);
}
#endif

static PyObject *
Py_gettid(PyObject *self)
{
    (void)self;
#if defined(__linux__)
    return PyInt_FromLong(gettid());
#else
    Py_RETURN_NONE;
#endif
}

static PyMethodDef module_methods[] = {
    {"acquire_fork",  (PyCFunction)acquire_fork, METH_NOARGS, NULL},
    {"release_fork",  (PyCFunction)release_fork, METH_NOARGS, NULL},
    {"acquire_lock",  (PyCFunction)acquire_lock, METH_NOARGS, NULL},
    {"release_lock",  (PyCFunction)release_lock, METH_NOARGS, NULL},
    {"get_locked_thread_count",  (PyCFunction)get_locked_thread_count, METH_NOARGS, NULL},
    {"get_thread_lock_count",  (PyCFunction)get_thread_lock_count, METH_NOARGS, NULL},
    {"get_fork_state",  (PyCFunction)get_fork_state, METH_NOARGS, NULL},
    {"set_fork_friendly_acquire_timeout",
        (PyCFunction)set_fork_friendly_acquire_timeout, METH_VARARGS, NULL},
#if DEBUG_FORK_LOCKS
    {"set_debug_fd",  (PyCFunction)set_debug_fd, METH_VARARGS, NULL},
    {"write",  (PyCFunction)write_debug, METH_VARARGS, NULL},
#endif
    {"gettid",        (PyCFunction)Py_gettid, METH_NOARGS, NULL},
    {NULL,            NULL,                      0,           NULL}
};

PyMODINIT_FUNC
init_fork_locking(void)
{
    Py_InitModule3("_fork_locking", module_methods, NULL);
}
