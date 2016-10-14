#include <unistd.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/wait.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <assert.h>
#include <stdlib.h>
#include <limits.h>
#include <pwd.h>
#include <grp.h>

#ifdef PGRPGUARD_SUID
    #undef  PGRPGUARD_ALLOW_USER_CHANGE
#else
    #define PGRPGUARD_ALLOW_USER_CHANGE
#endif

#define PREVENT_WAITPID_KILL_RACE
#define BLOCK_COMMON_TERM_SIGNALS_IN_WRAPPER

enum {
    EXIT_MIN            = 200,
    EXIT_STATUS_IN_FILE = 200,
    EXIT_NO_ARGV        = 201,
    EXIT_NOT_SUPER_USER = 202,
    EXIT_FAILED_REPORT  = 203,
    EXIT_BAD_REPORT_FD  = 204,
    EXIT_USER_CHANGE_NOT_ALLOWED = 205,
    EXIT_BAD_NEW_UID_GID = 206,
    EXIT_MAX            = 220, // with reserve
};

enum {
    ERROR_OK,
    ERROR_SHM,
    ERROR_SETRESUID,
    ERROR_FORK,
    ERROR_EXECVE,
};

enum {
    CS_NOT_CREATED,
    CS_RUNNING,
    CS_ZOMBIE,
    CS_WAITED,
};

enum {
    RT_ERROR        = 1,
    RT_CHILD_STATUS = 2,
};

static const int PRC_TERM_SIGNAL = SIGUSR1;
static const int GRP_KILL_SIGNAL = SIGUSR2;

static volatile int child_state = CS_NOT_CREATED;
static pid_t child_pid = 0;

static int* child_error_type  = NULL;
static int* child_error_errno = NULL;
static int child_status = 0;
static int report_fd = -1;

static uid_t tgt_uid = -1;
static gid_t tgt_gid = -1;

static int set_cloexec(int fd) {
    int flags;

    if ((flags = fcntl(fd, F_GETFD, 0)) == -1) {
        return -1;
    }

    return fcntl(fd, F_SETFD, FD_CLOEXEC | flags);
}

static void change_signal_mask(int sig, int how) {
    sigset_t blocked;
    sigemptyset(&blocked);
    sigaddset(&blocked, sig);
    sigprocmask(how, &blocked, NULL);
}

static int run_child_inner(int, char *argv[]) {
    if (tgt_gid != (tgt_gid)-1) {
        if (setregid(tgt_gid, tgt_gid) == -1) {
            return ERROR_SETRESUID; // FIXME ERROR_SETREGID
        }
    }

    if (setreuid(tgt_uid, tgt_uid) == -1) {
        return ERROR_SETRESUID;
    }

    if (setpgrp() == -1) {
        assert(0);
    }

    change_signal_mask(PRC_TERM_SIGNAL, SIG_UNBLOCK);
    change_signal_mask(GRP_KILL_SIGNAL, SIG_UNBLOCK);
    change_signal_mask(SIGCHLD, SIG_UNBLOCK);

#ifdef BLOCK_COMMON_TERM_SIGNALS_IN_WRAPPER
    change_signal_mask(SIGINT,  SIG_UNBLOCK);
    change_signal_mask(SIGTERM, SIG_UNBLOCK);
    change_signal_mask(SIGQUIT, SIG_UNBLOCK);
#endif

    // 22 cases of fail
    execvp(argv[0], argv);
    return ERROR_EXECVE;
}

static void run_child(int argc, char *argv[]) {
    *child_error_type = run_child_inner(argc, argv);
    *child_error_errno = errno;
}

static void set_signal_handler(int sig, void (*handler)(int)) {
    struct sigaction act;
    bzero(&act, sizeof(act));
    act.sa_handler = handler;
    sigaction(sig, &act, NULL);
}

static void parent_term_sighandler(int sig) {
    if (child_state != CS_RUNNING) {
        return;
    }

    int saved_errno = errno;

    if (sig == PRC_TERM_SIGNAL) {
        kill(child_pid, SIGTERM);
    }
    else if (sig == GRP_KILL_SIGNAL) {
        kill(-child_pid, SIGKILL);
    }

    errno = saved_errno;
}

static void parent_chld_sighandler(int) {
    child_state = CS_ZOMBIE;

    // In sh -c 'sleep 1000; false' only `sh' itself
    // will be killed by kill(child_pid, SIGTERM)
    kill(-child_pid, SIGKILL);
}

static void wait_child(int* status) {
    int ret = 0;

    do {
        ret = waitpid(child_pid, status, 0);
    } while (ret == -1 && errno == EINTR);

    if (ret == -1) {
        assert(0);
    }
}

static void set_term_sighandler() {
    struct sigaction act;
    bzero(&act, sizeof(act));

    act.sa_handler = &parent_term_sighandler;

    sigaddset(&act.sa_mask, PRC_TERM_SIGNAL); // TODO test that this works
    sigaddset(&act.sa_mask, GRP_KILL_SIGNAL);
    sigaddset(&act.sa_mask, SIGCHLD);

    sigaction(PRC_TERM_SIGNAL, &act, NULL);
    sigaction(GRP_KILL_SIGNAL, &act, NULL);
}

static void set_chld_termhandler() {
    struct sigaction act;
    bzero(&act, sizeof(act));

    act.sa_handler = &parent_chld_sighandler;

    act.sa_flags |= SA_NOCLDSTOP;

    sigaddset(&act.sa_mask, PRC_TERM_SIGNAL);
    sigaddset(&act.sa_mask, GRP_KILL_SIGNAL);
    sigaddset(&act.sa_mask, SIGCHLD);

    sigaction(SIGCHLD, &act, NULL);
}

// ~300ms
static void closefds_max(const int but) {
    const int max_fd = sysconf(_SC_OPEN_MAX);

    for (long fd = 0; fd < max_fd; ++fd) {
        if (fd != but) {
            close(fd);
        }
    }
}

// ~1ms
static bool closefds_proc(const int but) {
    char fdpath[PATH_MAX];
    snprintf(fdpath, sizeof(fdpath), "/proc/%ld/fd", (long)getpid());

    DIR *dirp = opendir(fdpath);
    if (!dirp) {
        return false;
    }

    struct dirent *dent;
    const int proc_dir_fd = dirfd(dirp);

    while (dent = readdir(dirp)) {
        char *endp = NULL;
        long fd = strtol(dent->d_name, &endp, 10);

        if (dent->d_name != endp && *endp == '\0' &&
            fd >= 0 && fd < INT_MAX && fd != but && fd != proc_dir_fd)
        {
            close((int)fd);
        }
    }

    closedir(dirp);

    return true;
}

static void closefds(const int but) {
    if (!closefds_proc(but)) {
        closefds_max(but);
    }
}

static int run_parent() {
    closefds(report_fd);

    set_term_sighandler();
    set_chld_termhandler();

#ifdef PREVENT_WAITPID_KILL_RACE
    { // FIXME Prevent race condition of waitpid and kill in signal handlers
        sigset_t waitmask;
        sigprocmask(SIG_SETMASK, NULL, &waitmask);

        sigdelset(&waitmask, PRC_TERM_SIGNAL);
        sigdelset(&waitmask, GRP_KILL_SIGNAL);
        sigdelset(&waitmask, SIGCHLD);

        while (child_state != CS_ZOMBIE) {
            sigsuspend(&waitmask);
        }
    }
#else
    change_signal_mask(PRC_TERM_SIGNAL, SIG_UNBLOCK);
    change_signal_mask(GRP_KILL_SIGNAL, SIG_UNBLOCK);
    change_signal_mask(SIGCHLD, SIG_UNBLOCK);
#endif

    wait_child(&child_status);

    if (*child_error_type) {
        errno = *child_error_errno;
        return *child_error_type;
    }

    assert(WIFEXITED(child_status) || WIFSIGNALED(child_status));

    return ERROR_OK;
}

static int init_shared_memory() {
    void *mem = mmap(0, 4096, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    if (mem == MAP_FAILED) {
        return ERROR_SHM;
    }

    child_error_type  = (int*)mem;
    child_error_errno = child_error_type + 1;

    *child_error_type  = 0;
    *child_error_errno = 0;

    return ERROR_OK;
}

static int run(int argc, char *argv[]) {
    {
        int err = init_shared_memory();
        if (err != ERROR_OK) {
            return err;
        }
    }

    // Reset for child (may be set to SIG_IGN, XXX and actually set in run.py)
    set_signal_handler(PRC_TERM_SIGNAL, SIG_DFL);
    set_signal_handler(GRP_KILL_SIGNAL, SIG_DFL);

    change_signal_mask(SIGCHLD, SIG_BLOCK);

    if ((child_pid = fork()) == -1) {
        return ERROR_FORK;
    }

    if (!child_pid) {
        run_child(argc, argv);
        _exit(0);
    }

    child_state = CS_RUNNING;
    return run_parent();
}

#define write_(ptr, size) \
    if (write(report_fd, ptr, size) != size) return false;

#define write_int(i) do { \
        int v = i; \
        write_(&v, sizeof(v)) \
    } while (0);

#define write_str(s) do { \
        int len = strlen(s); \
        write_int(len); \
        write_(s, len); \
    } while (0);

static bool report_child_status() {
    write_int(RT_CHILD_STATUS);
    write_int(child_status);
    if (close(report_fd) == -1) {
        return false;
    }
    return true;
}

static bool report_error(const int error, const int saved_errno) {
    write_int(RT_ERROR);

    write_int(error);
    write_int(saved_errno);

    const char* msg = NULL;

    switch (error) {
    case ERROR_SHM:
        msg = "Failed to initialize shared memory";
        break;

    case ERROR_SETRESUID:
        msg = "setreuid(2) failed";
        break;

    case ERROR_FORK:
        msg = "fork(2) failed";
        break;

    case ERROR_EXECVE:
        msg = "execve(2) failed";
        break;

    default:
        assert(0);
    }

    write_str(msg);

    if (close(report_fd) == -1) {
        return false;
    }

    return true;
}

#undef write_
#undef write_str
#undef write_int

static int parse_report_fd_number(const char* str) {
    if (strlen(str) == 0) {
        return -1;
    }

    if (str[0] < '0' || str[0] > '9') {
        return -1;
    }

    char* endptr = NULL;

    errno = 0;
    long ret = strtol(str, &endptr, 10);

    if ((errno == ERANGE && (ret == LONG_MAX || ret == LONG_MIN))
            || (errno != 0 && ret == 0)) {
        return -1;
    }

    // No digits were found
    if (endptr == str) {
        return -1;
    }

    // Further characters after number
    if (*endptr != '\0') {
        return -1;
    }

    if (ret > INT_MAX || ret < INT_MIN) {
        return -1;
    }

    return ret;
}

static int parse_new_credentials(const char* descr, uid_t* uid, gid_t* gid) {
    if (!*descr) {
        return -1;
    }

    const char* sep = strchrnul(descr, ':');

    if (descr == sep) {
        return -1;
    }

    {
        const size_t user_name_len = sep - descr;
        char user_name[user_name_len + 1];
        strncpy(user_name, descr, user_name_len); // TODO check return value

        struct passwd* user = getpwnam(user_name);
        if (!user) {
            return -1;
        }

        *uid = user->pw_uid;
    }

    *gid = -1;

    if (!*sep) {
        return 0;
    }

    {
        struct group* group = getgrnam(sep + 1);
        if (!group) {
            return -1;
        }

        *gid = group->gr_gid;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    ++argv;
    --argc;

    // XXX These signals must be blocked before execve(pgrpguard)
    change_signal_mask(PRC_TERM_SIGNAL, SIG_BLOCK);
    change_signal_mask(GRP_KILL_SIGNAL, SIG_BLOCK);

    // FIXME What to do with all other signals?
#ifdef BLOCK_COMMON_TERM_SIGNALS_IN_WRAPPER
    change_signal_mask(SIGINT,  SIG_BLOCK);
    change_signal_mask(SIGTERM, SIG_BLOCK);
    change_signal_mask(SIGQUIT, SIG_BLOCK);
#endif

    uid_t ruid;
    {
        uid_t euid, suid;
        getresuid(&ruid, &euid, &suid);

        if (euid != 0) {
            return EXIT_NOT_SUPER_USER;
        }
    }

    if (argc && argv[0][0] == '@') {
#ifdef PGRPGUARD_SUID
        return EXIT_USER_CHANGE_NOT_ALLOWED;
#endif
        if (parse_new_credentials(argv[0] + 1, &tgt_uid, &tgt_gid) == -1) {
            return EXIT_BAD_NEW_UID_GID;
        }

        ++argv;
        --argc;
    } else {
        tgt_uid = ruid;
        tgt_gid = -1;
    }

    if (argc < 2) {
        return EXIT_NO_ARGV;
    }

    if ((report_fd = parse_report_fd_number(argv[0])) == -1) {
        return EXIT_BAD_REPORT_FD;
    }
    ++argv;
    --argc;

    if (set_cloexec(report_fd) == -1) {
        return EXIT_BAD_REPORT_FD;
    }

    int error = run(argc, argv);
    int saved_errno = errno;

#ifndef PREVENT_WAITPID_KILL_RACE
    // For no EINTR in report (SA_RESTART will not always help)
    change_signal_mask(PRC_TERM_SIGNAL, SIG_BLOCK);
    change_signal_mask(GRP_KILL_SIGNAL, SIG_BLOCK);
    change_signal_mask(SIGCHLD, SIG_BLOCK);
#endif

    bool report_success = true;

    if (error != ERROR_OK) {
        report_success = report_error(error, saved_errno);
    }
    else {
        if (WIFEXITED(child_status)) {
            int exit_code = WEXITSTATUS(child_status);
            if (exit_code < EXIT_MIN || exit_code > EXIT_MAX) {
                return exit_code;
            }
        }

        report_success = report_child_status();
    }

    return report_success
        ? EXIT_STATUS_IN_FILE
        : EXIT_FAILED_REPORT;
}
