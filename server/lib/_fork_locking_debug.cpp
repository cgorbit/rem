#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>

#include <util/generic/ptr.h>
#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/stream/str.h>
#include <util/string/printf.h>
#include <util/system/tls.h>

Y_POD_STATIC_THREAD(ui64) AcquireTime;

static size_t debugFd = 2;

static const TDuration PointsDumpInterval = TDuration::Seconds(1);
static const TDuration PointsHistoryInterval = TDuration::Seconds(1);
static ydeque<TInstant> AcquireTimePoints;
static TInstant LastPointsStatDump = TInstant(0);

extern "C" {

void fld_set_debug_fd(int fd) {
    debugFd = fd;
}

void fld_write_debug(const char* s, size_t len) {
    write(debugFd, s, len);
}

void fld_write_int(size_t val) {
    static Stroka Buf;
    Buf.clear();
    TStringOutput out(Buf);
    out << val << '\n';
    write(debugFd, ~Buf, +Buf);
}

void fld_add_acquire_time_point(size_t locked_thread_count) {
    TInstant now = TInstant::Now();
    AcquireTimePoints.push_back(now);

    if (now - LastPointsStatDump > PointsHistoryInterval) {
        LastPointsStatDump = now;

        TInstant cutoff = now - PointsHistoryInterval;
        while (AcquireTimePoints.front() < cutoff) {
            AcquireTimePoints.pop_front();
        }

        static Stroka Buf;
        Buf.clear();

        TStringOutput out(Buf);
        out << "+ locks: " << (+AcquireTimePoints / PointsHistoryInterval.SecondsFloat()) << "\n";
        out << "+ locked_thread_count = " << locked_thread_count << '\n';
        write(debugFd, ~Buf, +Buf);
    }
}

void fld_thread_has_acquired_locks() {
    AcquireTime = TInstant::Now().MicroSeconds();
}

void fld_thread_release_all_locks() {
    TDuration lockedTime = TInstant::Now() - TInstant::MicroSeconds(AcquireTime);

    static Stroka Buf;
    Buf.clear();
    TStringOutput out(Buf);
    out << "+ thread_locked_time " << lockedTime << '\n';
    write(debugFd, ~Buf, +Buf);
}

}
