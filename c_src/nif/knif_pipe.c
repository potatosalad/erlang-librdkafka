// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#include "knif_pipe.h"

#include <fcntl.h>
#include <unistd.h>

static int cloexec_pipe(int fds[2]);
static int set_cloexec(int fd);

int
knif_pipe_open(int fds[2])
{
    int retval;
    retval = cloexec_pipe(fds);
    if (retval == 0) {
        int fl;
        fl = fcntl(fds[0], F_GETFL, 0);
        (void)fcntl(fds[0], F_SETFL, fl | O_NONBLOCK);
        fl = fcntl(fds[1], F_GETFL, 0);
        (void)fcntl(fds[1], F_SETFL, fl | O_NONBLOCK);
    } else {
        fds[0] = -1;
        fds[1] = -1;
    }
    return retval;
}

int
knif_pipe_close(int fds[2])
{
    int rv0 = 0;
    int rv1 = 0;
    if (fds[0] != -1) {
        rv0 = close(fds[0]);
        fds[0] = -1;
    }
    if (fds[1] != -1) {
        rv1 = close(fds[1]);
        fds[1] = -1;
    }
    return (rv0 || rv1);
}

static int
cloexec_pipe(int fds[2])
{
#ifdef __linux__
    return pipe2(fds, O_CLOEXEC);
#else
    int ret = -1;
    (void)enif_mutex_lock(librdkafka_nif_mutex);

    if (pipe(fds) != 0)
        goto Exit;
    if (set_cloexec(fds[0]) != 0 || set_cloexec(fds[1]) != 0)
        goto Exit;
    ret = 0;

Exit:
    (void)enif_mutex_unlock(librdkafka_nif_mutex);
    return ret;
#endif
}

static int
set_cloexec(int fd)
{
    return fcntl(fd, F_SETFD, FD_CLOEXEC) != -1 ? 0 : -1;
}
