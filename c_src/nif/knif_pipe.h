// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#ifndef KNIF_PIPE_H
#define KNIF_PIPE_H

#include "librdkafka_nif.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int knif_pipe_open(int fds[2]);
extern int knif_pipe_close(int fds[2]);

#ifdef __cplusplus
}
#endif

#endif
