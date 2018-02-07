// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#ifndef LIBRDKAFKA_NIF_H
#define LIBRDKAFKA_NIF_H

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <erl_nif.h>

#include "xnif_trace.h"
#include "xnif_memory.h"

extern ErlNifMutex *librdkafka_nif_mutex;

#ifdef __cplusplus
extern "C" {
#endif

static int knif_inspect_iolist_as_cstring(ErlNifEnv *env, ERL_NIF_TERM iolist, ErlNifBinary *buffer, const char **sp);
static ERL_NIF_TERM knif_cstring_to_binary(ErlNifEnv *env, const char *s, size_t maxlen);
static ERL_NIF_TERM knif_string_to_binary(ErlNifEnv *env, const char *s, size_t len);
#define knif_literal_to_binary(env, s) knif_string_to_binary(env, (s), sizeof(s) - 1)

inline int
knif_inspect_iolist_as_cstring(ErlNifEnv *env, ERL_NIF_TERM iolist, ErlNifBinary *buffer, const char **sp)
{
    ErlNifBinary binary;
    if (!enif_inspect_iolist_as_binary(env, iolist, &binary)) {
        return 0;
    }
    if (binary.size > 0) {
        if (buffer->size == 0) {
            if (!enif_alloc_binary(binary.size + 1, buffer)) {
                return 0;
            }
        } else if (buffer->size <= binary.size) {
            if (!enif_realloc_binary(buffer, binary.size + 1)) {
                return 0;
            }
        }
        (void)xnif_memcpy(buffer->data, binary.data, binary.size);
        buffer->data[binary.size] = '\0';
    } else {
        if (buffer->size > 0) {
            buffer->data[0] = '\0';
        }
    }
    *sp = (const char *)buffer->data;
    return 1;
}

inline ERL_NIF_TERM
knif_cstring_to_binary(ErlNifEnv *env, const char *s, size_t maxlen)
{
    return knif_string_to_binary(env, s, strnlen(s, maxlen));
}

inline ERL_NIF_TERM
knif_string_to_binary(ErlNifEnv *env, const char *s, size_t len)
{
    ERL_NIF_TERM bin_term;
    unsigned char *buf = enif_make_new_binary(env, len, &bin_term);
    (void)xnif_memcpy(buf, s, len);
    return bin_term;
}

#ifdef __cplusplus
}
#endif

#endif
