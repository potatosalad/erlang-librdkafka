// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#ifndef XNIF_MEMORY_H
#define XNIF_MEMORY_H

#ifdef __sun__
#include <alloca.h>
#endif
#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <erl_nif.h>

#include "xnif_trace.h"

#ifdef __cplusplus
extern "C" {
#endif

#define XNIF_STRUCT_FROM_MEMBER(s, m, p) ((s *)((char *)(p)-offsetof(s, m)))
#define XNIF_ALIGNOF(type) (__alignof__(type))

#define XNIF_TO__STR(n) #n
#define XNIF_TO_STR(n) XNIF_TO__STR(n)

#define XNIF_VECTOR(type)                                                                                                          \
    struct {                                                                                                                       \
        type *entries;                                                                                                             \
        size_t size;                                                                                                               \
        size_t capacity;                                                                                                           \
    }

typedef XNIF_VECTOR(void) xnif_vector_t;
typedef XNIF_VECTOR(ERL_NIF_TERM) xnif_term_vector_t;

/**
 * prints an error message and aborts
 */
#define xnif_fatal(msg) xnif__fatal(__FILE__ ":" XNIF_TO_STR(__LINE__) ":" msg)
extern void xnif__fatal(const char *msg);

/**
 * A version of memcpy that can take a NULL @src to avoid UB
 */
static void *xnif_memcpy(void *dst, const void *src, size_t n);
/**
 * wrapper of enif_alloc; allocates given size of memory or dies if impossible
 */
static void *xnif_mem_alloc(size_t sz);
/**
 * wrapper of enif_realloc; reallocs the given chunk or dies if impossible
 */
static void *xnif_mem_realloc(void *oldp, size_t sz);

/**
 * grows the vector so that it could store at least new_capacity elements of given size (or dies if impossible).
 * @param vector the vector
 * @param element_size size of the elements stored in the vector
 * @param new_capacity the capacity of the buffer after the function returns
 */
#define xnif_vector_reserve(vector, new_capacity)                                                                                  \
    xnif_vector__reserve((xnif_vector_t *)(void *)(vector), XNIF_ALIGNOF((vector)->entries[0]), sizeof((vector)->entries[0]),      \
                         (new_capacity))
static void xnif_vector__reserve(xnif_vector_t *vector, size_t alignment, size_t element_size, size_t new_capacity);
extern void xnif_vector__expand(xnif_vector_t *vector, size_t alignment, size_t element_size, size_t new_capacity);

/* inline defs */

inline void *
xnif_memcpy(void *dst, const void *src, size_t n)
{
    if (src != NULL)
        return memcpy(dst, src, n);
    else if (n != 0)
        xnif_fatal("null pointer passed to memcpy");
    return dst;
}

inline void *
xnif_mem_alloc(size_t sz)
{
    void *p = enif_alloc(sz);
    if (p == NULL) {
        xnif_fatal("no memory");
    }
    return p;
}

inline void *
xnif_mem_realloc(void *oldp, size_t sz)
{
    void *newp = enif_realloc(oldp, sz);
    if (newp == NULL) {
        xnif_fatal("no memory");
        return oldp;
    }
    return newp;
}

inline void
xnif_vector__reserve(xnif_vector_t *vector, size_t alignment, size_t element_size, size_t new_capacity)
{
    if (vector->capacity < new_capacity) {
        xnif_vector__expand(vector, alignment, element_size, new_capacity);
    }
}

#ifdef __cplusplus
}
#endif

#endif
