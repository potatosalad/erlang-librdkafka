// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#include "xnif_memory.h"

void
xnif__fatal(const char *msg)
{
    fprintf(stderr, "fatal:%s\n", msg);
    abort();
}

void
xnif_vector__expand(xnif_vector_t *vector, size_t alignment, size_t element_size, size_t new_capacity)
{
    void *new_entries;
    assert(vector->capacity < new_capacity);
    if (vector->capacity == 0) {
        vector->capacity = 4;
    }
    while (vector->capacity < new_capacity) {
        vector->capacity *= 2;
    }
    if (vector->entries == NULL) {
        new_entries = xnif_mem_alloc(element_size * vector->capacity);
    } else {
        new_entries = xnif_mem_realloc(vector->entries, element_size * vector->capacity);
    }
    vector->entries = new_entries;
}
