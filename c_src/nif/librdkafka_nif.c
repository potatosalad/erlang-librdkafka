// -*- mode: c; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c et

#include "librdkafka_nif.h"
#include "xnif_slice.h"

static ERL_NIF_TERM ATOM_false;
static ERL_NIF_TERM ATOM_ok;
static ERL_NIF_TERM ATOM_true;

/* NIF Function Declarations */

static ERL_NIF_TERM librdkafka_nif_check_0(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/* NIF Function Definitions */

/* librdkafka_nif:check/0 */

static ERL_NIF_TERM
librdkafka_nif_check_0(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return ATOM_ok;
}

/* NIF Callbacks */

static ErlNifFunc librdkafka_nif_funcs[] = {{"check", 0, librdkafka_nif_check_0}};

static void librdkafka_nif_make_atoms(ErlNifEnv *env);
static int librdkafka_nif_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info);
static int librdkafka_nif_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info);
static void librdkafka_nif_unload(ErlNifEnv *env, void *priv_data);

static void
librdkafka_nif_make_atoms(ErlNifEnv *env)
{
#define ATOM(Id, Value)                                                                                                            \
    {                                                                                                                              \
        Id = enif_make_atom(env, Value);                                                                                           \
    }
    ATOM(ATOM_false, "false");
    ATOM(ATOM_ok, "ok");
    ATOM(ATOM_true, "true");
#undef ATOM
}

static int
librdkafka_nif_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    int retval;

    /* Load XNIF slice */
    if ((retval = xnif_slice_load(env, priv_data, load_info)) != 0) {
        return retval;
    }

    /* Initialize common atoms */
    (void)librdkafka_nif_make_atoms(env);

    return retval;
}

static int
librdkafka_nif_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info)
{
    int retval;

    /* Upgrade XNIF slice */
    if ((retval = xnif_slice_upgrade(env, priv_data, old_priv_data, load_info)) != 0) {
        return retval;
    }

    /* Initialize common atoms */
    (void)librdkafka_nif_make_atoms(env);

    return retval;
}

static void
librdkafka_nif_unload(ErlNifEnv *env, void *priv_data)
{
    (void)xnif_slice_unload(env, priv_data);
    return;
}

ERL_NIF_INIT(librdkafka_nif, librdkafka_nif_funcs, librdkafka_nif_load, NULL, librdkafka_nif_upgrade, librdkafka_nif_unload);
