// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#include "knif_config.h"

int
knif_kafka_config_parse(ErlNifEnv *env, ERL_NIF_TERM list, rd_kafka_conf_t **confp, ERL_NIF_TERM *err_termp)
{
    if (!enif_is_list(env, list) || confp == NULL || err_termp == NULL) {
        *err_termp = enif_make_badarg(env);
        return 0;
    }

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (conf == NULL) {
        *err_termp = enif_make_badarg(env);
        return 0;
    }

    char errstr[512];
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = list;
    const ERL_NIF_TERM *tuple;
    int arity;
    ErlNifBinary key_bin;
    const char *key_str;
    ErlNifBinary val_bin;
    const char *val_str;

    *confp = NULL;
    *err_termp = (ERL_NIF_TERM)0;
    key_bin.size = 0;
    val_bin.size = 0;

    // if (rd_kafka_conf_set(conf, "enable.auto.commit", "true", NULL, 0) != RD_KAFKA_CONF_OK ||
    //     rd_kafka_conf_set(conf, "enable.auto.offset.store", "false", NULL, 0) != RD_KAFKA_CONF_OK ||
    //     rd_kafka_conf_set(conf, "enable.partition.eof", "false", NULL, 0) != RD_KAFKA_CONF_OK) {
    //     *err_termp = enif_raise_exception(env, knif_literal_to_binary(env, "failed to apply default kafka config"));
    //     (void)rd_kafka_conf_destroy(conf);
    //     return 0;
    // }

    if (rd_kafka_conf_set(conf, "log.queue", "true", NULL, 0) != RD_KAFKA_CONF_OK) {
        *err_termp = enif_raise_exception(env, knif_literal_to_binary(env, "failed to apply default kafka config"));
        (void)rd_kafka_conf_destroy(conf);
        return 0;
    }

    // if (rd_kafka_conf_set(conf, "statistics.interval.ms", "5000", NULL, 0) != RD_KAFKA_CONF_OK) {
    //     *err_termp = enif_raise_exception(env, knif_literal_to_binary(env, "failed to apply default kafka config"));
    //     (void)rd_kafka_conf_destroy(conf);
    //     return 0;
    // }

#ifdef SIGIO
    // quick termination
    char tmp[128];
    (void)snprintf(tmp, sizeof(tmp), "%i", SIGIO);

    if (rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0) != RD_KAFKA_CONF_OK) {
        *err_termp = enif_raise_exception(env, knif_literal_to_binary(env, "failed to apply default kafka config"));
        (void)rd_kafka_conf_destroy(conf);
        return 0;
    }
#endif

    while (enif_get_list_cell(env, tail, &head, &tail) != 0) {
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity != 2) {
            *err_termp = enif_raise_exception(env, head);
            break;
        }
        if (!knif_inspect_iolist_as_cstring(env, tuple[0], &key_bin, &key_str)) {
            *err_termp = enif_raise_exception(env, tuple[0]);
            break;
        }
        if (!knif_inspect_iolist_as_cstring(env, tuple[1], &val_bin, &val_str)) {
            *err_termp = enif_raise_exception(env, tuple[1]);
            break;
        }
        if (rd_kafka_conf_set(conf, key_str, val_str, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            *err_termp = enif_raise_exception(env, knif_cstring_to_binary(env, errstr, sizeof(errstr)));
            break;
        }
    }

    if (key_bin.size != 0) {
        (void)enif_release_binary(&key_bin);
    }
    if (val_bin.size != 0) {
        (void)enif_release_binary(&val_bin);
    }

    if (*err_termp != (ERL_NIF_TERM)0) {
        (void)rd_kafka_conf_destroy(conf);
        return 0;
    }

    if (!enif_is_empty_list(env, tail)) {
        *err_termp = enif_raise_exception(env, tail);
        (void)rd_kafka_conf_destroy(conf);
        return 0;
    }

    *confp = conf;

    return 1;
}

int
knif_topic_config_parse(ErlNifEnv *env, ERL_NIF_TERM list, rd_kafka_topic_conf_t **confp, ERL_NIF_TERM *err_termp)
{
    if (!enif_is_list(env, list) || confp == NULL || err_termp == NULL) {
        *err_termp = enif_make_badarg(env);
        return 0;
    }

    rd_kafka_topic_conf_t *conf = rd_kafka_topic_conf_new();
    if (conf == NULL) {
        *err_termp = enif_make_badarg(env);
        return 0;
    }

    char errstr[512];
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = list;
    const ERL_NIF_TERM *tuple;
    int arity;
    ErlNifBinary key_bin;
    const char *key_str;
    ErlNifBinary val_bin;
    const char *val_str;

    *confp = NULL;
    *err_termp = (ERL_NIF_TERM)0;
    key_bin.size = 0;
    val_bin.size = 0;

    if (rd_kafka_topic_conf_set(conf, "produce.offset.report", "true", NULL, 0) != RD_KAFKA_CONF_OK) {
        *err_termp = enif_raise_exception(env, knif_literal_to_binary(env, "failed to apply default topic config"));
        (void)rd_kafka_topic_conf_destroy(conf);
        return 0;
    }

    while (enif_get_list_cell(env, tail, &head, &tail) != 0) {
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity != 2) {
            *err_termp = enif_raise_exception(env, head);
            break;
        }
        if (!knif_inspect_iolist_as_cstring(env, tuple[0], &key_bin, &key_str)) {
            *err_termp = enif_raise_exception(env, tuple[0]);
            break;
        }
        if (!knif_inspect_iolist_as_cstring(env, tuple[1], &val_bin, &val_str)) {
            *err_termp = enif_raise_exception(env, tuple[1]);
            break;
        }
        if (rd_kafka_topic_conf_set(conf, key_str, val_str, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            *err_termp = enif_raise_exception(env, knif_cstring_to_binary(env, errstr, sizeof(errstr)));
            break;
        }
    }

    if (key_bin.size != 0) {
        (void)enif_release_binary(&key_bin);
    }
    if (val_bin.size != 0) {
        (void)enif_release_binary(&val_bin);
    }

    if (*err_termp != (ERL_NIF_TERM)0) {
        (void)rd_kafka_topic_conf_destroy(conf);
        return 0;
    }

    if (!enif_is_empty_list(env, tail)) {
        *err_termp = enif_raise_exception(env, tail);
        (void)rd_kafka_topic_conf_destroy(conf);
        return 0;
    }

    *confp = conf;

    return 1;
}
