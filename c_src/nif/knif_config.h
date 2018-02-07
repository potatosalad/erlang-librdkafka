// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#ifndef KNIF_CONFIG_H
#define KNIF_CONFIG_H

#include "librdkafka_nif.h"
#include "rdkafka.h"

// typedef struct knif_kafka_config_s {
//     rd_kafka_conf_t *conf;
// } knif_kafka_config_t;

// typedef struct knif_topic_config_s {
//     rd_kafka_topic_conf_t *conf;
// } knif_topic_config_t;

#ifdef __cplusplus
extern "C" {
#endif

// extern int knif_config_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info);
// extern int knif_config_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info);
// extern void knif_config_unload(ErlNifEnv *env, void **priv_data);
// extern knif_kafka_config_t *knif_kafka_config_create(void);
// extern knif_topic_config_t *knif_topic_config_create(void);
extern int knif_kafka_config_parse(ErlNifEnv *env, ERL_NIF_TERM list, rd_kafka_conf_t **confp, ERL_NIF_TERM *err_termp);
extern int knif_topic_config_parse(ErlNifEnv *env, ERL_NIF_TERM list, rd_kafka_topic_conf_t **confp, ERL_NIF_TERM *err_termp);

#ifdef __cplusplus
}
#endif

#endif