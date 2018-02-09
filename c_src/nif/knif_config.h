// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#ifndef KNIF_CONFIG_H
#define KNIF_CONFIG_H

#include "librdkafka_nif.h"
#include "rdkafka.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int knif_kafka_config_parse(ErlNifEnv *env, ERL_NIF_TERM list, rd_kafka_conf_t **confp, ERL_NIF_TERM *err_termp);
extern int knif_topic_config_parse(ErlNifEnv *env, ERL_NIF_TERM list, rd_kafka_topic_conf_t **confp, ERL_NIF_TERM *err_termp);

#ifdef __cplusplus
}
#endif

#endif