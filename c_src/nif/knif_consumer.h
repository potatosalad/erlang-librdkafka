// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#ifndef KNIF_CONSUMER_H
#define KNIF_CONSUMER_H

#include "librdkafka_nif.h"
#include "rdkafka.h"

typedef struct knif_consumer_s knif_consumer_t;
typedef struct knif_consumer_cb_s knif_consumer_cb_t;

struct knif_consumer_s {
    rd_kafka_t *rk;
    rd_kafka_queue_t *rkqu;
    rd_kafka_topic_partition_list_t *rkparlist;
    rd_kafka_conf_t *kc;
    rd_kafka_topic_conf_t *tc;
    int kc_readonly;
    int tc_readonly;
    ErlNifPid pid;
    ErlNifMonitor mon;
    int fds[2];
    const knif_consumer_cb_t *cb;
};

struct knif_consumer_cb_s {
    ERL_NIF_TERM(*make_partition)
    (ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_t *rkpart, int is_assignment);
    ERL_NIF_TERM (*make_assign_partitions)(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions);
    ERL_NIF_TERM (*make_revoke_partitions)(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions);
    ERL_NIF_TERM (*make_message)(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM event);
    ERL_NIF_TERM (*make_log)(ErlNifEnv *env, knif_consumer_t *consumer, int rkloglevel, const char *rklogfac, const char *rklogstr);
    ERL_NIF_TERM (*make_stats)(ErlNifEnv *env, knif_consumer_t *consumer, const char *rkstats);
    ERL_NIF_TERM (*make_offset_commit)(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions);
    ERL_NIF_TERM (*make_error)(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_resp_err_t rkresperr, const char *rkrespstr);
};

#ifdef __cplusplus
extern "C" {
#endif

extern int knif_consumer_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info);
extern int knif_consumer_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info);
extern void knif_consumer_unload(ErlNifEnv *env, void **priv_data);
extern knif_consumer_t *knif_consumer_new(ErlNifEnv *env, const knif_consumer_cb_t *cb, rd_kafka_conf_t *kafka_conf,
                                          rd_kafka_topic_conf_t *topic_conf, ERL_NIF_TERM topic_list, char *errstr,
                                          size_t errstr_size);
extern int knif_consumer_get(ErlNifEnv *env, ERL_NIF_TERM consumer_term, knif_consumer_t **consumer);
extern void knif_consumer_error(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_resp_err_t rkresperr, const char *rkrespstr);
extern void knif_consumer_log(ErlNifEnv *env, knif_consumer_t *consumer, int rkloglevel, const char *rklogfac,
                              const char *rklogstr);
extern void knif_consumer_offset_commit(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_resp_err_t rkresperr,
                                        rd_kafka_topic_partition_list_t *rkparlist);
extern void knif_consumer_rebalance(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_resp_err_t rkresperr,
                                    rd_kafka_topic_partition_list_t *rkparlist);
extern void knif_consumer_stats(ErlNifEnv *env, knif_consumer_t *consumer, const char *rkstats);

#ifdef __cplusplus
}
#endif

#endif