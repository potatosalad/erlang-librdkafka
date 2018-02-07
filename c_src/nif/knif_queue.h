// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#ifndef KNIF_QUEUE_H
#define KNIF_QUEUE_H

#include "librdkafka_nif.h"
#include "knif_consumer.h"
#include "rdkafka.h"

typedef struct knif_queue_s knif_queue_t;
// typedef struct knif_queue_cb_s knif_queue_cb_t;

struct knif_queue_s {
    const knif_consumer_t *consumer;
    rd_kafka_queue_t *rkqu;
    ErlNifPid pid;
    ErlNifMonitor mon;
    int fds[2];
    // const knif_queue_cb_t *cb;
};

// struct knif_consumer_cb_s {
//     ERL_NIF_TERM(*make_message)
//     (ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_t *rkpart, int is_assignment);
//     // ERL_NIF_TERM (*rebalance_assign_partition)(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_t *rkpart);
//     // ERL_NIF_TERM (*rebalance_revoke_partition)(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_t *rkpart);
//     ERL_NIF_TERM (*make_assign_partitions)(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions);
//     ERL_NIF_TERM (*make_revoke_partitions)(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions);
// };

#ifdef __cplusplus
extern "C" {
#endif

extern int knif_queue_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info);
extern int knif_queue_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info);
extern void knif_queue_unload(ErlNifEnv *env, void **priv_data);
extern knif_queue_t *knif_queue_new(ErlNifEnv *env, const knif_consumer_t *consumer, const rd_kafka_topic_partition_t *rkpart);
extern int knif_queue_get(ErlNifEnv *env, ERL_NIF_TERM queue_term, knif_queue_t **queue);

#ifdef __cplusplus
}
#endif

#endif