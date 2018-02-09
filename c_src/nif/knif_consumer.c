// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#include "knif_consumer.h"
#include "knif_pipe.h"

static ErlNifResourceType *knif_consumer_type = NULL;

static void knif_consumer_dtor(ErlNifEnv *env, void *obj);
static void knif_consumer_stop(ErlNifEnv *env, void *obj, ErlNifEvent event, int is_direct_call);
static void knif_consumer_down(ErlNifEnv *env, void *obj, ErlNifPid *pid, ErlNifMonitor *mon);

static ErlNifResourceTypeInit knif_consumer_init = {
    .dtor = knif_consumer_dtor, .stop = knif_consumer_stop, .down = knif_consumer_down};

static void
knif_consumer_dtor(ErlNifEnv *env, void *obj)
{
    XNIF_TRACE_F("knif_consumer_dtor()\n");
    if (obj == NULL) {
        return;
    }
    knif_consumer_t *p = (void *)obj;
    if (p->rkqu != NULL) {
        (void)rd_kafka_queue_destroy(p->rkqu);
        p->rkqu = NULL;
        // XNIF_TRACE_F("rd_kafka_queue_destroy()\n");
    }
    if (p->rk != NULL) {
        // XNIF_TRACE_F("rd_kafka_consumer_close() ?\n");
        (void)rd_kafka_consumer_close(p->rk);
        // XNIF_TRACE_F("rd_kafka_consumer_close() !\n");
        (void)rd_kafka_destroy(p->rk);
        // XNIF_TRACE_F("rd_kafka_destroy()\n");
        p->rk = NULL;
    }
    if (p->rkparlist != NULL) {
        // XNIF_TRACE_F("rd_kafka_topic_partition_list_destroy() ?\n");
        (void)rd_kafka_topic_partition_list_destroy(p->rkparlist);
        // XNIF_TRACE_F("rd_kafka_topic_partition_list_destroy() !\n");
        p->rkparlist = NULL;
    }
    if (p->kc != NULL) {
        if (!p->kc_readonly) {
            // XNIF_TRACE_F("rd_kafka_conf_destroy() ?\n");
            (void)rd_kafka_conf_destroy(p->kc);
            // XNIF_TRACE_F("rd_kafka_conf_destroy() !\n");
        }
        p->kc = NULL;
    }
    if (p->tc != NULL) {
        if (!p->tc_readonly) {
            // XNIF_TRACE_F("rd_kafka_topic_conf_destroy() ?\n");
            (void)rd_kafka_topic_conf_destroy(p->tc);
            // XNIF_TRACE_F("rd_kafka_topic_conf_destroy() !\n");
        }
        p->tc = NULL;
    }
    (void)knif_pipe_close(p->fds);
    return;
}

static void
knif_consumer_stop(ErlNifEnv *env, void *obj, ErlNifEvent event, int is_direct_call)
{
    XNIF_TRACE_F("knif_consumer_stop()\n");
    return;
}

static void
knif_consumer_down(ErlNifEnv *env, void *obj, ErlNifPid *pid, ErlNifMonitor *mon)
{
    XNIF_TRACE_F("knif_consumer_down()\n");
    return;
}

int
knif_consumer_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    /* Open resource types */
    if (knif_consumer_type == NULL) {
        knif_consumer_type = enif_open_resource_type_x(env, "librdkafka_nif_consumer", &knif_consumer_init,
                                                       ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
        if (knif_consumer_type == NULL) {
            return -1;
        }
    }
    return 0;
}

int
knif_consumer_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info)
{
    return knif_consumer_load(env, priv_data, load_info);
}

void
knif_consumer_unload(ErlNifEnv *env, void **priv_data)
{
    knif_consumer_type = NULL;
    return;
}

static knif_consumer_t *knif_consumer_create(ErlNifEnv *env, rd_kafka_conf_t *kafka_conf, rd_kafka_topic_conf_t *topic_conf);

static knif_consumer_t *
knif_consumer_create(ErlNifEnv *env, rd_kafka_conf_t *kafka_conf, rd_kafka_topic_conf_t *topic_conf)
{
    knif_consumer_t *p = (void *)enif_alloc_resource(knif_consumer_type, sizeof(*p));
    if (p == NULL) {
        return NULL;
    }
    (void)memset(p, 0, sizeof(*p));
    p->kc = kafka_conf;
    p->tc = topic_conf;
    (void)rd_kafka_conf_set_opaque(p->kc, (void *)p);
    (void)rd_kafka_topic_conf_set_opaque(p->tc, (void *)p);
    (void)rd_kafka_conf_set_default_topic_conf(p->kc, p->tc);
    p->tc_readonly = 1;
    // #define RD_KAFKA_EVENT_DR            0x1  /**< Producer Delivery report batch */
    // #define RD_KAFKA_EVENT_FETCH         0x2  /**< Fetched message (consumer) */
    // #define RD_KAFKA_EVENT_LOG           0x4  /**< Log message */
    // #define RD_KAFKA_EVENT_ERROR         0x8  /**< Error */
    // #define RD_KAFKA_EVENT_REBALANCE     0x10 /**< Group rebalance (consumer) */
    // #define RD_KAFKA_EVENT_OFFSET_COMMIT 0x20 /**< Offset commit result */
    // #define RD_KAFKA_EVENT_STATS         0x40 /**< Stats */
    (void)rd_kafka_conf_set_events(p->kc, RD_KAFKA_EVENT_FETCH | RD_KAFKA_EVENT_LOG | RD_KAFKA_EVENT_ERROR |
                                              RD_KAFKA_EVENT_REBALANCE | RD_KAFKA_EVENT_OFFSET_COMMIT | RD_KAFKA_EVENT_STATS);
    // (void)rd_kafka_conf_set_events(p->kc, RD_KAFKA_EVENT_REBALANCE);
    // (void)rd_kafka_conf_set_log_cb(p->kc, logger_callback);
    // (void)rd_kafka_conf_set_rebalance_cb(p->kc, rebalance_cb);
    // (void)rd_kafka_conf_set_stats_cb(p->kc, stats_callback);
    p->fds[0] = -1;
    p->fds[1] = -1;
    (void)enif_self(env, &p->pid);
    if (enif_monitor_process(env, (void *)p, &p->pid, &p->mon) != 0) {
        (void)enif_release_resource((void *)p);
        return NULL;
    }
    if (knif_pipe_open(p->fds) != 0) {
        (void)enif_release_resource((void *)p);
        return NULL;
    }
    return p;
}

knif_consumer_t *
knif_consumer_new(ErlNifEnv *env, const knif_consumer_cb_t *cb, rd_kafka_conf_t *kafka_conf, rd_kafka_topic_conf_t *topic_conf,
                  ERL_NIF_TERM topic_list, char *errstr, size_t errstr_size)
{
    unsigned topic_length;
    knif_consumer_t *p = NULL;
    rd_kafka_resp_err_t rkresperr;

    if (cb == NULL) {
        (void)enif_snprintf(errstr, errstr_size, "cb cannot be NULL");
        return NULL;
    }

    if (!enif_get_list_length(env, topic_list, &topic_length)) {
        (void)enif_snprintf(errstr, errstr_size, "invalid topic list for kafka consumer");
        return NULL;
    }

    p = knif_consumer_create(env, kafka_conf, topic_conf);
    if (p == NULL) {
        (void)enif_snprintf(errstr, errstr_size, "unable to allocate kafka consumer");
        return NULL;
    }

    XNIF_TRACE_F("knif_consumer_create()\n");

    p->cb = cb;
    p->rk = rd_kafka_new(RD_KAFKA_CONSUMER, p->kc, errstr, errstr_size);
    if (p->rk == NULL) {
        (void)enif_release_resource((void *)p);
        return NULL;
    }
    p->kc_readonly = 1;

    XNIF_TRACE_F("rd_kafka_new()\n");

    rkresperr = rd_kafka_poll_set_consumer(p->rk);
    if (rkresperr != RD_KAFKA_RESP_ERR_NO_ERROR) {
        (void)enif_snprintf(errstr, errstr_size, "failed during setup of kafka consumer: rd_kafka_poll_set_consumer() %s",
                            rd_kafka_err2str(rkresperr));
        (void)enif_release_resource((void *)p);
        return NULL;
    }

    XNIF_TRACE_F("rd_kafka_poll_set_consumer()\n");

    p->rkqu = rd_kafka_queue_get_consumer(p->rk);
    XNIF_TRACE_F("rd_kafka_queue_get_consumer()\n");
    p->rkparlist = rd_kafka_topic_partition_list_new((int)topic_length);
    XNIF_TRACE_F("rd_kafka_topic_partition_list_new()\n");
    if (p->rkparlist == NULL) {
        (void)enif_snprintf(errstr, errstr_size, "unable to allocate topic partition list for kafka consumer");
        (void)enif_release_resource((void *)p);
        return NULL;
    } else {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail = topic_list;
        ErlNifBinary topic_bin;
        const char *topic_str;
        topic_bin.size = 0;
        while (enif_get_list_cell(env, tail, &head, &tail) != 0) {
            if (!knif_inspect_iolist_as_cstring(env, head, &topic_bin, &topic_str)) {
                (void)enif_snprintf(errstr, errstr_size, "invalid topic for kafka consumer");
                if (topic_bin.size > 0) {
                    (void)enif_release_binary(&topic_bin);
                }
                (void)enif_release_resource((void *)p);
                return NULL;
            }
            (void)rd_kafka_topic_partition_list_add(p->rkparlist, topic_str, RD_KAFKA_PARTITION_UA);
            XNIF_TRACE_F("rd_kafka_topic_partition_list_add(%s)\n", topic_str);
        }
        if (topic_bin.size > 0) {
            (void)enif_release_binary(&topic_bin);
        }
    }

    rkresperr = rd_kafka_subscribe(p->rk, p->rkparlist);
    XNIF_TRACE_F("rd_kafka_subscribe()\n");
    if (rkresperr != RD_KAFKA_RESP_ERR_NO_ERROR) {
        (void)enif_snprintf(errstr, errstr_size, "failed during setup of kafka consumer: rd_kafka_subscribe() %s",
                            rd_kafka_err2str(rkresperr));
        (void)enif_release_resource((void *)p);
        return NULL;
    }

    rkresperr = rd_kafka_set_log_queue(p->rk, p->rkqu);
    XNIF_TRACE_F("rd_kafka_set_log_queue()\n");
    if (rkresperr != RD_KAFKA_RESP_ERR_NO_ERROR) {
        (void)enif_snprintf(errstr, errstr_size, "failed during setup of kafka consumer: rd_kafka_set_log_queue() %s",
                            rd_kafka_err2str(rkresperr));
        (void)enif_release_resource((void *)p);
        return NULL;
    }

    (void)rd_kafka_queue_io_event_enable(p->rkqu, p->fds[1], "1", 1);

    return p;
}

int
knif_consumer_get(ErlNifEnv *env, ERL_NIF_TERM consumer_term, knif_consumer_t **consumer)
{
    if (consumer != NULL) {
        *consumer = NULL;
    }
    knif_consumer_t *p = NULL;
    if (!enif_get_resource(env, consumer_term, knif_consumer_type, (void **)&p)) {
        return 0;
    }
    if (consumer != NULL) {
        *consumer = p;
    }
    return 1;
}

static ERL_NIF_TERM partitions_to_list(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_list_t *rkparlist,
                                       int is_assignment);

void
knif_consumer_error(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_resp_err_t rkresperr, const char *rkrespstr)
{
    ERL_NIF_TERM event;
    ERL_NIF_TERM message;

    event = consumer->cb->make_error(env, consumer, rkresperr, rkrespstr);
    message = consumer->cb->make_message(env, consumer, event);
    (void)enif_send(env, &consumer->pid, NULL, message);

    return;
}

void
knif_consumer_log(ErlNifEnv *env, knif_consumer_t *consumer, int rkloglevel, const char *rklogfac, const char *rklogstr)
{
    ERL_NIF_TERM event;
    ERL_NIF_TERM message;

    event = consumer->cb->make_log(env, consumer, rkloglevel, rklogfac, rklogstr);
    message = consumer->cb->make_message(env, consumer, event);
    (void)enif_send(env, &consumer->pid, NULL, message);

    return;
}

void
knif_consumer_offset_commit(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_resp_err_t rkresperr,
                            rd_kafka_topic_partition_list_t *rkparlist)
{
    XNIF_TRACE_F("(offset_commit) rkresperr = %d\n", rkresperr);

    ERL_NIF_TERM partitions;
    ERL_NIF_TERM event;
    ERL_NIF_TERM message;

    partitions = partitions_to_list(env, consumer, rkparlist, 0);
    event = consumer->cb->make_offset_commit(env, consumer, partitions);
    message = consumer->cb->make_message(env, consumer, event);
    (void)enif_send(env, &consumer->pid, NULL, message);

    return;
}

static void assign_partitions(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_list_t *rkparlist);
static void revoke_partitions(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_list_t *rkparlist);

void
knif_consumer_rebalance(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_resp_err_t rkresperr,
                        rd_kafka_topic_partition_list_t *rkparlist)
{
    switch (rkresperr) {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        (void)assign_partitions(env, consumer, rkparlist);
        break;

    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        (void)revoke_partitions(env, consumer, rkparlist);
        break;

        // default:
        //     log_message(rk, kRdLogLevelError, "rebalance error: "+std::string(rd_kafka_err2str(err)));
        //     revoke_partitions(env, consumer, rk, partitions);
    }

    return;
}

static void
assign_partitions(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_list_t *rkparlist)
{
    rd_kafka_resp_err_t rkresperr;
    ERL_NIF_TERM partitions;
    ERL_NIF_TERM event;
    ERL_NIF_TERM message;

    rkresperr = rd_kafka_assign(consumer->rk, rkparlist);
    if (rkresperr != RD_KAFKA_RESP_ERR_NO_ERROR) {
        XNIF_TRACE_F("unable to assign new partitions\n");
    }

    partitions = partitions_to_list(env, consumer, rkparlist, 1);
    event = consumer->cb->make_assign_partitions(env, consumer, partitions);
    message = consumer->cb->make_message(env, consumer, event);
    (void)enif_send(env, &consumer->pid, NULL, message);

    return;
}

static void
revoke_partitions(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_list_t *rkparlist)
{
    ERL_NIF_TERM partitions;
    ERL_NIF_TERM message;

    (void)rd_kafka_assign(consumer->rk, NULL);

    partitions = partitions_to_list(env, consumer, rkparlist, 0);
    message = consumer->cb->make_revoke_partitions(env, consumer, partitions);
    (void)enif_send(env, &consumer->pid, NULL, message);

    return;
}

static ERL_NIF_TERM
partitions_to_list(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_list_t *rkparlist, int is_assignment)
{
    xnif_term_vector_t tv;
    ERL_NIF_TERM *elements = NULL;
    rd_kafka_topic_partition_t *rkpart = NULL;
    int i;
    ERL_NIF_TERM list;

    if (rkparlist == NULL) {
        return enif_make_list(env, 0);
    }

    tv.entries = NULL;
    tv.size = 0;
    tv.capacity = 0;

    (void)xnif_vector_reserve(&tv, rkparlist->cnt);
    elements = tv.entries;

    for (i = 0; i < rkparlist->cnt; i++) {
        rkpart = &(rkparlist->elems[i]);
        elements[i] = consumer->cb->make_partition(env, consumer, rkpart, is_assignment);
    }

    list = enif_make_list_from_array(env, elements, rkparlist->cnt);
    (void)enif_free((void *)elements);
    return list;
}

void
knif_consumer_stats(ErlNifEnv *env, knif_consumer_t *consumer, const char *rkstats)
{
    ERL_NIF_TERM event;
    ERL_NIF_TERM message;

    event = consumer->cb->make_stats(env, consumer, rkstats);
    message = consumer->cb->make_message(env, consumer, event);
    (void)enif_send(env, &consumer->pid, NULL, message);

    return;
}
