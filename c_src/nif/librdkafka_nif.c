// -*- mode: c; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c et

#include "librdkafka_nif.h"
#include "xnif_slice.h"

#include "rdkafka.h"
#include "knif_config.h"
#include "knif_consumer.h"
#include "knif_queue.h"

#include <unistd.h>

ErlNifMutex *librdkafka_nif_mutex = NULL;

static ERL_NIF_TERM ATOM_assign_partitions;
static ERL_NIF_TERM ATOM_badarg;
static ERL_NIF_TERM ATOM_closed;
static ERL_NIF_TERM ATOM_error;
static ERL_NIF_TERM ATOM_false;
static ERL_NIF_TERM ATOM_kafka;
static ERL_NIF_TERM ATOM_nil;
static ERL_NIF_TERM ATOM_ok;
static ERL_NIF_TERM ATOM_rebalance;
static ERL_NIF_TERM ATOM_revoke_partitions;
static ERL_NIF_TERM ATOM_true;
static ERL_NIF_TERM ATOM_undefined;

/* NIF Function Declarations */

static ERL_NIF_TERM librdkafka_nif_consumer_new_4(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM librdkafka_nif_consumer_select_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM librdkafka_nif_consumer_poll_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM librdkafka_nif_queue_select_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM librdkafka_nif_queue_poll_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/* Consumer Helper Functions */

static ERL_NIF_TERM librdkafka_nif_make_partition(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_t *rkpart,
                                                  int is_assignment);
static ERL_NIF_TERM librdkafka_nif_make_assign_partitions(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions);
static ERL_NIF_TERM librdkafka_nif_make_revoke_partitions(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions);
static ERL_NIF_TERM librdkafka_nif_make_message(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM event);

static const knif_consumer_cb_t librdkafka_nif_consumer_cb = {.make_partition = librdkafka_nif_make_partition,
                                                              .make_assign_partitions = librdkafka_nif_make_assign_partitions,
                                                              .make_revoke_partitions = librdkafka_nif_make_revoke_partitions,
                                                              .make_message = librdkafka_nif_make_message};

static ERL_NIF_TERM
librdkafka_nif_make_partition(ErlNifEnv *env, knif_consumer_t *consumer, rd_kafka_topic_partition_t *rkpart, int is_assignment)
{
    ERL_NIF_TERM tuple[4];
    tuple[0] = knif_cstring_to_binary(env, rkpart->topic, 255);
    tuple[1] = enif_make_int(env, (int)rkpart->partition);
    if (is_assignment) {
        knif_queue_t *queue = NULL;
        tuple[2] = enif_make_int64(env, (ErlNifSInt64)rkpart->offset);
        queue = knif_queue_new(env, consumer, rkpart);
        if (queue == NULL) {
            tuple[3] =
                enif_make_tuple2(env, ATOM_error, knif_literal_to_binary(env, "failed to create consumer queue: knif_queue_new()"));
        } else {
            tuple[3] = enif_make_resource(env, (void *)queue);
            (void)enif_release_resource((void *)queue);
        }
        return enif_make_tuple_from_array(env, tuple, 4);
    } else {
        return enif_make_tuple_from_array(env, tuple, 2);
    }
}

static ERL_NIF_TERM
librdkafka_nif_make_assign_partitions(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions)
{
    ERL_NIF_TERM tuple[3];

    tuple[0] = ATOM_rebalance;
    tuple[1] = ATOM_assign_partitions;
    tuple[2] = partitions;

    return enif_make_tuple_from_array(env, tuple, 3);
}

static ERL_NIF_TERM
librdkafka_nif_make_revoke_partitions(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM partitions)
{
    ERL_NIF_TERM tuple[3];

    tuple[0] = ATOM_rebalance;
    tuple[1] = ATOM_revoke_partitions;
    tuple[2] = partitions;

    return enif_make_tuple_from_array(env, tuple, 3);
}

static ERL_NIF_TERM
librdkafka_nif_make_message(ErlNifEnv *env, knif_consumer_t *consumer, ERL_NIF_TERM event)
{
    ERL_NIF_TERM tuple[3];

    tuple[0] = ATOM_kafka;
    tuple[1] = enif_make_resource(env, (void *)consumer);
    tuple[2] = event;

    return enif_make_tuple_from_array(env, tuple, 3);
}

/* NIF Function Definitions */

// /* librdkafka_nif:check/0 */

// static ERL_NIF_TERM
// librdkafka_nif_check_0(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
// {
//     return ATOM_ok;
// }

// /* librdkafka_nif:kafka_config/1 */

// static ERL_NIF_TERM
// librdkafka_nif_kafka_config_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
// {
//     knif_kafka_config_t *p = NULL;
//     ERL_NIF_TERM out_term;

//     if (argc != 1 || !enif_is_list(env, argv[0])) {
//         return enif_make_badarg(env);
//     }

//     p = knif_kafka_config_create();
//     if (p == NULL) {
//         return enif_make_badarg(env);
//     }

//     if (!knif_kafka_config_parse(env, p, argv[0], &out_term)) {
//         out_term = enif_raise_exception(env, enif_make_tuple2(env, ATOM_badarg, out_term));
//         (void)enif_release_resource((void *)p);
//         return out_term;
//     }

//     out_term = enif_make_resource(env, (void *)p);
//     (void)enif_release_resource((void *)p);

//     return out_term;
// }

// /* librdkafka_nif:topic_config/1 */

// static ERL_NIF_TERM
// librdkafka_nif_topic_config_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
// {
//     knif_topic_config_t *p = NULL;
//     ERL_NIF_TERM out_term;

//     if (argc != 1 || !enif_is_list(env, argv[0])) {
//         return enif_make_badarg(env);
//     }

//     p = knif_topic_config_create();
//     if (p == NULL) {
//         return enif_make_badarg(env);
//     }

//     if (!knif_topic_config_parse(env, p, argv[0], &out_term)) {
//         out_term = enif_raise_exception(env, enif_make_tuple2(env, ATOM_badarg, out_term));
//         (void)enif_release_resource((void *)p);
//         return out_term;
//     }

//     out_term = enif_make_resource(env, (void *)p);
//     (void)enif_release_resource((void *)p);

//     return out_term;
// }

/* librdkafka_nif:consumer_new/4 */

static ERL_NIF_TERM
librdkafka_nif_consumer_new_4(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM out_term;
    ErlNifBinary group_id_bin;
    const char *group_id_str;

    group_id_bin.size = 0;

    if (argc != 4 || !knif_inspect_iolist_as_cstring(env, argv[0], &group_id_bin, &group_id_str) || !enif_is_list(env, argv[1]) ||
        !enif_is_list(env, argv[2]) || !enif_is_list(env, argv[3])) {
        if (group_id_bin.size > 0) {
            (void)enif_release_binary(&group_id_bin);
        }
        return enif_make_badarg(env);
    }

    rd_kafka_conf_t *kafka_conf;
    rd_kafka_topic_conf_t *topic_conf;

    if (!knif_kafka_config_parse(env, argv[2], &kafka_conf, &out_term)) {
        if (group_id_bin.size > 0) {
            (void)enif_release_binary(&group_id_bin);
        }
        return out_term;
    }

    if (!knif_topic_config_parse(env, argv[3], &topic_conf, &out_term)) {
        (void)rd_kafka_conf_destroy(kafka_conf);
        if (group_id_bin.size > 0) {
            (void)enif_release_binary(&group_id_bin);
        }
        return out_term;
    }

    char errstr[512];

    if (rd_kafka_conf_set(kafka_conf, "group.id", group_id_str, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        out_term = enif_raise_exception(env, knif_cstring_to_binary(env, errstr, sizeof(errstr)));
        (void)rd_kafka_topic_conf_destroy(topic_conf);
        (void)rd_kafka_conf_destroy(kafka_conf);
        if (group_id_bin.size > 0) {
            (void)enif_release_binary(&group_id_bin);
        }
        return out_term;
    }

    if (group_id_bin.size > 0) {
        (void)enif_release_binary(&group_id_bin);
    }

    knif_consumer_t *consumer =
        knif_consumer_new(env, &librdkafka_nif_consumer_cb, kafka_conf, topic_conf, argv[1], errstr, sizeof(errstr));

    if (consumer == NULL) {
        out_term = enif_raise_exception(env, knif_cstring_to_binary(env, errstr, sizeof(errstr)));
        (void)rd_kafka_topic_conf_destroy(topic_conf);
        (void)rd_kafka_conf_destroy(kafka_conf);
        return out_term;
    }

    out_term = enif_make_resource(env, (void *)consumer);
    (void)enif_release_resource((void *)consumer);

    return out_term;
}

/* librdkafka_nif:consumer_select/1 */

static ERL_NIF_TERM
librdkafka_nif_consumer_select_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    knif_consumer_t *consumer = NULL;

    if (argc != 1 || !knif_consumer_get(env, argv[0], &consumer)) {
        return enif_make_badarg(env);
    }

    if (consumer->fds[0] == -1) {
        return enif_make_tuple2(env, ATOM_error, ATOM_closed);
    }

    if (enif_select(env, (ErlNifEvent)consumer->fds[0], ERL_NIF_SELECT_READ, (void *)consumer, &consumer->pid, ATOM_undefined) <
        0) {
        return enif_make_tuple2(env, ATOM_error, ATOM_undefined);
    }

    return ATOM_ok;
}

/* librdkafka_nif:consumer_poll/1 */

static ERL_NIF_TERM
librdkafka_nif_consumer_poll_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    knif_consumer_t *consumer = NULL;

    if (argc != 1 || !knif_consumer_get(env, argv[0], &consumer)) {
        return enif_make_badarg(env);
    }

    if (consumer->fds[0] == -1 || consumer->rkqu == NULL) {
        return enif_make_tuple2(env, ATOM_error, ATOM_closed);
    }

    rd_kafka_event_t *rkev = NULL;
    int retval;
    char b;
    size_t evcnt = 0;

    retval = read(consumer->fds[0], &b, 1);
    (void)retval;

    rd_kafka_resp_err_t rkresperr;
    rd_kafka_topic_partition_list_t *rkparlist = NULL;

    while ((rkev = rd_kafka_queue_poll(consumer->rkqu, 0)) != NULL) {
        evcnt++;
        XNIF_TRACE_F("[consumer] got %s: %s\n", rd_kafka_event_name(rkev), rd_kafka_err2str(rd_kafka_event_error(rkev)));
        switch (rd_kafka_event_type(rkev)) {
        case RD_KAFKA_EVENT_REBALANCE:
            rkresperr = rd_kafka_event_error(rkev);
            rkparlist = rd_kafka_event_topic_partition_list(rkev);
            (void)knif_consumer_rebalance(env, consumer, rkresperr, rkparlist);
            break;
        default:
            break;
        }
        // XNIF_TRACE_F("got event: %d\n", rd_kafka_event_type(rkev));
        (void)rd_kafka_event_destroy(rkev);
    }

    XNIF_TRACE_F("[consumer] read %lu events\n", evcnt);

    return ATOM_ok;
}

/* librdkafka_nif:queue_select/1 */

static ERL_NIF_TERM
librdkafka_nif_queue_select_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    knif_queue_t *queue = NULL;

    if (argc != 1 || !knif_queue_get(env, argv[0], &queue)) {
        return enif_make_badarg(env);
    }

    if (queue->fds[0] == -1) {
        return enif_make_tuple2(env, ATOM_error, ATOM_closed);
    }

    if (enif_select(env, (ErlNifEvent)queue->fds[0], ERL_NIF_SELECT_READ, (void *)queue, &queue->pid,
                    enif_make_resource(env, (void *)queue->consumer)) < 0) {
        return enif_make_tuple2(env, ATOM_error, ATOM_undefined);
    }

    return ATOM_ok;
}

/* librdkafka_nif:queue_poll/1 */

static ERL_NIF_TERM
librdkafka_nif_queue_poll_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    knif_queue_t *queue = NULL;

    if (argc != 1 || !knif_queue_get(env, argv[0], &queue)) {
        return enif_make_badarg(env);
    }

    if (queue->fds[0] == -1 || queue->rkqu == NULL) {
        return enif_make_tuple2(env, ATOM_error, ATOM_closed);
    }

    rd_kafka_event_t *rkev = NULL;
    int retval;
    char b;
    size_t evcnt = 0;

    retval = read(queue->fds[0], &b, 1);
    (void)retval;

    rd_kafka_resp_err_t rkresperr;
    const rd_kafka_message_t *rkmsg = NULL;
    xnif_term_vector_t tv = {.entries = NULL, .size = 0, .capacity = 0};
    size_t rkmsgcnt = 0;
    size_t tvoffset = 0;
    ERL_NIF_TERM *messages = NULL;
    ERL_NIF_TERM message;
    size_t i;

    int has_message_set = 0;
    ERL_NIF_TERM message_set_topic;
    ERL_NIF_TERM message_set_partition;
    ERL_NIF_TERM message_set_offset;
    ERL_NIF_TERM message_set;

    // (void)xnif_vector_reserve(&tv, rkparlist->cnt);

    while ((rkev = rd_kafka_queue_poll(queue->rkqu, 0)) != NULL) {
        evcnt++;
        XNIF_TRACE_F("[queue] got %s: %s\n", rd_kafka_event_name(rkev), rd_kafka_err2str(rd_kafka_event_error(rkev)));
        switch (rd_kafka_event_type(rkev)) {
        case RD_KAFKA_EVENT_FETCH:
            rkresperr = rd_kafka_event_error(rkev);
            rkmsgcnt = rd_kafka_event_message_count(rkev);
            tvoffset += rkmsgcnt;
            (void)xnif_vector_reserve(&tv, tvoffset);
            messages = tv.entries + tv.size;
            for (i = 0; i < rkmsgcnt; i++) {
                (tv.size)++;
                rkmsg = rd_kafka_event_message_next(rkev);
                if (!has_message_set) {
                    const char *topic = rd_kafka_topic_name(rkmsg->rkt);
                    message_set_topic = knif_cstring_to_binary(env, topic, 255);
                    message_set_partition = enif_make_int(env, rkmsg->partition);
                    has_message_set = 1;
                }
                message_set_offset = enif_make_int64(env, rkmsg->offset);
                message =
                    enif_make_tuple3(env, (rkmsg->key == NULL) ? ATOM_nil : knif_string_to_binary(env, rkmsg->key, rkmsg->key_len),
                                     message_set_offset, knif_string_to_binary(env, rkmsg->payload, rkmsg->len));
                messages[i] = message;
            }
            // XNIF_TRACE_F("[queue] fetch rkresperr = %d, count = %d\n", rkresperr, rd_kafka_event_message_count(rkev));
            // (void)knif_consumer_rebalance(env, consumer, rkresperr, rkparlist);
            break;
        default:
            break;
        }
        // XNIF_TRACE_F("got event: %d\n", rd_kafka_event_type(rkev));
        (void)rd_kafka_event_destroy(rkev);
    }

    XNIF_TRACE_F("[queue] read %lu events\n", evcnt);

    messages = tv.entries;
    message_set = enif_make_list_from_array(env, messages, tv.size);
    message_set = enif_make_tuple4(env, message_set_topic, message_set_partition, message_set_offset, message_set);
    (void)enif_send(env, &queue->pid, NULL, message_set);
    (void)enif_free((void *)messages);

    return ATOM_ok;
}

/* NIF Callbacks */

static ErlNifFunc librdkafka_nif_funcs[] = {{"consumer_new", 4, librdkafka_nif_consumer_new_4},
                                            {"consumer_select", 1, librdkafka_nif_consumer_select_1},
                                            {"consumer_poll", 1, librdkafka_nif_consumer_poll_1, ERL_NIF_DIRTY_JOB_IO_BOUND},
                                            {"queue_select", 1, librdkafka_nif_queue_select_1},
                                            {"queue_poll", 1, librdkafka_nif_queue_poll_1, ERL_NIF_DIRTY_JOB_IO_BOUND}};

// static ErlNifFunc librdkafka_nif_funcs[] = {{"check", 0, librdkafka_nif_check_0},
//                                             {"kafka_config", 1, librdkafka_nif_kafka_config_1},
//                                             {"topic_config", 1, librdkafka_nif_topic_config_1}};

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
    ATOM(ATOM_assign_partitions, "assign_partitions");
    ATOM(ATOM_badarg, "badarg");
    ATOM(ATOM_closed, "closed");
    ATOM(ATOM_error, "error");
    ATOM(ATOM_false, "false");
    ATOM(ATOM_kafka, "kafka");
    ATOM(ATOM_nil, "nil");
    ATOM(ATOM_ok, "ok");
    ATOM(ATOM_rebalance, "rebalance");
    ATOM(ATOM_revoke_partitions, "revoke_partitions");
    ATOM(ATOM_true, "true");
    ATOM(ATOM_undefined, "undefined");
#undef ATOM
}

static int
librdkafka_nif_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    int retval;

    if (librdkafka_nif_mutex == NULL) {
        librdkafka_nif_mutex = enif_mutex_create("librdkafka_nif_mutex");
    }
    (void)enif_mutex_lock(librdkafka_nif_mutex);

    /* Load knif_consumer */
    if ((retval = knif_consumer_load(env, priv_data, load_info)) != 0) {
        (void)enif_mutex_unlock(librdkafka_nif_mutex);
        return retval;
    }
    /* Load knif_queue */
    if ((retval = knif_queue_load(env, priv_data, load_info)) != 0) {
        (void)knif_consumer_unload(env, priv_data);
        (void)enif_mutex_unlock(librdkafka_nif_mutex);
        return retval;
    }
    /* Load xnif_slice */
    if ((retval = xnif_slice_load(env, priv_data, load_info)) != 0) {
        (void)knif_queue_unload(env, priv_data);
        (void)knif_consumer_unload(env, priv_data);
        (void)enif_mutex_unlock(librdkafka_nif_mutex);
        return retval;
    }

    /* Initialize common atoms */
    (void)librdkafka_nif_make_atoms(env);

    (void)enif_mutex_unlock(librdkafka_nif_mutex);
    return retval;
}

static int
librdkafka_nif_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info)
{
    int retval;

    if (librdkafka_nif_mutex == NULL) {
        librdkafka_nif_mutex = enif_mutex_create("librdkafka_nif_mutex");
    }
    (void)enif_mutex_lock(librdkafka_nif_mutex);

    /* Upgrade knif_consumer */
    if ((retval = knif_consumer_upgrade(env, priv_data, old_priv_data, load_info)) != 0) {
        (void)enif_mutex_unlock(librdkafka_nif_mutex);
        return retval;
    }
    /* Upgrade knif_queue */
    if ((retval = knif_queue_upgrade(env, priv_data, old_priv_data, load_info)) != 0) {
        (void)enif_mutex_unlock(librdkafka_nif_mutex);
        return retval;
    }
    /* Upgrade xnif_slice */
    if ((retval = xnif_slice_upgrade(env, priv_data, old_priv_data, load_info)) != 0) {
        (void)enif_mutex_unlock(librdkafka_nif_mutex);
        return retval;
    }

    /* Initialize common atoms */
    (void)librdkafka_nif_make_atoms(env);

    (void)enif_mutex_unlock(librdkafka_nif_mutex);
    return retval;
}

static void
librdkafka_nif_unload(ErlNifEnv *env, void *priv_data)
{
    if (librdkafka_nif_mutex != NULL) {
        (void)enif_mutex_lock(librdkafka_nif_mutex);
    }
    (void)xnif_slice_unload(env, priv_data);
    (void)knif_queue_unload(env, priv_data);
    (void)knif_consumer_unload(env, priv_data);
    if (librdkafka_nif_mutex != NULL) {
        (void)enif_mutex_unlock(librdkafka_nif_mutex);
        (void)enif_mutex_destroy(librdkafka_nif_mutex);
        librdkafka_nif_mutex = NULL;
    }
    return;
}

ERL_NIF_INIT(librdkafka_nif, librdkafka_nif_funcs, librdkafka_nif_load, NULL, librdkafka_nif_upgrade, librdkafka_nif_unload);
