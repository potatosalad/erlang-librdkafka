// -*- mode: c++; tab-width: 4; indent-tabs-mode: nil; st-rulers: [132] -*-
// vim: ts=4 sw=4 ft=c++ et

#include "knif_queue.h"
#include "knif_pipe.h"

static ErlNifResourceType *knif_queue_type = NULL;

static void knif_queue_dtor(ErlNifEnv *env, void *obj);
static void knif_queue_stop(ErlNifEnv *env, void *obj, ErlNifEvent event, int is_direct_call);
static void knif_queue_down(ErlNifEnv *env, void *obj, ErlNifPid *pid, ErlNifMonitor *mon);

static ErlNifResourceTypeInit knif_queue_init = {.dtor = knif_queue_dtor, .stop = knif_queue_stop, .down = knif_queue_down};

static void
knif_queue_dtor(ErlNifEnv *env, void *obj)
{
    XNIF_TRACE_F("knif_queue_dtor()\n");
    if (obj == NULL) {
        return;
    }
    knif_queue_t *p = (void *)obj;
    if (p->rkqu != NULL) {
        (void)rd_kafka_queue_destroy(p->rkqu);
        p->rkqu = NULL;
    }
    (void)knif_pipe_close(p->fds);
    if (p->consumer != NULL) {
        (void)enif_release_resource((void *)p->consumer);
        p->consumer = NULL;
    }
    return;
}

static void
knif_queue_stop(ErlNifEnv *env, void *obj, ErlNifEvent event, int is_direct_call)
{
    XNIF_TRACE_F("knif_queue_stop()\n");
    return;
}

static void
knif_queue_down(ErlNifEnv *env, void *obj, ErlNifPid *pid, ErlNifMonitor *mon)
{
    XNIF_TRACE_F("knif_queue_down()\n");
    return;
}

int
knif_queue_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    /* Open resource types */
    if (knif_queue_type == NULL) {
        knif_queue_type =
            enif_open_resource_type_x(env, "librdkafka_nif_queue", &knif_queue_init, ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
        if (knif_queue_type == NULL) {
            return -1;
        }
    }
    return 0;
}

int
knif_queue_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info)
{
    return knif_queue_load(env, priv_data, load_info);
}

void
knif_queue_unload(ErlNifEnv *env, void **priv_data)
{
    knif_queue_type = NULL;
    return;
}

knif_queue_t *
knif_queue_new(ErlNifEnv *env, const knif_consumer_t *consumer, const rd_kafka_topic_partition_t *rkpart)
{
    knif_queue_t p_buff;
    knif_queue_t *p = &p_buff;
    (void)memset(p, 0, sizeof(p_buff));
    p->rkqu = rd_kafka_queue_get_partition(consumer->rk, rkpart->topic, rkpart->partition);
    if (p->rkqu == NULL) {
        return NULL;
    }
    p = (void *)enif_alloc_resource(knif_queue_type, sizeof(p_buff));
    if (p == NULL) {
        (void)rd_kafka_queue_destroy(p_buff.rkqu);
        return NULL;
    }
    (void)xnif_memcpy(p, &p_buff, sizeof(p_buff));
    (void)enif_keep_resource((void *)consumer);
    p->consumer = consumer;
    (void)rd_kafka_queue_forward(p->rkqu, NULL);
    p->fds[0] = -1;
    p->fds[1] = -1;
    p->pid = consumer->pid;
    if (enif_monitor_process(env, (void *)p, &p->pid, &p->mon) != 0) {
        (void)enif_release_resource((void *)p);
        return NULL;
    }
    if (knif_pipe_open(p->fds) != 0) {
        (void)enif_release_resource((void *)p);
        return NULL;
    }
    (void)rd_kafka_queue_io_event_enable(p->rkqu, p->fds[1], "1", 1);
    return p;
}

int
knif_queue_get(ErlNifEnv *env, ERL_NIF_TERM queue_term, knif_queue_t **queue)
{
    if (queue != NULL) {
        *queue = NULL;
    }
    knif_queue_t *p = NULL;
    if (!enif_get_resource(env, queue_term, knif_queue_type, (void **)&p)) {
        return 0;
    }
    if (queue != NULL) {
        *queue = p;
    }
    return 1;
}
