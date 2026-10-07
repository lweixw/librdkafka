/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"

#include "../src/tinycthread.h"

/**
 * @name The application assignor (rd_kafka_conf_set_assignor()) against
 *       the mock cluster.
 *
 * Three consumers under an assignor registered by the test. The assignor
 * hands partition p to member (p mod member count) in consumer order (read
 * from each member's subscription userdata), but
 * from the second rebalance on it withholds a partition whose owner is not
 * its new target, the way Kafka Streams' assignor does, so that the owner
 * revokes it and a later rebalance moves it. The test checks:
 *
 *  - every member's subscription userdata reaches the leader and every
 *    member gets back the userdata the leader set for it;
 *  - with "pending", the leader finishes the assignment from another thread
 *    through rd_kafka_assignor_complete();
 *  - a withheld partition is NOT given back to its owner (the cooperative
 *    adjustment the built-in assignors get is skipped for the application
 *    assignor), and a follow-up rebalance then hands it to its target;
 *  - with "violation", an assignment that moves an owned partition directly
 *    is rejected and the group rejoins until the assignor behaves.
 */

#define TOPIC    "app"
#define PART_CNT 4
#define C_CNT    3

static mtx_t lock;

struct consumer {
        char name[16];
        rd_kafka_t *rk;
        /** The userdata the last on_assignment_cb delivered. */
        char userdata[64];
        int on_assignment_cnt;
};

static struct {
        rd_bool_t pending;
        rd_bool_t violation;
        /** Leave the second member's assignment userdata unset: it must
         *  then receive none, not its own subscription userdata. */
        rd_bool_t untouched;
        /** Rejoin while the first assignment is pending: the stale
         *  completion must be dropped and the members it wrote stay
         *  valid. */
        rd_bool_t rejoin_during_pending;
        int assign_calls;
        /** The member id of the leader that ran the last assign call. */
        char leader_id[128];
        /** Subscription userdata seen by the leader, by member order. */
        char seen[C_CNT][16];
        int seen_cnt;
} scn;

struct pending_job {
        rd_kafka_assignor_pending_t *pending;
        rd_kafka_assignor_member_t *members;
        size_t member_cnt;
        int call;
};

static void subscription_cb(rd_kafka_t *rk,
                            const char **topics,
                            size_t topic_cnt,
                            const rd_kafka_topic_partition_list_t *owned,
                            void **userdata,
                            size_t *userdata_size,
                            void *opaque) {
        struct consumer *c = opaque;
        char buf[32];
        size_t len;

        TEST_ASSERT(topic_cnt == 1 && !strcmp(topics[0], TOPIC),
                    "%s: unexpected topics (%d)", c->name, (int)topic_cnt);

        len = rd_snprintf(buf, sizeof(buf), "sub:%s", c->name);
        *userdata = rd_kafka_mem_malloc(rk, len);
        memcpy(*userdata, buf, len);
        *userdata_size = len;
}

/**
 * @brief Orders members by their subscription userdata, "sub:c<N>", which
 *        is the consumer's index; member ids carry no such order.
 */
static int member_cmp(const void *a, const void *b) {
        size_t la, lb;
        const char *ua = rd_kafka_assignor_member_userdata(*(void *const *)a,
                                                           &la);
        const char *ub = rd_kafka_assignor_member_userdata(*(void *const *)b,
                                                           &lb);
        TEST_ASSERT(ua && ub && la == 6 && lb == 6,
                    "members without subscription userdata");
        return strncmp(ua, ub, 6);
}

/**
 * @brief The assignment itself, on whichever thread runs it.
 */
static void assign_members(rd_kafka_assignor_member_t *members,
                           size_t member_cnt,
                           int call) {
        rd_kafka_assignor_member_t *sorted[C_CNT];
        size_t i;
        int p;

        TEST_ASSERT(member_cnt <= C_CNT, "%d members", (int)member_cnt);
        for (i = 0; i < member_cnt; i++)
                sorted[i] = rd_kafka_assignor_member_at(members, i);
        qsort(sorted, member_cnt, sizeof(*sorted), member_cmp);

        mtx_lock(&lock);
        scn.seen_cnt = 0;
        for (i = 0; i < member_cnt; i++) {
                size_t size;
                const char *ud =
                    rd_kafka_assignor_member_userdata(sorted[i], &size);
                TEST_ASSERT(ud && size == 6 && !strncmp(ud, "sub:c", 5),
                            "member %d: unexpected subscription userdata",
                            (int)i);
                memcpy(scn.seen[scn.seen_cnt], ud, size);
                scn.seen[scn.seen_cnt++][size] = '\0';
        }
        mtx_unlock(&lock);

        for (i = 0; i < member_cnt; i++) {
                rd_kafka_topic_partition_list_t *assignment =
                    rd_kafka_topic_partition_list_new(PART_CNT);
                char ud[64];

                for (p = 0; p < PART_CNT; p++) {
                        size_t target = (size_t)p % member_cnt;
                        size_t owner  = member_cnt; /* nobody */
                        size_t j;

                        if (target != i)
                                continue;

                        for (j = 0; j < member_cnt; j++) {
                                const rd_kafka_topic_partition_list_t *owned =
                                    rd_kafka_assignor_member_owned_partitions(
                                        sorted[j]);
                                if (owned &&
                                    rd_kafka_topic_partition_list_find(
                                        owned, TOPIC, p))
                                        owner = j;
                        }

                        /* A partition another member still owns is
                         * withheld (Streams' way), except once in the
                         * "violation" scenario, where it is moved outright
                         * and the library must refuse the assignment. */
                        if (owner != member_cnt && owner != i &&
                            !(scn.violation && call == 2))
                                continue;

                        rd_kafka_topic_partition_list_add(assignment, TOPIC,
                                                          p);
                }

                rd_kafka_assignor_member_set_assignment(sorted[i],
                                                        assignment);
                rd_kafka_topic_partition_list_destroy(assignment);

                if (scn.untouched && i == 1)
                        continue;

                rd_snprintf(ud, sizeof(ud), "asg:%s",
                            rd_kafka_assignor_member_id(sorted[i]));
                rd_kafka_assignor_member_set_userdata(sorted[i], ud,
                                                      strlen(ud));
        }
}

static int pending_thread(void *arg) {
        struct pending_job *job = arg;

        rd_usleep(300 * 1000, NULL);
        assign_members(job->members, job->member_cnt, job->call);
        rd_kafka_assignor_complete(job->pending, RD_KAFKA_RESP_ERR_NO_ERROR,
                                   NULL);
        free(job);
        return 0;
}

static rd_kafka_assignor_result_t
assign_cb(rd_kafka_t *rk,
          const char *member_id,
          const rd_kafka_metadata_t *metadata,
          rd_kafka_assignor_member_t *members,
          size_t member_cnt,
          rd_kafka_assignor_pending_t *pending,
          char *errstr,
          size_t errstr_size,
          void *opaque) {
        int call;
        int i;
        rd_bool_t topic_seen = rd_false;

        mtx_lock(&lock);
        call = ++scn.assign_calls;
        rd_snprintf(scn.leader_id, sizeof(scn.leader_id), "%s", member_id);
        mtx_unlock(&lock);

        for (i = 0; i < metadata->topic_cnt; i++)
                if (!strcmp(metadata->topics[i].topic, TOPIC) &&
                    metadata->topics[i].partition_cnt == PART_CNT)
                        topic_seen = rd_true;
        TEST_ASSERT(topic_seen, "metadata lacks %s with %d partitions", TOPIC,
                    PART_CNT);

        TEST_SAY("assign call %d by %s for %d member(s)%s\n", call, member_id,
                 (int)member_cnt, scn.pending ? " (pending)" : "");

        if (scn.pending) {
                struct pending_job *job = calloc(1, sizeof(*job));
                thrd_t thrd;

                job->pending    = pending;
                job->members    = members;
                job->member_cnt = member_cnt;
                job->call       = call;
                TEST_ASSERT(thrd_create(&thrd, pending_thread, job) ==
                                thrd_success,
                            "thrd_create failed");
                thrd_detach(thrd);
                return RD_KAFKA_ASSIGNOR_PENDING;
        }

        assign_members(members, member_cnt, call);
        return RD_KAFKA_ASSIGNOR_DONE;
}

static void on_assignment_cb(rd_kafka_t *rk,
                             const rd_kafka_topic_partition_list_t *assignment,
                             const void *userdata,
                             size_t userdata_size,
                             const rd_kafka_consumer_group_metadata_t *cgmd,
                             void *opaque) {
        struct consumer *c = opaque;

        mtx_lock(&lock);
        c->on_assignment_cnt++;
        if (userdata && userdata_size < sizeof(c->userdata)) {
                memcpy(c->userdata, userdata, userdata_size);
                c->userdata[userdata_size] = '\0';
        } else
                *c->userdata = '\0';
        mtx_unlock(&lock);
}

/**
 * @brief Poll every consumer until each one's assignment is exactly the
 *        expected set, or the timeout passes.
 */
static void await_assignments(struct consumer *c,
                              int c_cnt,
                              const int expected[C_CNT][PART_CNT],
                              const char *what) {
        int64_t abs_timeout = test_clock() + (20 * 1000 * 1000);
        int i;

        TEST_SAY("Awaiting assignments: %s\n", what);
        while (1) {
                int matched = 0;

                for (i = 0; i < c_cnt; i++) {
                        rd_kafka_topic_partition_list_t *assignment;
                        int p, cnt = 0, ok = 1;

                        test_consumer_poll_no_msgs(what, c[i].rk, 0, 100);
                        TEST_CALL_ERR__(
                            rd_kafka_assignment(c[i].rk, &assignment));
                        for (p = 0; p < PART_CNT; p++) {
                                int has = rd_kafka_topic_partition_list_find(
                                              assignment, TOPIC, p) != NULL;
                                cnt += expected[i][p];
                                if (has != expected[i][p])
                                        ok = 0;
                        }
                        if (assignment->cnt != cnt)
                                ok = 0;
                        rd_kafka_topic_partition_list_destroy(assignment);
                        matched += ok;
                }

                if (matched == c_cnt)
                        return;

                if (test_clock() > abs_timeout) {
                        for (i = 0; i < c_cnt; i++) {
                                rd_kafka_topic_partition_list_t *assignment;
                                TEST_CALL_ERR__(rd_kafka_assignment(
                                    c[i].rk, &assignment));
                                TEST_SAY("%s has %d partition(s)\n", c[i].name,
                                         assignment->cnt);
                                rd_kafka_topic_partition_list_destroy(
                                    assignment);
                        }
                        TEST_FAIL("Timed out waiting for assignments: %s",
                                  what);
                }
        }
}

static void check_userdata(struct consumer *c, int c_cnt) {
        int i;

        mtx_lock(&lock);
        for (i = 0; i < c_cnt; i++) {
                char *member_id = rd_kafka_memberid(c[i].rk);
                char expected[64];
                if (scn.untouched && i == 1)
                        *expected = '\0';
                else
                        rd_snprintf(expected, sizeof(expected), "asg:%s",
                                    member_id);
                TEST_ASSERT(!strcmp(c[i].userdata, expected),
                            "%s: assignment userdata \"%s\", expected \"%s\"",
                            c[i].name, c[i].userdata, expected);
                rd_kafka_mem_free(c[i].rk, member_id);
        }
        mtx_unlock(&lock);
}

static void await_assign_calls(int cnt) {
        int64_t abs_timeout = test_clock() + (10 * 1000 * 1000);
        while (1) {
                mtx_lock(&lock);
                if (scn.assign_calls >= cnt) {
                        mtx_unlock(&lock);
                        return;
                }
                mtx_unlock(&lock);
                TEST_ASSERT(test_clock() < abs_timeout,
                            "timed out waiting for assign call %d", cnt);
                rd_usleep(10 * 1000, NULL);
        }
}

static void do_test(rd_bool_t pending,
                    rd_bool_t violation,
                    rd_bool_t untouched,
                    rd_bool_t rejoin_during_pending) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_conf_t *conf;
        struct consumer c[C_CNT];
        const char *groupid = test_mk_topic_name("0154", 1);
        int i;
        /* Two members: p -> p mod 2. */
        const int round1[C_CNT][PART_CNT] = {{1, 0, 1, 0}, {0, 1, 0, 1}};
        /* Three members: p -> p mod 3; p2 (owned by c0, target c2) and p3
         * (owned by c1, target c0) are withheld. */
        const int round2[C_CNT][PART_CNT] = {
            {1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 0, 0}};
        /* The follow-up rebalance: the withheld partitions reach their
         * targets. */
        const int round3[C_CNT][PART_CNT] = {
            {1, 0, 0, 1}, {0, 1, 0, 0}, {0, 0, 1, 0}};

        SUB_TEST_QUICK("%s%s%s%s", pending ? "pending" : "done",
                       violation ? ", violation" : "",
                       untouched ? ", untouched member" : "",
                       rejoin_during_pending ? ", rejoin during pending" : "");

        memset(&scn, 0, sizeof(scn));
        scn.pending               = pending;
        scn.violation             = violation;
        scn.untouched             = untouched;
        scn.rejoin_during_pending = rejoin_during_pending;
        memset(c, 0, sizeof(c));

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, TOPIC, PART_CNT, 1);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "partition.assignment.strategy", "stream");
        test_conf_set(conf, "session.timeout.ms", "6000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");

        for (i = 0; i < C_CNT; i++) {
                rd_kafka_conf_t *cconf = rd_kafka_conf_dup(conf);
                rd_snprintf(c[i].name, sizeof(c[i].name), "c%d", i);
                test_conf_set(cconf, "client.id", c[i].name);
                rd_kafka_conf_set_assignor(cconf, "stream",
                                           RD_KAFKA_ASSIGNOR_PROTOCOL_COOPERATIVE,
                                           subscription_cb, assign_cb,
                                           on_assignment_cb, &c[i]);
                c[i].rk = test_create_consumer(groupid, NULL, cconf, NULL);
        }
        rd_kafka_conf_destroy(conf);

        /* Round 1: two members. */
        for (i = 0; i < 2; i++)
                test_consumer_subscribe(c[i].rk, TOPIC);
        if (rejoin_during_pending) {
                /* The first assignment is pending for 300ms. The leader
                 * leaves the group under it (a subscription change would
                 * wait for the rebalance to end; leaving does not), so its
                 * completion is stale and the members it writes into have
                 * been handed to the pending; the other member becomes
                 * leader and assigns, then the old leader rejoins. */
                int leader = -1;
                await_assign_calls(1);
                for (i = 0; i < 2; i++) {
                        char *member_id = rd_kafka_memberid(c[i].rk);
                        mtx_lock(&lock);
                        if (!strcmp(member_id, scn.leader_id))
                                leader = i;
                        mtx_unlock(&lock);
                        rd_kafka_mem_free(c[i].rk, member_id);
                }
                TEST_ASSERT(leader >= 0, "leader not found among c0, c1");
                TEST_SAY("leader %s leaves under its pending assignment\n",
                         c[leader].name);
                TEST_CALL_ERR__(rd_kafka_unsubscribe(c[leader].rk));
                await_assign_calls(2);
                test_consumer_subscribe(c[leader].rk, TOPIC);
        }
        await_assignments(c, 2, round1, "round 1, two members");
        check_userdata(c, 2);
        if (rejoin_during_pending) {
                mtx_lock(&lock);
                TEST_ASSERT(scn.assign_calls >= 3,
                            "expected the pending assignment to be "
                            "abandoned and rerun, saw %d assign calls",
                            scn.assign_calls);
                mtx_unlock(&lock);
        }
        mtx_lock(&lock);
        TEST_ASSERT(scn.seen_cnt == 2 && !strcmp(scn.seen[0], "sub:c0") &&
                        !strcmp(scn.seen[1], "sub:c1"),
                    "leader saw %d subscription userdata (%s, %s)",
                    scn.seen_cnt, scn.seen[0], scn.seen[1]);
        mtx_unlock(&lock);

        /* Round 2: a third member; the moving partitions are withheld, and
         * in the violation scenario first moved outright, which the library
         * refuses, so the assignor runs again. */
        test_consumer_subscribe(c[2].rk, TOPIC);
        await_assignments(c, C_CNT, round2, "round 2, withheld partitions");
        check_userdata(c, C_CNT);
        if (violation) {
                mtx_lock(&lock);
                TEST_ASSERT(scn.assign_calls >= 3,
                            "expected the violating assignment to be "
                            "rejected and rerun, saw %d assign calls",
                            scn.assign_calls);
                mtx_unlock(&lock);
        }

        /* Round 3: the owners revoked the withheld partitions; a rejoin
         * (a new subscription) lets the assignor hand them over. */
        test_consumer_subscribe(c[0].rk, TOPIC);
        await_assignments(c, C_CNT, round3, "round 3, follow-up");
        check_userdata(c, C_CNT);

        for (i = 0; i < C_CNT; i++) {
                test_consumer_close(c[i].rk);
                rd_kafka_destroy(c[i].rk);
        }

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief The registration is copied by rd_kafka_conf_dup(), a strategy
 *        naming it is accepted, an unknown name is still refused, and a
 *        name clashing with a built-in assignor fails rd_kafka_new().
 */
static void do_test_conf(void) {
        rd_kafka_conf_t *conf, *dup;
        rd_kafka_t *rk;
        char errstr[256];

        SUB_TEST_QUICK();

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "group.id", "0154-conf");
        rd_kafka_conf_set_assignor(conf, "stream",
                                   RD_KAFKA_ASSIGNOR_PROTOCOL_COOPERATIVE,
                                   subscription_cb, assign_cb,
                                   on_assignment_cb, NULL);

        /* Named through the copy. */
        dup = rd_kafka_conf_dup(conf);
        test_conf_set(dup, "partition.assignment.strategy", "stream");
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, dup, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "rd_kafka_new with the application assignor: %s",
                    errstr);
        rd_kafka_destroy(rk);

        /* Not named: an unknown strategy still fails. */
        dup = rd_kafka_conf_dup(conf);
        test_conf_set(dup, "partition.assignment.strategy", "nosuch");
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, dup, errstr, sizeof(errstr));
        TEST_ASSERT(!rk, "expected an unknown strategy to fail");
        TEST_SAY("unknown strategy: %s\n", errstr);
        rd_kafka_conf_destroy(dup);

        /* A built-in name is refused. */
        dup = rd_kafka_conf_dup(conf);
        rd_kafka_conf_set_assignor(dup, "range",
                                   RD_KAFKA_ASSIGNOR_PROTOCOL_EAGER,
                                   subscription_cb, assign_cb,
                                   on_assignment_cb, NULL);
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, dup, errstr, sizeof(errstr));
        TEST_ASSERT(!rk, "expected a built-in name to be refused");
        TEST_SAY("built-in name: %s\n", errstr);
        rd_kafka_conf_destroy(dup);

        rd_kafka_conf_destroy(conf);

        SUB_TEST_PASS();
}

int main_0154_app_assignor_mock(int argc, char **argv) {
        mtx_init(&lock, mtx_plain);

        do_test_conf();
        do_test(rd_false, rd_false, rd_false, rd_false);
        do_test(rd_false, rd_false, rd_true, rd_false);
        do_test(rd_true, rd_false, rd_false, rd_false);
        do_test(rd_true, rd_false, rd_false, rd_true);
        do_test(rd_false, rd_true, rd_false, rd_false);

        mtx_destroy(&lock);
        return 0;
}
