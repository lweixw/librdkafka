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
 * from each member's subscription userdata), but from the second rebalance
 * on it withholds a partition whose owner is not its new target, the way
 * Kafka Streams' assignor does, so that the owner revokes it and a later
 * rebalance moves it. Scenarios:
 *
 *  - done: the leader assigns synchronously; every member's subscription
 *    userdata reaches the leader (the third member sends none) and every
 *    member gets back the userdata the leader set for it; the accessors
 *    report the subscription, no group instance id and no rack; a withheld
 *    partition is NOT given back to its owner (the cooperative adjustment
 *    the built-in assignors get is skipped), and the follow-up rebalance
 *    hands it over;
 *  - untouched member: a member the leader sets no userdata for receives
 *    none, not its own subscription userdata echoed;
 *  - pending: the leader finishes from another thread through
 *    rd_kafka_assignor_complete();
 *  - close during pending: the leader is closed and destroyed while its
 *    assignment is pending and the handle completes afterwards: the members
 *    behind the handle stay valid and the completion is a safe no-op;
 *  - join during pending: a member joins under a pending assignment;
 *  - timeout: a pending assignment is not completed within
 *    session.timeout.ms: the library gives it up and rejoins, the late
 *    completion is dropped;
 *  - fail sync / fail async: the leader fails one assignment, synchronously
 *    or through rd_kafka_assignor_complete(err), and the group rejoins;
 *  - violation: an assignment that moves an owned partition directly is
 *    rejected and the group rejoins until the assignor behaves;
 *  - eager: under the EAGER protocol every rebalance starts from nothing
 *    owned, so partitions move at once.
 */

#define TOPIC    "app"
#define PART_CNT 4
#define C_CNT    3

static mtx_t lock;
static cnd_t cnd;

struct consumer {
        char name[16];
        rd_kafka_t *rk;
        /** The userdata the last on_assignment_cb delivered. */
        char userdata[64];
        int on_assignment_cnt;
        /** Last generation on_assignment_cb reported. */
        int32_t generation;
};

enum mode {
        MODE_DONE,
        MODE_UNTOUCHED,
        MODE_PENDING,
        MODE_CLOSE_DURING_PENDING,
        MODE_JOIN_DURING_PENDING,
        MODE_TIMEOUT,
        MODE_FAIL_SYNC,
        MODE_FAIL_ASYNC,
        MODE_VIOLATION,
        MODE_EAGER,
};

static struct {
        enum mode mode;
        int assign_calls;
        /** The member id of the leader that ran the last assign call. */
        char leader_id[128];
        /** Member count of the violating assign call. */
        int violation_members;
        /** The first assign call with all three members: the one the
         *  violation and failure scenarios act on. */
        int first_three_member_call;
        /** Subscription userdata seen by the leader, by member order. */
        char seen[C_CNT][16];
        int seen_cnt;
        /** Pending threads wait on this before completing when set. */
        rd_bool_t hold_completion;
        int threads;
} scn;

struct pending_job {
        rd_kafka_assignor_pending_t *pending;
        rd_kafka_assignor_member_t *members;
        size_t member_cnt;
        int call;
};

static void subscription_cb(rd_kafka_t *rk,
                            const char *const *topics,
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

        /* The third consumer sends no userdata at all. */
        if (!strcmp(c->name, "c2"))
                return;

        len = rd_snprintf(buf, sizeof(buf), "sub:%s", c->name);
        *userdata = rd_kafka_mem_malloc(rk, len);
        memcpy(*userdata, buf, len);
        *userdata_size = len;
}

/**
 * @brief Orders members by their subscription userdata, "sub:c<N>", which
 *        is the consumer's index; the member without userdata (c2) last.
 *        Member ids carry no such order.
 */
static int member_cmp(const void *a, const void *b) {
        size_t la, lb;
        const char *ua = rd_kafka_assignor_member_userdata(*(void *const *)a,
                                                           &la);
        const char *ub = rd_kafka_assignor_member_userdata(*(void *const *)b,
                                                           &lb);
        if (!ua || !ub)
                return (ua ? 0 : 1) - (ub ? 0 : 1);
        return strncmp(ua, ub, RD_MIN(la, lb));
}

/**
 * @brief The assignment itself, on whichever thread runs it.
 */
static void assign_members(rd_kafka_assignor_member_t *members,
                           size_t member_cnt,
                           int call) {
        rd_kafka_assignor_member_t *sorted[C_CNT];
        enum mode mode;
        rd_bool_t violate;
        size_t i;
        int p;

        mtx_lock(&lock);
        mode    = scn.mode;
        violate = mode == MODE_VIOLATION &&
                  call == scn.first_three_member_call;
        mtx_unlock(&lock);

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
                const rd_kafka_topic_partition_list_t *sub =
                    rd_kafka_assignor_member_subscription(sorted[i]);

                TEST_ASSERT(sub && sub->cnt == 1 &&
                                !strcmp(sub->elems[0].topic, TOPIC),
                            "member %d: unexpected subscription", (int)i);
                TEST_ASSERT(!rd_kafka_assignor_member_group_instance_id(
                                sorted[i]),
                            "member %d: unexpected group instance id",
                            (int)i);
                TEST_ASSERT(!rd_kafka_assignor_member_rack_id(sorted[i]),
                            "member %d: unexpected rack id", (int)i);
                TEST_ASSERT(*rd_kafka_assignor_member_id(sorted[i]),
                            "member %d: empty member id", (int)i);

                if (ud) {
                        TEST_ASSERT(size == 6 && !strncmp(ud, "sub:c", 5),
                                    "member %d: unexpected subscription "
                                    "userdata",
                                    (int)i);
                        memcpy(scn.seen[scn.seen_cnt], ud, size);
                        scn.seen[scn.seen_cnt++][size] = '\0';
                } else {
                        TEST_ASSERT(size == 0 && i == member_cnt - 1,
                                    "member %d without userdata is not c2",
                                    (int)i);
                        strcpy(scn.seen[scn.seen_cnt++], "none");
                }
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
                         * violation scenario, where it is moved outright
                         * and the library must refuse the assignment. */
                        if (owner != member_cnt && owner != i && !violate)
                                continue;

                        rd_kafka_topic_partition_list_add(assignment, TOPIC,
                                                          p);
                }

                /* An empty assignment through NULL, the others as lists. */
                rd_kafka_assignor_member_set_assignment(
                    sorted[i], assignment->cnt ? assignment : NULL);
                rd_kafka_topic_partition_list_destroy(assignment);

                if (mode == MODE_UNTOUCHED && i == 1)
                        continue;

                rd_snprintf(ud, sizeof(ud), "asg:%s",
                            rd_kafka_assignor_member_id(sorted[i]));
                rd_kafka_assignor_member_set_userdata(sorted[i], ud,
                                                      strlen(ud));
        }

        if (violate) {
                mtx_lock(&lock);
                scn.violation_members = (int)member_cnt;
                mtx_unlock(&lock);
        }
}

static int pending_thread(void *arg) {
        struct pending_job *job = arg;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        enum mode mode;

        mtx_lock(&lock);
        mode = scn.mode;
        scn.threads++;
        mtx_unlock(&lock);

        rd_usleep(300 * 1000, NULL);
        assign_members(job->members, job->member_cnt, job->call);

        mtx_lock(&lock);
        if (mode == MODE_FAIL_ASYNC && job->call == scn.first_three_member_call)
                err = RD_KAFKA_RESP_ERR__FAIL;
        mtx_unlock(&lock);

        /* Scenarios that act on the group under the pending assignment
         * release the thread once they are done. */
        mtx_lock(&lock);
        while (scn.hold_completion)
                cnd_wait(&cnd, &lock);
        mtx_unlock(&lock);

        rd_kafka_assignor_complete(job->pending, err,
                                   err ? "failed on purpose" : NULL);

        mtx_lock(&lock);
        scn.threads--;
        cnd_broadcast(&cnd);
        mtx_unlock(&lock);

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
        enum mode mode;
        int call;
        int i;
        rd_bool_t topic_seen = rd_false;
        rd_bool_t fail_sync;

        mtx_lock(&lock);
        mode = scn.mode;
        call = ++scn.assign_calls;
        if (member_cnt == C_CNT && !scn.first_three_member_call)
                scn.first_three_member_call = call;
        fail_sync = mode == MODE_FAIL_SYNC &&
                    call == scn.first_three_member_call;
        rd_snprintf(scn.leader_id, sizeof(scn.leader_id), "%s", member_id);
        mtx_unlock(&lock);

        for (i = 0; i < metadata->topic_cnt; i++)
                if (!strcmp(metadata->topics[i].topic, TOPIC) &&
                    metadata->topics[i].partition_cnt == PART_CNT)
                        topic_seen = rd_true;
        TEST_ASSERT(topic_seen, "metadata lacks %s with %d partitions", TOPIC,
                    PART_CNT);

        TEST_SAY("assign call %d for %d member(s)\n", call, (int)member_cnt);

        if (fail_sync) {
                rd_snprintf(errstr, errstr_size, "failed on purpose");
                return RD_KAFKA_ASSIGNOR_FAILED;
        }

        if (mode == MODE_PENDING || mode == MODE_CLOSE_DURING_PENDING ||
            mode == MODE_JOIN_DURING_PENDING || mode == MODE_TIMEOUT ||
            mode == MODE_FAIL_ASYNC) {
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
        c->generation = rd_kafka_consumer_group_metadata_generation_id(cgmd);
        if (userdata && userdata_size < sizeof(c->userdata)) {
                memcpy(c->userdata, userdata, userdata_size);
                c->userdata[userdata_size] = '\0';
        } else
                *c->userdata = '\0';
        mtx_unlock(&lock);
}

/**
 * @brief Poll every open consumer until each one's assignment is exactly
 *        the expected set, or the timeout passes.
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

                        if (!c[i].rk) {
                                matched++;
                                continue;
                        }

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
                                if (!c[i].rk)
                                        continue;
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

/**
 * @brief Each open consumer's last assignment userdata names its own member
 *        id, except the untouched member's, which is empty.
 */
static void check_userdata(struct consumer *c, int c_cnt) {
        char *member_ids[C_CNT] = {NULL};
        int i;

        /* rd_kafka_memberid() waits for the client's main thread, which the
         * callbacks take `lock` on: read the ids before taking it. */
        for (i = 0; i < c_cnt; i++)
                if (c[i].rk)
                        member_ids[i] = rd_kafka_memberid(c[i].rk);

        mtx_lock(&lock);
        for (i = 0; i < c_cnt; i++) {
                char expected[64];
                if (!c[i].rk)
                        continue;
                if (scn.mode == MODE_UNTOUCHED && i == 1)
                        *expected = '\0';
                else
                        rd_snprintf(expected, sizeof(expected), "asg:%s",
                                    member_ids[i]);
                TEST_ASSERT(!strcmp(c[i].userdata, expected),
                            "%s: assignment userdata \"%s\", expected \"%s\"",
                            c[i].name, c[i].userdata, expected);
                TEST_ASSERT(c[i].on_assignment_cnt > 0 && c[i].generation > 0,
                            "%s: %d on_assignment calls, generation %d",
                            c[i].name, c[i].on_assignment_cnt,
                            c[i].generation);
        }
        mtx_unlock(&lock);

        for (i = 0; i < c_cnt; i++)
                if (member_ids[i])
                        rd_kafka_mem_free(c[i].rk, member_ids[i]);
}

static int assign_calls(void) {
        int n;
        mtx_lock(&lock);
        n = scn.assign_calls;
        mtx_unlock(&lock);
        return n;
}

static void await_assign_calls(int cnt) {
        int64_t abs_timeout = test_clock() + (15 * 1000 * 1000);
        while (assign_calls() < cnt) {
                TEST_ASSERT(test_clock() < abs_timeout,
                            "timed out waiting for assign call %d", cnt);
                rd_usleep(10 * 1000, NULL);
        }
}

static void release_pending_threads(void) {
        mtx_lock(&lock);
        scn.hold_completion = rd_false;
        cnd_broadcast(&cnd);
        while (scn.threads > 0)
                cnd_wait(&cnd, &lock);
        mtx_unlock(&lock);
}

static int leader_index(struct consumer *c, int c_cnt) {
        int leader = -1;
        int i;
        for (i = 0; i < c_cnt; i++) {
                char *member_id;
                if (!c[i].rk)
                        continue;
                member_id = rd_kafka_memberid(c[i].rk);
                mtx_lock(&lock);
                if (!strcmp(member_id, scn.leader_id))
                        leader = i;
                mtx_unlock(&lock);
                rd_kafka_mem_free(c[i].rk, member_id);
        }
        TEST_ASSERT(leader >= 0, "leader not found");
        return leader;
}

static const char *mode_name(enum mode mode) {
        static const char *names[] = {
            "done",         "untouched member", "pending",
            "close during pending", "join during pending", "timeout",
            "fail sync",    "fail async",       "violation",
            "eager",
        };
        return names[mode];
}

static void do_test(enum mode mode) {
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
         * targets. Also three members with nothing owned: p mod 3 outright. */
        const int round3[C_CNT][PART_CNT] = {
            {1, 0, 0, 1}, {0, 1, 0, 0}, {0, 0, 1, 0}};
        /* One member left, owning everything. */
        const int alone[C_CNT][PART_CNT] = {{1, 1, 1, 1}};

        SUB_TEST_QUICK("%s", mode_name(mode));

        memset(&scn, 0, sizeof(scn));
        scn.mode = mode;
        memset(c, 0, sizeof(c));

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, TOPIC, PART_CNT, 1);
        rd_kafka_mock_group_initial_rebalance_delay_ms(mcluster, 0);

        test_conf_init(&conf, NULL, 90);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "partition.assignment.strategy", "stream");
        test_conf_set(conf, "session.timeout.ms", "3000");
        test_conf_set(conf, "heartbeat.interval.ms", "500");

        for (i = 0; i < C_CNT; i++) {
                rd_kafka_conf_t *cconf = rd_kafka_conf_dup(conf);
                rd_snprintf(c[i].name, sizeof(c[i].name), "c%d", i);
                test_conf_set(cconf, "client.id", c[i].name);
                rd_kafka_conf_set_assignor(
                    cconf, "stream",
                    mode == MODE_EAGER ? RD_KAFKA_ASSIGNOR_PROTOCOL_EAGER
                                       : RD_KAFKA_ASSIGNOR_PROTOCOL_COOPERATIVE,
                    subscription_cb, assign_cb, on_assignment_cb, &c[i]);
                c[i].rk = test_create_consumer(groupid, NULL, cconf, NULL);
        }
        rd_kafka_conf_destroy(conf);

        /* Round 1: two members. */
        for (i = 0; i < 2; i++)
                test_consumer_subscribe(c[i].rk, TOPIC);

        if (mode == MODE_CLOSE_DURING_PENDING) {
                /* The leader is closed and destroyed while its assignment
                 * is pending; the thread completes the stale handle after
                 * that, which must be a no-op. The other member is alone. */
                int leader;
                mtx_lock(&lock);
                scn.hold_completion = rd_true;
                mtx_unlock(&lock);
                await_assign_calls(1);
                leader = leader_index(c, 2);
                TEST_SAY("closing leader %s under its pending assignment\n",
                         c[leader].name);
                test_consumer_close(c[leader].rk);
                rd_kafka_destroy(c[leader].rk);
                c[leader].rk = NULL;
                release_pending_threads();
                {
                        const int survivor = leader == 0 ? 1 : 0;
                        int expected[C_CNT][PART_CNT] = {{0}};
                        memcpy(expected[survivor], alone[0],
                               sizeof(expected[survivor]));
                        await_assignments(c, 2, expected,
                                          "round 1, survivor alone");
                }
                check_userdata(c, 2);
                goto done;
        }

        if (mode == MODE_TIMEOUT) {
                /* The first assignment is held past session.timeout.ms
                 * (3s): the library gives it up and rejoins, the next
                 * assignment completes, and the late completion of the
                 * first is dropped. */
                mtx_lock(&lock);
                scn.hold_completion = rd_true;
                mtx_unlock(&lock);
                await_assign_calls(1);
                rd_usleep(4000 * 1000, NULL);
                await_assign_calls(2);
                release_pending_threads();
                await_assignments(c, 2, round1, "round 1 after a timeout");
                check_userdata(c, 2);
                goto done;
        }

        if (mode == MODE_JOIN_DURING_PENDING) {
                /* A member joins under the first (held) assignment; once
                 * released, that completion's SyncGroup is stale and the
                 * leader reassigns for three members. */
                mtx_lock(&lock);
                scn.hold_completion = rd_true;
                mtx_unlock(&lock);
                await_assign_calls(1);
                test_consumer_subscribe(c[2].rk, TOPIC);
                rd_usleep(500 * 1000, NULL);
                release_pending_threads();
                await_assignments(c, C_CNT, round3,
                                  "round 1, three members from nothing");
                check_userdata(c, C_CNT);
                goto done;
        }

        await_assignments(c, 2, round1, "round 1, two members");
        check_userdata(c, 2);
        mtx_lock(&lock);
        TEST_ASSERT(scn.seen_cnt == 2 && !strcmp(scn.seen[0], "sub:c0") &&
                        !strcmp(scn.seen[1], "sub:c1"),
                    "leader saw %d subscription userdata (%s, %s)",
                    scn.seen_cnt, scn.seen[0], scn.seen[1]);
        mtx_unlock(&lock);

        /* Round 2: a third member. Under COOPERATIVE the moving partitions
         * are withheld; in the violation scenario first moved outright,
         * which the library refuses, so the assignor runs again; in the
         * failing scenarios the first attempt fails and the group rejoins.
         * Under EAGER nothing is owned going in, so p mod 3 applies at once. */
        test_consumer_subscribe(c[2].rk, TOPIC);
        await_assignments(c, C_CNT, mode == MODE_EAGER ? round3 : round2,
                          "round 2, three members");
        check_userdata(c, C_CNT);
        mtx_lock(&lock);
        TEST_ASSERT(scn.seen_cnt == 3 && !strcmp(scn.seen[2], "none"),
                    "leader saw %d subscription userdata, third \"%s\"",
                    scn.seen_cnt, scn.seen[2]);
        if (mode == MODE_VIOLATION)
                TEST_ASSERT(scn.violation_members == 3 && scn.assign_calls >= 3,
                            "expected the violating 3-member assignment to be "
                            "rejected and rerun, saw %d members, %d calls",
                            scn.violation_members, scn.assign_calls);
        if (mode == MODE_FAIL_SYNC || mode == MODE_FAIL_ASYNC)
                TEST_ASSERT(scn.assign_calls >= 3,
                            "expected the failed assignment to be rerun, "
                            "saw %d calls",
                            scn.assign_calls);
        mtx_unlock(&lock);

        if (mode == MODE_EAGER)
                goto done;

        /* Round 3: the owners revoked the withheld partitions; the
         * cooperative follow-up rebalance lets the assignor hand them over. */
        await_assignments(c, C_CNT, round3, "round 3, follow-up");
        check_userdata(c, C_CNT);

done:
        release_pending_threads();
        for (i = 0; i < C_CNT; i++) {
                if (!c[i].rk)
                        continue;
                test_consumer_close(c[i].rk);
                rd_kafka_destroy(c[i].rk);
        }

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief The registration is copied by rd_kafka_conf_dup(), a strategy
 *        naming it (alone or with a built-in of the same protocol) is
 *        accepted, an unknown name or a mixed-protocol strategy is refused,
 *        and a name clashing with a built-in assignor fails rd_kafka_new().
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

        /* Alongside a built-in of the same protocol. */
        dup = rd_kafka_conf_dup(conf);
        test_conf_set(dup, "partition.assignment.strategy",
                      "stream,cooperative-sticky");
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, dup, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "rd_kafka_new with stream,cooperative-sticky: %s",
                    errstr);
        rd_kafka_destroy(rk);

        /* Mixed protocols are refused as for the built-ins. */
        dup = rd_kafka_conf_dup(conf);
        test_conf_set(dup, "partition.assignment.strategy", "stream,range");
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, dup, errstr, sizeof(errstr));
        TEST_ASSERT(!rk, "expected stream,range to fail");
        TEST_ASSERT(strstr(errstr, "protocol type"), "mixed protocols: %s",
                    errstr);
        rd_kafka_conf_destroy(dup);

        /* Not named: an unknown strategy still fails. */
        dup = rd_kafka_conf_dup(conf);
        test_conf_set(dup, "partition.assignment.strategy", "nosuch");
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, dup, errstr, sizeof(errstr));
        TEST_ASSERT(!rk, "expected an unknown strategy to fail");
        TEST_ASSERT(strstr(errstr, "Unsupported partition.assignment.strategy"),
                    "unknown strategy: %s", errstr);
        rd_kafka_conf_destroy(dup);

        /* A built-in name is refused. */
        dup = rd_kafka_conf_dup(conf);
        rd_kafka_conf_set_assignor(dup, "range",
                                   RD_KAFKA_ASSIGNOR_PROTOCOL_EAGER,
                                   subscription_cb, assign_cb,
                                   on_assignment_cb, NULL);
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, dup, errstr, sizeof(errstr));
        TEST_ASSERT(!rk, "expected a built-in name to be refused");
        TEST_ASSERT(strstr(errstr, "Failed to register application assignor"),
                    "built-in name: %s", errstr);
        rd_kafka_conf_destroy(dup);

        rd_kafka_conf_destroy(conf);

        SUB_TEST_PASS();
}

int main_0154_app_assignor_mock(int argc, char **argv) {
        enum mode mode;

        mtx_init(&lock, mtx_plain);
        cnd_init(&cnd);

        do_test_conf();
        for (mode = MODE_DONE; mode <= MODE_EAGER; mode++)
                do_test(mode);

        cnd_destroy(&cnd);
        mtx_destroy(&lock);
        return 0;
}
