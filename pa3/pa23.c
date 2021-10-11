#include <sys/types.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <wait.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <sched.h>

#include "banking.h"
#include "pa2345.h"
#include "common.h"
#include "ipc.h"


struct ipc {
    local_id n;
    pid_t pid;
    pid_t ppid;
    local_id id;
    balance_t balance;
    FILE * const events_log;

    struct {
        int in;
        int out;
    } pipes[MAX_PROCESS_ID + 1];
};

// c99 limitation
int kill(pid_t pid, int sig);


static timestamp_t current_lamport_time = 0;

static bool parse_args(int argc, char * argv[], local_id * n, balance_t ** balances) {
    int opt;

    local_id x;
    bool p = false;
    while ((opt = getopt(argc, argv, "p:")) > 0) {
        if (opt == 'p') {
            x = (local_id) atoi(optarg);
            p = true;
        }
    }

    if (!p) {
        return false;
    }

    if (argc - optind < x) {
        return false;
    }

    *balances = malloc(sizeof(**balances) * *n);
    if (!*balances) {
        return false;
    }

    *n = x + 1;
    for (int i = 0; i < x; ++i, ++optind) {
        (*balances)[i] = atoi(argv[optind]);
    }

    return true;
}

static bool make_nonblock(int fd) {
    const int flag = fcntl(fd, F_GETFL);

    if (flag == -1) {
        return false;
    }

    if (fcntl(fd, F_SETFL, flag | O_NONBLOCK) != 0) {
        return false;
    }

    return true;
}

static bool open_pipes(int n, int ** in, int ** out) {
    *in = malloc(n * n * sizeof(int));

    if (!*in) {
        return false;
    }

    *out = malloc(n * n * sizeof(int));
    if (!*out) {
        goto free_in;
    }

    FILE * const pipes_log_file = fopen(pipes_log, "w+");
    if (pipes_log_file == NULL) {
        goto free_out;
    }

    int k = 0;
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            if (i == j) {
                (*in)[k] = -1;
                (*out)[k] = -1;
            } else {
                int pipefd[2];

                if (pipe(pipefd) < 0) {
                    goto close_pipes;
                }

                if (!make_nonblock(pipefd[0])) {
                    goto close_pipes;
                }

                if (!make_nonblock(pipefd[1])) {
                    goto close_pipes;
                }

                (*in)[k] = pipefd[0];
                (*out)[k] = pipefd[1];

                fprintf(pipes_log_file, "%d / %d\n", pipefd[0], pipefd[1]);
            }

            ++k;
        }
    }

    fclose(pipes_log_file);
    return true;

close_pipes:
    for (int i = 0; i < k; ++i) {
        if ((*in)[i] > 0) {
            close((*in)[i]);
        }

        if ((*out)[i] > 0) {
            close((*out)[i]);
        }
    }

// close_pipes_log:
    fclose(pipes_log_file);

free_out:
    free(*out);

free_in:
    free(*in);

    return false;
}

static void free_pipes(int n, int * in, int * out) {
    for (int i = 0, k = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j, ++k) {
            if (i == j) {
                continue;
            }

            if (in[k] > 0) {
                close(in[k]);
            }

            if (out[k] > 0) {
                close(out[k]);
            }
        }
    }

    free(in);
    free(out);
}

static void save_my_pipes(int n, int p, int * in, int * out, struct ipc * ipc) {
    for (int i = 0; i < n; ++i) {
        ipc->pipes[i].in = in[p * n + i];
        in[p * n + i] = -1;

        ipc->pipes[i].out = out[i * n + p];
        out[i * n + p] = -1;
    }
}

static void close_pipes(struct ipc * ipc) {
    for (int i = 0; i < ipc->n; ++i) {
        if (ipc->pipes[i].in > 0) {
            close(ipc->pipes[i].in);
        }

        if (ipc->pipes[i].out > 0) {
            close(ipc->pipes[i].out);
        }
    }
}

static bool printf_message(Message * msg, MessageType type, const char * format, va_list ap) {
    msg->s_header.s_magic = MESSAGE_MAGIC;
    msg->s_header.s_type = type;

    const int length = vsnprintf(msg->s_payload, MAX_PAYLOAD_LEN, format, ap);
    if (length < 0) {
        return false;
    }

    msg->s_header.s_payload_len = length;
    return true;
}

static bool send_printf_message_multicast(struct ipc * ipc, MessageType type, const char * format, ...) {
    Message msg;

    va_list ap;
    va_start(ap, format);
    bool res = printf_message(&msg, type, format, ap);
    va_end(ap);

    if (!res) {
        return false;
    }

    if (send_multicast(ipc, &msg)) {
        return false;
    }

    return true;
}

static void log_msg(FILE * log_file, const char * format, ...) {
    va_list ap;

    va_start(ap, format);
    vprintf(format, ap);
    va_end(ap);

    fflush(stdout);

    va_start(ap, format);
    vfprintf(log_file, format, ap);
    va_end(ap);

    fflush(log_file);
}

static int receive_blocking(const struct ipc * ipc, local_id id, Message * msg) {
    while (true) {
        const int ret = receive((void *) ipc, id, msg);

        if (ret != 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                sched_yield();
                continue;
            }
        }

        return ret;
    }
}

static int subprocess_main(struct ipc ipc) {
    int ret = 0;

    log_msg(ipc.events_log, log_started_fmt, get_lamport_time(), ipc.id, ipc.pid, ipc.ppid, ipc.balance);

    if (!send_printf_message_multicast(&ipc, STARTED, log_started_fmt, get_lamport_time(), ipc.id, ipc.pid, ipc.ppid, ipc.balance)) {
        ret = 1;
        goto end;
    }

    for (local_id id = 1; id < ipc.n; ++id) {
        if (id == ipc.id) {
            continue;
        }

        Message msg;
        if (receive_blocking(&ipc, id, &msg) < 0) {
            ret = 2;
            goto end;
        }

        assert(msg.s_header.s_type == STARTED);
    }

    log_msg(ipc.events_log, log_received_all_started_fmt, get_lamport_time(), ipc.id);

    size_t done = 1;
    bool current_done = false;
    BalanceHistory history = { .s_id = ipc.id, .s_history_len = 0 };
    while (true) {
        Message msg;

        if (receive_any(&ipc, &msg) != 0) {
            ret = 3;
            goto end;
        }

        const timestamp_t t = get_lamport_time();
        for (timestamp_t i = history.s_history_len; i < t; ++i) {
            history.s_history[i].s_time = i;
            history.s_history[i].s_balance = ipc.balance;
            history.s_history[i].s_balance_pending_in = 0;
        }

        history.s_history_len = t;

        switch (msg.s_header.s_type) {
            case TRANSFER: {
                const TransferOrder * const transfer = (TransferOrder *) msg.s_payload;

                assert((!current_done && ipc.id == transfer->s_src) || ipc.id == transfer->s_dst);

                if (ipc.id == transfer->s_src) {
                    ipc.balance -= transfer->s_amount;

                    if (send(&ipc, transfer->s_dst, &msg) != 0) {
                        ret = 4;
                        goto end;
                    }

                    log_msg(ipc.events_log, log_transfer_out_fmt, t, ipc.id, transfer->s_amount, transfer->s_dst);
                } else {
                    log_msg(ipc.events_log, log_transfer_in_fmt, t, ipc.id, transfer->s_amount, transfer->s_src);

                    ipc.balance += transfer->s_amount;

                    for (timestamp_t i = msg.s_header.s_local_time - 1; i < t; ++i) {
                        history.s_history[i].s_balance_pending_in += transfer->s_amount;
                    }

                    Message ack;
                    ack.s_header.s_magic = MESSAGE_MAGIC;
                    ack.s_header.s_type = ACK;
                    ack.s_header.s_payload_len = 0;

                    if (send(&ipc, PARENT_ID, &ack) != 0) {
                        ret = 5;
                        goto end;
                    }
                }

                break;
            }

            case STOP:
                assert(!current_done);

                current_done = true;
                ++done;

                log_msg(ipc.events_log, log_done_fmt, get_lamport_time(), ipc.id, ipc.balance);
                if (!send_printf_message_multicast(&ipc, DONE, log_done_fmt, get_lamport_time(), ipc.id, ipc.balance)) {
                    ret = 6;
                    goto end;
                }

                break;

            case DONE:
                ++done;

                if (done == ipc.n) {
                    goto break_while;
                }

                break;
        }
    }

break_while: ;
    history.s_history[history.s_history_len].s_time = history.s_history_len;
    history.s_history[history.s_history_len].s_balance = ipc.balance;
    history.s_history[history.s_history_len].s_balance_pending_in = 0;
    ++history.s_history_len;

    Message msg;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = BALANCE_HISTORY;
    msg.s_header.s_payload_len = offsetof(BalanceHistory, s_history) + sizeof(BalanceState) * history.s_history_len;
    memcpy(msg.s_payload, &history, msg.s_header.s_payload_len);

    if (send(&ipc, PARENT_ID, &msg) != 0) {
        ret = 7;
        goto end;
    }

    log_msg(ipc.events_log, log_received_all_done_fmt, get_lamport_time(), ipc.id);

end:
    close_pipes(&ipc);
    fclose(ipc.events_log);
    return ret;
}

static bool start_processes(local_id n, struct ipc * ipc, pid_t ** pids, const balance_t * balances) {
    *pids = malloc(n * sizeof(pid_t));

    if (!*pids) {
        return false;
    }

    (*pids)[0] = 0;

    FILE * const events_log_file = fopen(events_log, "a+");
    if (events_log_file == NULL) {
        goto free_pids;
    }

    int * in, * out;
    if (!open_pipes(n, &in, &out)) {
        goto close_events_log;
    }

    const pid_t parent_pid = getpid();

    int i = 1;
    for (; i < n; ++i) {
        const pid_t pid = fork();

        if (pid < 0) {
            goto end_processes;
        }

        if (pid == 0) {
            struct ipc child_ipc = {
                .n = n,
                .pid = getpid(),
                .ppid = parent_pid,
                .balance = balances[i - 1],
                .id = (local_id) i,
                .events_log = events_log_file,
            };

            save_my_pipes(n, i, in, out, &child_ipc);
            free_pipes(n, in, out);

            exit(subprocess_main(child_ipc));
        }

        (*pids)[i] = pid;
    }

    ipc->n = n;
    ipc->pid = parent_pid;
    ipc->ppid = parent_pid;
    ipc->id = PARENT_ID;

    save_my_pipes(n, PARENT_ID, in, out, ipc);
    free_pipes(n, in, out);

    fclose(events_log_file);
    return true;

end_processes:
    for (int j = 1; j < i; ++j) {
        kill((*pids)[j], SIGKILL);
    }

// free_pipes:
    free_pipes(n, in, out);

close_events_log:
    fclose(events_log_file);

free_pids:
    free(*pids);

    return false;
}

static bool main_work(struct ipc * ipc, const pid_t * pids) {
    for (local_id id = 1; id < ipc->n; ++id) {
        Message msg;

        if (receive_blocking(ipc, id, &msg) < 0) {
            return false;
        }

        assert(msg.s_header.s_type == STARTED);
    }

    bank_robbery(ipc, ipc->n - 1);

    {
        Message msg;

        msg.s_header.s_magic = MESSAGE_MAGIC;
        msg.s_header.s_type = STOP;
        msg.s_header.s_payload_len = 0;

        if (send_multicast(ipc, &msg) < 0) {
            return false;
        }
    }

    for (local_id id = 1; id < ipc->n; ++id) {
        Message msg;

        if (receive_blocking(ipc, id, &msg) < 0) {
            return false;
        }

        assert(msg.s_header.s_type == DONE);
    }

    AllHistory allHistory;
    allHistory.s_history_len = ipc->n - 1;

    for (local_id id = 1; id < ipc->n; ++id) {
        Message msg;

        if (receive_blocking(ipc, id, &msg) < 0) {
            return false;
        }

        assert(msg.s_header.s_type == BALANCE_HISTORY);

        const BalanceHistory * const history = (BalanceHistory *) msg.s_payload;

        allHistory.s_history[id - 1].s_id = history->s_id;
        allHistory.s_history[id - 1].s_history_len = history->s_history_len;

        for (size_t i = 0; i < history->s_history_len; ++i) {
            allHistory.s_history[id - 1].s_history[i] = history->s_history[i];
        }
    }

    print_history(&allHistory);

    for (int i = 1; i < ipc->n; ++i) {
        if (waitpid(pids[i], NULL, 0) < 0) {
            if (errno == ECHILD) {
                break;
            }

            return false;
        }
    }

    return true;
}

int main(int argc, char * argv[]) {
    local_id n;

    balance_t * balances;
    if (!parse_args(argc, argv, &n, &balances)) {
        return 1;
    }

    pid_t * pids;
    struct ipc ipc;
    if (!start_processes(n, &ipc, &pids, balances)) {
        perror("Cannot create processes");
        free(balances);
        return 2;
    }

    if (!main_work(&ipc, pids)) {
        perror("Error");
        close_pipes(&ipc);
        free(pids);
        free(balances);
        return 3;
    }

    close_pipes(&ipc);
    free(pids);
    free(balances);
    return 0;
}

static bool write_full(int fd, const void * buf, size_t remaining) {
    const uint8_t * ptr = buf;

    if (remaining == 0) {
        return true;
    }

    errno = 0;
    while (true) {
        const ssize_t wrote = write(fd, ptr, remaining);

        if (wrote < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                sched_yield();
                continue;
            }

            break;
        }

        remaining -= wrote;
        ptr += wrote;

        if (remaining == 0) {
            return true;
        }
    }

    return false;
}

static int do_send(const struct ipc * ipc, local_id dst, const Message * msg) {
    if (!write_full(ipc->pipes[dst].out, msg, sizeof(MessageHeader) + msg->s_header.s_payload_len)) {
        return -1;
    }

    return 0;
}

static void prepare_to_send(const Message * msg, Message * new_msg) {
    ++current_lamport_time;

    new_msg->s_header = msg->s_header;
    new_msg->s_header.s_local_time = current_lamport_time;
    memcpy(new_msg->s_payload, msg->s_payload, msg->s_header.s_payload_len);
}

int send(void * self, local_id dst, const Message * msg) {
    Message new_msg;

    prepare_to_send(msg, &new_msg);
    return do_send(self, dst, &new_msg);
}

int send_multicast(void * self, const Message * msg) {
    struct ipc * const ipc = self;
    Message new_msg;

    prepare_to_send(msg, &new_msg);
    for (local_id id = 0; id < ipc->n; ++id) {
        if (id == ipc->id) {
            continue;
        }

        if (do_send(ipc, id, &new_msg) != 0) {
            return -1;
        }
    }

    return 0;
}

static bool read_full(int fd, void * buf, size_t remaining) {
    uint8_t * ptr = buf;

    if (remaining == 0) {
        return true;
    }

    errno = 0;
    while (true) {
        const ssize_t bytes_read = read(fd, ptr, remaining);

        if (bytes_read < 0) {
            if (ptr != buf) {
                if (errno == EWOULDBLOCK || errno == EPIPE) {
                    sched_yield();
                    continue;
                }
            }

            break;
        }

        if (bytes_read == 0) {
            if (errno == 0) {
                errno = EPIPE;
            }

            break;
        }

        remaining -= bytes_read;
        ptr += bytes_read;

        if (remaining == 0) {
            return true;
        }
    }

    return false;
}

int receive(void * self, local_id from, Message * msg) {
    struct ipc * const ipc = self;

    if (!read_full(ipc->pipes[from].in, &(msg->s_header), sizeof(MessageHeader))) {
        return -1;
    }

    while (!read_full(ipc->pipes[from].in, msg->s_payload, msg->s_header.s_payload_len)) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            sched_yield();
            continue;
        }

        return -2;
    }

    if (current_lamport_time < msg->s_header.s_local_time) {
        current_lamport_time = msg->s_header.s_local_time;
    }

    ++current_lamport_time;
    return 0;
}

int receive_any(void * self, Message * msg) {
    struct ipc * const ipc = self;

    while (true) {
        for (local_id id = 0; id < ipc->n; ++id) {
            if (id == ipc->id) {
                continue;
            }

            if (receive(ipc, id, msg) == 0) {
                return 0;
            }

            if (errno != EPIPE && errno != EWOULDBLOCK && errno != EAGAIN) {
                return -1;
            }
        }

        sched_yield();
    }
}

void transfer(void * parent_data, local_id src, local_id dst, balance_t amount) {
    const struct ipc * ipc = parent_data;

    assert(ipc->id == PARENT_ID);

    Message msg;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = TRANSFER;
    msg.s_header.s_payload_len = sizeof(TransferOrder);
    TransferOrder * const transfer = (TransferOrder *) msg.s_payload;
    transfer->s_src = src;
    transfer->s_dst = dst;
    transfer->s_amount = amount;
    send(parent_data, src, &msg);

    receive_blocking(parent_data, dst, &msg);
    assert(msg.s_header.s_type == ACK);
}

timestamp_t get_lamport_time() {
    return current_lamport_time;
}
