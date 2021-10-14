#include <sys/types.h>
#include <inttypes.h>
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


#define MUTEX_QUEUE_SIZE (MAX_PROCESS_ID + 1)

struct mutex_queue {
    struct {
        local_id id;
        timestamp_t time;
    } buffer[MUTEX_QUEUE_SIZE];

    local_id amount;
};

struct ipc {
    local_id n;
    pid_t pid;
    pid_t ppid;
    local_id id;
    bool use_mutex;
    struct mutex_queue queue;
    FILE * const events_log;
    struct {
        local_id started;
        local_id done;
        local_id replies;
    } context;

    struct {
        int in;
        int out;
    } pipes[MAX_PROCESS_ID + 1];
};

// c99 limitation
int kill(pid_t pid, int sig);


static timestamp_t current_lamport_time = 0;

static bool parse_args(int argc, char * argv[], local_id * n, bool * use_mutex) {
    opterr = 0;

    int opt;
    bool p = false;
    while ((opt = getopt(argc, argv, "p:")) > 0) {
        if (opt == 'p') {
            *n = (local_id) (atoi(optarg) + 1);
            p = true;
            break;
        }
    }

    if (!p) {
        return false;
    }

    for (int i = 1; i < argc; ++i) {
        if (strcmp("--mutexl", argv[i]) == 0) {
            *use_mutex = true;
        }
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

static bool mutex_queue_empty(const struct mutex_queue * queue) {
    return queue->amount == 0;
}

static local_id mutex_queue_find_min(const struct mutex_queue * queue) {
    assert(queue->amount > 0);

    local_id min_i;
    timestamp_t min_t = 32767;
    local_id min_id = MAX_PROCESS_ID;
    for (local_id i = 0; i < queue->amount; ++i) {
        if (min_t < queue->buffer[i].time) {
            continue;
        }

        if (min_t == queue->buffer[i].time && min_id < queue->buffer[i].id) {
            continue;
        }

        min_t = queue->buffer[i].time;
        min_id = queue->buffer[i].id;
        min_i = i;
    }

    return min_i;
}

static local_id mutex_queue_peek(const struct mutex_queue * queue) {
    return queue->buffer[mutex_queue_find_min(queue)].id;
}

static void mutex_queue_push(struct mutex_queue * queue, local_id id, timestamp_t t) {
    assert(queue->amount <= MAX_PROCESS_ID);

    queue->buffer[queue->amount].id = id;
    queue->buffer[queue->amount].time = t;
    ++queue->amount;
}

static void mutex_queue_pop(struct mutex_queue * queue, local_id id) {
    const local_id min = mutex_queue_find_min(queue);
    assert(queue->buffer[min].id == id);

    --queue->amount;
    queue->buffer[min] = queue->buffer[queue->amount];
}

static bool receive_next_message(struct ipc * ipc, Message * msg, local_id * src) {
    const int ret = receive_any(ipc, msg);

    if (ret < 0) {
        return false;
    }

    if (src) {
        *src = (local_id) ret;
    }

    return true;
}

static bool handle_next_message(struct ipc * ipc) {
    local_id src;
    Message msg;

    if (!receive_next_message(ipc, &msg, &src)) {
        return false;
    }

    switch (msg.s_header.s_type) {
        case STARTED:
            ++ipc->context.started;
            break;

        case DONE:
            ++ipc->context.done;
            break;

        case CS_REQUEST:
            mutex_queue_push(&(ipc->queue), src, msg.s_header.s_local_time);

            {
                Message reply;
                reply.s_header.s_magic = MESSAGE_MAGIC;
                reply.s_header.s_type = CS_REPLY;
                reply.s_header.s_payload_len = 0;

                if (send(ipc, src, &reply) < 0) {
                    return false;
                }
            }

            break;

        case CS_REPLY:
            ++ipc->context.replies;
            break;

        case CS_RELEASE:
            mutex_queue_pop(&(ipc->queue), src);
            break;

        default:
            errno = EINVAL;
            return false;
    }

    return true;
}

static bool phase_started(struct ipc * ipc) {
    log_msg(ipc->events_log, log_started_fmt, get_lamport_time(), ipc->id, ipc->pid, ipc->ppid, 0);

    if (!send_printf_message_multicast(ipc, STARTED, log_started_fmt, get_lamport_time(), ipc->id, ipc->pid, ipc->ppid, 0)) {
        return false;
    }

    // N - parent - me
    const local_id needed_started = (local_id) (ipc->n - 2);
    while (ipc->context.started < needed_started) {
        if (!handle_next_message(ipc)) {
            return false;
        }
    }

    log_msg(ipc->events_log, log_received_all_started_fmt, get_lamport_time(), ipc->id);
    return true;
}

static bool phase_work(struct ipc * ipc) {
    const unsigned int print_amount = ipc->id * 5;

    for (unsigned int i = 1; i <= print_amount; ++i) {
        if (ipc->use_mutex) {
            if (request_cs(ipc) < 0) {
                return false;
            }
        }

        char str[256];
        snprintf(str, 256, log_loop_operation_fmt, ipc->id, i, print_amount);
        print(str);

        if (ipc->use_mutex) {
            if (release_cs(ipc) < 0) {
                return false;
            }
        }
    }

    return true;
}

static bool phase_done(struct ipc * ipc) {
    log_msg(ipc->events_log, log_done_fmt, get_lamport_time(), ipc->id, 0);

    if (!send_printf_message_multicast(ipc, DONE, log_done_fmt, get_lamport_time(), ipc->id, 0)) {
        return false;
    }

    // N - parent - me
    const local_id needed_done = (local_id) (ipc->n - 2);
    while (ipc->context.done < needed_done) {
        if (!handle_next_message(ipc)) {
            return false;
        }
    }

    log_msg(ipc->events_log, log_received_all_done_fmt, get_lamport_time(), ipc->id);
    return true;
}

static int subprocess_main(struct ipc ipc) {
    int ret = 0;

    if (!phase_started(&ipc)) {
        ret = -1;
        goto end;
    }

    if (!phase_work(&ipc)) {
        ret = -2;
        goto end;
    }

    if (!phase_done(&ipc)) {
        ret = -3;
        goto end;
    }

end:
    close_pipes(&ipc);
    fclose(ipc.events_log);
    return ret;
}

static bool start_processes(local_id n, struct ipc * ipc, pid_t ** pids, bool use_mutex) {
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
                .id = (local_id) i,
                .use_mutex = use_mutex,
                .queue = { .amount = 0, },
                .events_log = events_log_file,
                .context = { 0 },
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
    ipc->use_mutex = use_mutex;

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
    const local_id needed = (local_id) (ipc->n - 1);
    local_id started = 0, done = 0;

    while (started < needed || done < needed) {
        local_id src;
        Message msg;

        if (!receive_next_message(ipc, &msg, &src)) {
            return false;
        }

        switch (msg.s_header.s_type) {
            case STARTED:
                ++started;
                break;

            case DONE:
                ++done;
                break;
        }
    }

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

    bool use_mutex;
    if (!parse_args(argc, argv, &n, &use_mutex)) {
        return 1;
    }

    pid_t * pids;
    struct ipc ipc;
    if (!start_processes(n, &ipc, &pids, use_mutex)) {
        perror("Cannot create processes");
        return 2;
    }

    if (!main_work(&ipc, pids)) {
        perror("Error");
        close_pipes(&ipc);
        free(pids);
        return 3;
    }

    close_pipes(&ipc);
    free(pids);
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
                return id;
            }

            if (errno != EPIPE && errno != EWOULDBLOCK && errno != EAGAIN) {
                return -1;
            }
        }

        sched_yield();
    }
}

int request_cs(const void * self) {
    struct ipc * ipc = (struct ipc *) self;

    {
        Message msg;
        msg.s_header.s_magic = MESSAGE_MAGIC;
        msg.s_header.s_type = CS_REQUEST;
        msg.s_header.s_payload_len = 0;

        const int ret = send_multicast(ipc, &msg);
        if (ret < 0) {
            return ret;
        }
    }

    mutex_queue_push(&(ipc->queue), ipc->id, get_lamport_time());
    ipc->context.replies = 0;

    // N - parent - me
    const local_id needed_replies = (local_id) (ipc->n - 2);
    while (ipc->context.replies < needed_replies || mutex_queue_peek(&(ipc->queue)) != ipc->id) {
        if (!handle_next_message(ipc)) {
            return -1;
        }
    }

    return 0;
}

int release_cs(const void * self) {
    struct ipc * ipc = (struct ipc *) self;

    if (mutex_queue_empty(&(ipc->queue))) {
        return -1;
    }

    if (mutex_queue_peek(&(ipc->queue)) != ipc->id) {
        return -2;
    }

    mutex_queue_pop(&(ipc->queue), ipc->id);

    Message msg;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = CS_RELEASE;
    msg.s_header.s_payload_len = 0;

    return send_multicast(ipc, &msg);
}

timestamp_t get_lamport_time() {
    return current_lamport_time;
}
