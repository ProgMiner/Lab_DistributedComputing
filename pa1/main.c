#include <sys/types.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <wait.h>
#include <errno.h>
#include <sched.h>
#include <assert.h>

#include "common.h"
#include "ipc.h"
#include "pa1.h"


// c99 limitation
int kill(pid_t pid, int sig);

struct ipc {
    local_id n;
    pid_t pid;
    pid_t ppid;
    local_id id;
    FILE * const events_log;

    struct {
        int in;
        int out;
    } pipes[MAX_PROCESS_ID + 1];
};

static bool parse_args(int argc, char * argv[], local_id * n) {
    int opt;

    while ((opt = getopt(argc, argv, "p:")) > 0) {
        if (opt == 'p') {
            *n = (local_id) (atoi(optarg) + 1);
            return true;
        }
    }

    return false;
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

    va_start(ap, format);
    vfprintf(log_file, format, ap);
    va_end(ap);
}

static int subprocess_main(struct ipc ipc) {
    int ret = 0;

    log_msg(ipc.events_log, log_started_fmt, ipc.id, ipc.pid, ipc.ppid);

    if (!send_printf_message_multicast(&ipc, STARTED, log_started_fmt, ipc.id, ipc.pid, ipc.ppid)) {
        perror("Error in subprocess (1)");
        ret = 1;
        goto end;
    }

    for (local_id id = 1; id < ipc.n; ++id) {
        if (id == ipc.id) {
            continue;
        }


        Message msg;
        if (receive(&ipc, id, &msg) < 0) {
            perror("Error in subprocess (2)");
            ret = 2;
            goto end;
        }

        assert(msg.s_header.s_type == STARTED);
    }

    log_msg(ipc.events_log, log_received_all_started_fmt, ipc.id);

    log_msg(ipc.events_log, log_done_fmt, ipc.id);

    if (!send_printf_message_multicast(&ipc, DONE, log_done_fmt, ipc.id)) {
        perror("Error in subprocess (3)");
        ret = 3;
        goto end;
    }

    for (local_id id = 1; id < ipc.n; ++id) {
        if (id == ipc.id) {
            continue;
        }


        Message msg;
        if (receive(&ipc, id, &msg) < 0) {
            perror("Error in subprocess (2)");
            ret = 2;
            goto end;
        }

        assert(msg.s_header.s_type == DONE);
    }

    log_msg(ipc.events_log, log_received_all_done_fmt, ipc.id);

end:
    close_pipes(&ipc);
    fclose(ipc.events_log);
    return ret;
}

static bool start_processes(local_id n, struct ipc * ipc, pid_t ** pids) {
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

static bool await_processes(struct ipc * ipc, const pid_t * pids) {
    for (local_id id = 1; id < ipc->n; ++id) {
        Message msg;

        if (receive(ipc, id, &msg) < 0) {
            return false;
        }

        assert(msg.s_header.s_type == STARTED);
    }

    for (local_id id = 1; id < ipc->n; ++id) {
        Message msg;

        if (receive(ipc, id, &msg) < 0) {
            return false;
        }

        assert(msg.s_header.s_type == DONE);
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

    if (!parse_args(argc, argv, &n)) {
        return 1;
    }

    pid_t * pids;
    struct ipc ipc;
    if (!start_processes(n, &ipc, &pids)) {
        perror("Cannot create processes");
        return 2;
    }

    if (!await_processes(&ipc, pids)) {
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
    ssize_t wrote;

    errno = 0;
    while ((wrote = write(fd, ptr, remaining)) >= 0) {
        remaining -= wrote;
        ptr += wrote;

        if (remaining == 0) {
            return true;
        }
    }

    return false;
}

int send(void * self, local_id dst, const Message * msg) {
    struct ipc * const ipc = self;

    if (!write_full(ipc->pipes[dst].out, msg, sizeof(MessageHeader) + msg->s_header.s_payload_len)) {
        return -1;
    }

    return 0;
}

int send_multicast(void * self, const Message * msg) {
    struct ipc * const ipc = self;

    for (local_id id = 0; id < ipc->n; ++id) {
        if (id == ipc->id) {
            continue;
        }

        if (send(ipc, id, msg) != 0) {
            return -1;
        }
    }

    return 0;
}

static bool read_full(int fd, void * buf, size_t remaining) {
    uint8_t * ptr = buf;
    ssize_t bytes_read;

    errno = 0;
    while ((bytes_read = read(fd, ptr, remaining)) > 0) {
        remaining -= bytes_read;
        ptr += bytes_read;

        if (remaining == 0) {
            return true;
        }
    }

    if (bytes_read == 0 && errno == 0) {
        errno = EPIPE;
    }

    return false;
}

int receive(void * self, local_id from, Message * msg) {
    struct ipc * const ipc = self;

    if (!read_full(ipc->pipes[from].in, &(msg->s_header), sizeof(MessageHeader))) {
        return -1;
    }

    if (!read_full(ipc->pipes[from].in, msg->s_payload, msg->s_header.s_payload_len)) {
        return -2;
    }

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
                perror("ERROR receive_any");
                return -1;
            }

            sched_yield();
        }
    }
}
