#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static void execute(char **command, const char *prefix) {
    if (prefix == NULL) {
        execv(command[0], command);
        return;
    }
    char *root = realpath(prefix, NULL);
    if (root == NULL) return;
    if (strcmp(root, prefix) != 0) {
        errno = EACCES;
        return;
    }
    char *executable = realpath(command[0], NULL);
    if (executable == NULL) return;
    size_t length = strlen(root);
    if (strncmp(executable, root, length) != 0 || executable[length] != '/') {
        errno = EACCES;
        return;
    }
    execv(executable, command);
}

/* The existing command owner controls the process group and cancellation. */
int main(int argc, char **argv) {
    if (argc < 4 || strlen(argv[2]) != 32) return 125;
    for (const char *p = argv[2]; *p; ++p)
        if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f'))) return 125;
    const char *prefix = NULL;
    int command = 3;
    if (strcmp(argv[3], "--prefix") == 0) {
        if (argc < 6) return 125;
        prefix = argv[4];
        command = 5;
    }
    int channel[2];
    if (pipe2(channel, O_CLOEXEC) != 0) return 125;
    pid_t child = fork();
    if (child < 0) return 125;
    if (child == 0) {
        close(channel[0]);
        execute(&argv[command], prefix);
        int failure = errno;
        ssize_t written;
        do { written = write(channel[1], &failure, sizeof(failure)); } while (written < 0 && errno == EINTR);
        _exit(125);
    }
    close(channel[1]);
    int failure = 0;
    ssize_t count;
    do { count = read(channel[0], &failure, sizeof(failure)); } while (count < 0 && errno == EINTR);
    close(channel[0]);
    int status;
    while (waitpid(child, &status, 0) < 0) if (errno != EINTR) return 125;
    if (count != 0 && count != sizeof(failure)) return 125;
    if (!WIFEXITED(status) && !WIFSIGNALED(status)) return 125;
    const char *kind = count ? "exec_error" : WIFEXITED(status) ? "exit" : "signal";
    int code = count ? failure : WIFEXITED(status) ? WEXITSTATUS(status) : WTERMSIG(status);
    int descriptor = open(argv[1], O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
    if (descriptor < 0) return 125;
    int written = dprintf(descriptor, "{\"nonce\":\"%s\",\"kind\":\"%s\",\"code\":%d}\n", argv[2], kind, code);
    if (written < 0 || fsync(descriptor) != 0 || close(descriptor) != 0) return 125;
    return count ? 125 : WIFEXITED(status) ? code : 128 + code;
}
