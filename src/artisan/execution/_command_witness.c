#define _GNU_SOURCE
#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <unistd.h>

#define ACCESS_LIMIT (16 * 1024 * 1024)
#define ACCESS_RECEIPT ".artisan-external-files.json"

struct cursor { const unsigned char *next, *end; };
struct member { char *path; uint64_t size; int found; };

static int number(struct cursor *input, size_t width, uint64_t *value) {
    if ((size_t)(input->end - input->next) < width) return 0;
    *value = 0;
    for (size_t i = 0; i < width; ++i) *value = (*value << 8) | *input->next++;
    return 1;
}

static int field(struct cursor *input, const unsigned char **bytes, size_t *size) {
    uint64_t length;
    if (!number(input, 4, &length) || length > (uint64_t)(input->end - input->next)) return 0;
    *bytes = input->next;
    *size = (size_t)length;
    input->next += length;
    return 1;
}

static char *path_field(struct cursor *input, int absolute) {
    const unsigned char *bytes;
    size_t length;
    if (!field(input, &bytes, &length) || length == 0 || length >= PATH_MAX || memchr(bytes, 0, length)) return NULL;
    char *path = malloc(length + 1);
    if (path == NULL) return NULL;
    memcpy(path, bytes, length);
    path[length] = 0;
    if ((path[0] == '/') != absolute || path[length - 1] == '/' || strstr(path, "//") || strchr(path, '\\')) goto invalid;
    const char *component = path + absolute;
    while (*component) {
        size_t width = strcspn(component, "/");
        if ((width == 1 && component[0] == '.') || (width == 2 && strncmp(component, "..", 2) == 0)) goto invalid;
        if (!absolute && strncmp(component, ACCESS_RECEIPT, width) == 0 && width == strlen(ACCESS_RECEIPT)) goto invalid;
        component += width;
        if (*component) ++component;
    }
    return path;
invalid:
    free(path);
    return NULL;
}

static int receipt_matches(int root, const unsigned char *expected, size_t size) {
    int file = openat(root, ACCESS_RECEIPT, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (file < 0) return 0;
    struct stat info;
    int valid = fstat(file, &info) == 0 && S_ISREG(info.st_mode) && info.st_size >= 0 && (uint64_t)info.st_size == size;
    unsigned char buffer[4096];
    size_t offset = 0;
    while (valid && offset < size) {
        size_t length = size - offset < sizeof(buffer) ? size - offset : sizeof(buffer);
        ssize_t count = read(file, buffer, length);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0 || memcmp(buffer, expected + offset, (size_t)count) != 0) valid = 0;
        else offset += (size_t)count;
    }
    close(file);
    return valid;
}

static struct member *find_member(struct member *members, size_t count, const char *path) {
    size_t left = 0, right = count;
    while (left < right) {
        size_t middle = left + (right - left) / 2;
        int order = strcmp(members[middle].path, path);
        if (order == 0) return &members[middle];
        if (order < 0) left = middle + 1;
        else right = middle;
    }
    return NULL;
}

static int expected_directory(struct member *members, size_t count, const char *path) {
    size_t length = strlen(path);
    for (size_t i = 0; i < count; ++i)
        if (strncmp(members[i].path, path, length) == 0 && members[i].path[length] == '/') return 1;
    return 0;
}

static int layout_matches(int root, const char *prefix, struct member *members, size_t count) {
    int scan = dup(root);
    if (scan < 0) return 0;
    DIR *directory = fdopendir(scan);
    if (directory == NULL) { close(scan); return 0; }
    int valid = 1;
    struct dirent *entry;
    errno = 0;
    while (valid && (entry = readdir(directory)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;
        if (*prefix == 0 && strcmp(entry->d_name, ACCESS_RECEIPT) == 0) continue;
        size_t length = strlen(prefix) + strlen(entry->d_name) + 2;
        if (length > PATH_MAX) { valid = 0; break; }
        char *path = malloc(length);
        if (path == NULL) { valid = 0; break; }
        snprintf(path, length, "%s%s%s", prefix, *prefix ? "/" : "", entry->d_name);
        struct stat info;
        valid = fstatat(root, entry->d_name, &info, AT_SYMLINK_NOFOLLOW) == 0;
        if (valid && S_ISREG(info.st_mode)) {
            struct member *member = find_member(members, count, path);
            valid = member != NULL && !member->found && info.st_size >= 0 && member->size == (uint64_t)info.st_size;
            if (valid) member->found = 1;
        } else if (valid && S_ISDIR(info.st_mode)) {
            valid = expected_directory(members, count, path);
            int child = valid ? openat(root, entry->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC) : -1;
            valid = child >= 0 && layout_matches(child, path, members, count);
            if (child >= 0) close(child);
        } else valid = 0;
        free(path);
        errno = 0;
    }
    if (errno != 0) valid = 0;
    closedir(directory);
    return valid;
}

static int verify_installation(struct cursor *input) {
    char *path = path_field(input, 1);
    const unsigned char *receipt;
    size_t receipt_size;
    uint64_t count;
    if (path == NULL) return 0;
    int valid = field(input, &receipt, &receipt_size) && receipt_size > 0 && number(input, 4, &count) && count > 0 && count <= ACCESS_LIMIT / 12;
    struct member *members = valid ? calloc((size_t)count, sizeof(*members)) : NULL;
    if (members == NULL) { free(path); return 0; }
    for (size_t i = 0; valid && i < count; ++i) {
        members[i].path = path_field(input, 0);
        valid = members[i].path != NULL && number(input, 8, &members[i].size) && members[i].size <= INT64_MAX;
        if (valid && i > 0) valid = strcmp(members[i - 1].path, members[i].path) < 0;
    }
    char *canonical = valid ? realpath(path, NULL) : NULL;
    valid = canonical != NULL && strcmp(canonical, path) == 0;
    free(canonical);
    int root = valid ? open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC) : -1;
    struct statvfs mount;
    valid = root >= 0 && fstatvfs(root, &mount) == 0 && (mount.f_flag & ST_RDONLY) != 0;
    if (valid) valid = receipt_matches(root, receipt, receipt_size) && layout_matches(root, "", members, (size_t)count);
    for (size_t i = 0; i < count; ++i) {
        if (!members[i].found) valid = 0;
        free(members[i].path);
    }
    if (root >= 0) close(root);
    free(members);
    free(path);
    return valid;
}

static int verify_external_access(const char *path) {
    int file = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (file < 0) return 0;
    struct stat info;
    int valid = fstat(file, &info) == 0 && S_ISREG(info.st_mode) && info.st_size >= 12 && info.st_size <= ACCESS_LIMIT;
    unsigned char *bytes = valid ? malloc((size_t)info.st_size) : NULL;
    if (bytes == NULL) { close(file); return 0; }
    size_t offset = 0;
    while (valid && offset < (size_t)info.st_size) {
        ssize_t count = read(file, bytes + offset, (size_t)info.st_size - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) valid = 0;
        else offset += (size_t)count;
    }
    close(file);
    struct cursor input = { bytes + 8, bytes + info.st_size };
    uint64_t count;
    valid = valid && memcmp(bytes, "ARTEXT1\0", 8) == 0 && number(&input, 4, &count) && count > 0 && count <= ACCESS_LIMIT / 16;
    for (uint64_t i = 0; valid && i < count; ++i) valid = verify_installation(&input);
    valid = valid && input.next == input.end;
    free(bytes);
    return valid;
}

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
    if (strcmp(argv[command], "--external-access") == 0) {
        if (argc < command + 3 || !verify_external_access(argv[command + 1])) {
            fputs("External artifact access check failed before command execution\n", stderr);
            return 125;
        }
        command += 2;
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
