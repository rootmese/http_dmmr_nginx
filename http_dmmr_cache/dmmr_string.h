#ifndef DMMR_STRING_H
#define DMMR_STRING_H

#include <stddef.h>

#ifdef DMMR_HAVE_STRLCPY
#include <string.h>
#else
/*
 * Copies at most siz - 1 bytes and always NUL-terminates when siz > 0.
 * Returns the length of src; a return value >= siz indicates truncation.
 */
size_t strlcpy(char *restrict dst, const char *restrict src, size_t siz);
#endif

#endif /* DMMR_STRING_H */
