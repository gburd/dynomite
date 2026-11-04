/*
 * Oracle entry points over one C mbuf (src/dyn_mbuf.c) and the static
 * inline string search helpers in src/dyn_string.h, so Rust tests can
 * drive them without mirroring struct mbuf or struct mhdr.
 */
#include <dyn_core.h>

static struct mbuf *ob;

static void ob_ensure(void) {
  static int inited;
  if (!inited) {
    mbuf_init(MBUF_SIZE);
    inited = 1;
  }
  if (ob == NULL) ob = mbuf_get();
}

void oracle_mbuf_reset(void) {
  ob_ensure();
  mbuf_rewind(ob);
}

int oracle_mbuf_copy(const uint8_t *d, uint32_t n) {
  ob_ensure();
  if (n > mbuf_remaining_space(ob)) return -1;
  mbuf_copy(ob, (uint8_t *)d, n);
  return 0;
}

uint32_t oracle_mbuf_length(void) { ob_ensure(); return mbuf_length(ob); }
int oracle_mbuf_empty(void) { ob_ensure(); return mbuf_empty(ob); }
void oracle_mbuf_rewind(void) { ob_ensure(); mbuf_rewind(ob); }

uint32_t oracle_mbuf_read(uint8_t *out) {
  ob_ensure();
  uint32_t n = mbuf_length(ob);
  memcpy(out, ob->pos, n);
  return n;
}

/* Split the mbuf at pos + at; copy the tail out, return its length or -1. */
int oracle_mbuf_split(uint32_t at, uint8_t *tail_out) {
  struct mhdr h;
  struct mbuf *nbuf;
  int n;
  ob_ensure();
  STAILQ_INIT(&h);
  mbuf_insert(&h, ob);
  nbuf = mbuf_split(&h, ob->pos + at, NULL, NULL);
  mbuf_remove(&h, ob);
  if (nbuf == NULL) return -1;
  n = (int)mbuf_length(nbuf);
  memcpy(tail_out, nbuf->pos, n);
  mbuf_put(nbuf);
  return n;
}

/*
 * Re-run mbuf_init(size) and report mbuf_data_size() plus the writable
 * (end - start) and data (end_extra - start) spans of a fresh mbuf,
 * then restore the default size the other entry points rely on.
 */
void oracle_mbuf_geometry(uint32_t size, uint32_t out[3]) {
  struct mbuf *m;
  ob_ensure();
  mbuf_init(size);
  out[0] = (uint32_t)mbuf_data_size();
  m = mbuf_get();
  out[1] = (uint32_t)(m->end - m->start);
  out[2] = (uint32_t)(m->end_extra - m->start);
  mbuf_dealloc(m);
  mbuf_init(MBUF_SIZE);
}

const uint8_t *oracle_strchr(const uint8_t *p, uint32_t n, uint8_t c) {
  return _dn_strchr((uint8_t *)p, (uint8_t *)p + n, c);
}

const uint8_t *oracle_strrchr(const uint8_t *p, uint32_t n, uint8_t c) {
  if (n == 0) return NULL;
  return _dn_strrchr((uint8_t *)p + n - 1, (uint8_t *)p, c);
}
