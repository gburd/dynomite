/*
 * Oracle entry point for the C Redis response parser.
 *
 * oracle_redis_parse_rsp builds a response struct msg the way the C
 * allocator initialises one, places the input bytes in a single mbuf, runs
 * redis_parse_rsp once, and copies the observable parse outcome into a
 * plain struct the Rust side can read. Callers serialise calls: the mbuf
 * free list and the logger are process globals.
 */
#include <dyn_core.h>
#include <proto/dyn_proto.h>

#include <stdlib.h>
#include <string.h>

#define ORACLE_MAX_ARGS 64

struct oracle_rsp_out {
  int32_t result;
  int32_t type;
  uint32_t integer;
  int32_t is_error;
  uint32_t nkeys;
  uint32_t pos;
  uint32_t state;
  uint32_t nargs;
  uint32_t ntokens;
  uint32_t arg_start[ORACLE_MAX_ARGS];
  uint32_t arg_end[ORACLE_MAX_ARGS];
};

static bool oracle_read_repairs;
static bool oracle_ready;

bool is_read_repairs_enabled(void) { return oracle_read_repairs; }

bool is_msg_type_dyno_config(msg_type_t msg_type) {
  return msg_type == MSG_HACK_SETTING_CONN_CONSISTENCY;
}

void oracle_set_read_repairs(int enabled) { oracle_read_repairs = enabled != 0; }

#define DEFINE_ACTION(_name) #_name,
static const char *oracle_type_names[] = {MSG_TYPE_CODEC(DEFINE_ACTION)};
#undef DEFINE_ACTION

const char *oracle_msg_type_name(int32_t type) {
  if (type < 0 || type >= (int32_t)(sizeof(oracle_type_names) /
                                    sizeof(oracle_type_names[0]))) {
    return NULL;
  }
  return oracle_type_names[type];
}

uint32_t oracle_mbuf_capacity(void) { return MBUF_SIZE; }

static void oracle_init(void) {
  if (oracle_ready) {
    return;
  }
  log_init(LOG_EMERG, "/dev/null");
  mbuf_init(MBUF_SIZE);
  oracle_ready = true;
}

/* Field initialisation of a fresh response, as the C allocator does it. */
static struct msg *oracle_msg_new(void) {
  struct msg *msg = calloc(1, sizeof(*msg));
  if (msg == NULL) {
    return NULL;
  }
  init_object(&msg->object, OBJ_RSP, NULL);
  msg->id = 1;
  STAILQ_INIT(&msg->mhdr);
  msg->state = 0;
  msg->pos = NULL;
  msg->token = NULL;
  msg->latest_parsed_mbuf_idx = -1;
  msg->result = MSG_PARSE_OK;
  msg->type = MSG_UNKNOWN;
  msg->keys = array_create(1, sizeof(struct keypos));
  msg->args = array_create(1, sizeof(struct argpos));
  if (msg->keys == NULL || msg->args == NULL) {
    free(msg);
    return NULL;
  }
  msg->is_request = 0;
  msg->expect_datastore_reply = 1;
  msg->is_read = 1;
  msg->msg_routing = ROUTING_NORMAL;
  msg->consistency = DC_ONE;
  msg->orig_type = MSG_UNKNOWN;
  msg->rewrite_with_ts_possible = true;
  return msg;
}

/*
 * Parse `len` bytes at `data` (len <= MBUF_SIZE) the way a connection
 * receives them in two reads: the first `split` bytes are parsed, and when
 * that returns MSG_PARSE_AGAIN the rest is appended to the same mbuf and
 * the parse resumes from the saved state. Returns 0 on success, -1 when
 * the harness could not set up the call.
 */
int oracle_redis_parse_rsp_split(const uint8_t *data, uint32_t len,
                                 uint32_t split, struct oracle_rsp_out *out) {
  struct msg *r;
  struct mbuf *b;
  uint32_t i, n;

  oracle_init();
  if (len > MBUF_SIZE) {
    return -1;
  }
  r = oracle_msg_new();
  if (r == NULL) {
    return -1;
  }
  b = mbuf_get();
  if (b == NULL) {
    return -1;
  }
  if (split > len) {
    split = len;
  }
  memcpy(b->last, data, split);
  b->last += split;
  mbuf_insert(&r->mhdr, b);
  r->pos = b->pos;

  redis_parse_rsp(r, NULL);
  if (r->result == MSG_PARSE_AGAIN && split < len) {
    memcpy(b->last, data + split, len - split);
    b->last += len - split;
    redis_parse_rsp(r, NULL);
  }

  memset(out, 0, sizeof(*out));
  out->result = (int32_t)r->result;
  out->type = (int32_t)r->type;
  out->integer = r->integer;
  out->is_error = r->is_error ? 1 : 0;
  out->nkeys = r->nkeys;
  out->pos = (uint32_t)(r->pos - b->pos);
  out->state = (uint32_t)r->state;
  out->ntokens = r->ntokens;
  n = array_n(r->args);
  out->nargs = n;
  for (i = 0; i < n && i < ORACLE_MAX_ARGS; i++) {
    struct argpos *a = array_get(r->args, i);
    out->arg_start[i] = (uint32_t)(a->start - b->pos);
    out->arg_end[i] = (uint32_t)(a->end - b->pos);
  }

  mbuf_remove(&r->mhdr, b);
  mbuf_put(b);
  array_destroy(r->keys);
  array_destroy(r->args);
  free(r);
  return 0;
}

/* Parse `len` bytes at `data` in one read. */
int oracle_redis_parse_rsp(const uint8_t *data, uint32_t len,
                           struct oracle_rsp_out *out) {
  return oracle_redis_parse_rsp_split(data, len, len, out);
}
