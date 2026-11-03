/*
 * Link-only stand-ins for symbols src/proto/dyn_redis.c references from
 * the message, response-manager, and dnode layers. redis_parse_rsp never
 * calls any of them; the rest of dyn_redis.c (fragmenting, coalescing,
 * repair) does, and a static archive member links all or nothing.
 * Reaching one at run time is a harness bug and aborts.
 */
#include <stdlib.h>

#define ORACLE_LINK_STUB(name) \
  void name(void);             \
  void name(void) { abort(); }

ORACLE_LINK_STUB(msg_get)
ORACLE_LINK_STUB(msg_put)
ORACLE_LINK_STUB(msg_append)
ORACLE_LINK_STUB(msg_ensure_mbuf)
ORACLE_LINK_STUB(msg_prepend_format)
ORACLE_LINK_STUB(msg_gen_frag_id)
ORACLE_LINK_STUB(msg_get_full_key_copy)
ORACLE_LINK_STUB(msg_payload_crc32)
ORACLE_LINK_STUB(msg_dump)
ORACLE_LINK_STUB(msg_get_error)
ORACLE_LINK_STUB(rsp_get)
ORACLE_LINK_STUB(rsp_put)
ORACLE_LINK_STUB(rspmgr_clone_responses)
ORACLE_LINK_STUB(dnode_peer_idx_for_key_on_rack)
