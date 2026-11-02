/*
 * Link-only stand-ins for C Dynomite symbols that oracle-compiled
 * translation units reference but the oracled functions never call.
 *
 * dyn_vnode.c defines both vnode_update (which calls server_get_dc and
 * server_get_rack from dyn_server.c) and vnode_dispatch (pure ring lookup,
 * the function under test). A static archive member is all-or-nothing, so
 * linking vnode_dispatch drags in the references from vnode_update. These
 * definitions satisfy the linker; reaching one at run time is a harness bug
 * and aborts.
 */
#include <stdlib.h>

struct server_pool;
struct datacenter;
struct rack;
struct string;

struct datacenter *server_get_dc(struct server_pool *pool,
                                 struct string *dcname) {
  (void)pool;
  (void)dcname;
  abort();
}

struct rack *server_get_rack(struct datacenter *dc, struct string *rackname) {
  (void)dc;
  (void)rackname;
  abort();
}
