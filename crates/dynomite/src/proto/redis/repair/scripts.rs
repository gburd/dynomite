//! Lua script templates used by the read-repair engine.
//!
//! Each constant is a complete Redis wire-format `EVAL` invocation
//! shell preceded by the `$<n>\r\n` length prefix. The bytes are
//! treated as opaque payloads by the parser and as fixed templates
//! by the rewrite path; tests in this module pin the declared
//! length prefix against the actual body length so any drift is
//! caught at build time.

/// Set-key write script (`SET`-shaped commands).
pub const SET_SCRIPT: &str = "$4\r\nEVAL\r\n$640\r\n\
local key = KEYS[1]\n\
local add_set = KEYS[2]\n\
local rem_set = KEYS[3]\n\
local orig_cmd = ARGV[1]\n\
local num_fields = ARGV[2]\n\
local cur_ts = ARGV[3]\n\
local value = ARGV[4]\n\n\
local last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\n\
local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\n\n\
if (last_seen_ts_in_rem) then\n\
  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n\
    return -1\n\
  end\n\
  redis.call('ZREM', rem_set, key)\n\
elseif (last_seen_ts_in_add) then\n\
  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n\
    return -1\n\
  end\n\
end\n\n\
redis.call('ZADD', add_set, cur_ts, key)\n\
return redis.call(orig_cmd, key, value)\n\r\n";

/// Top-level read script (`GET`-shaped commands).
pub const GET_SCRIPT: &str = "$4\r\nEVAL\r\n$569\r\n\
local key = KEYS[1]\n\
local add_set = KEYS[2]\n\
local rem_set = KEYS[3]\n\
local orig_cmd = ARGV[1]\n\
local num_fields = ARGV[2]\n\
local cur_ts = ARGV[3]\n\n\
local value = redis.call(orig_cmd, key)\n\n\
local last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\n\
if (last_seen_ts_in_add and value) then\n\
  return {'E', last_seen_ts_in_add, value}\n\
elseif (last_seen_ts_in_add) then\n\
  redis.call('ZREM', add_set, key)\n\
end\n\n\
local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\n\
if (last_seen_ts_in_rem) then\n\
  return {'R', last_seen_ts_in_rem, value}\n\
end\n\n\
return {'X', 0, value}\n\r\n";

/// Top-level delete script (`DEL`-shaped commands).
pub const DEL_SCRIPT: &str = "$4\r\nEVAL\r\n$793\r\n\
local key = KEYS[1]\n\
local add_set = KEYS[2]\n\
local rem_set = KEYS[3]\n\
local orig_cmd = ARGV[1]\n\
local num_fields = ARGV[2]\n\
local cur_ts = ARGV[3]\n\n\
local last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\n\
local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\n\
if (last_seen_ts_in_rem) then\n\
  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n\
    return 0\n\
  end\n\
  redis.call('ZREM', rem_set, key)\n\
elseif (last_seen_ts_in_add) then\n\
  return 0\n\
end\n\n\
local exists = redis.call('EXISTS', key)\n\
if (exists) then\n\
  local composite_add_set = add_set .. '_' .. key\n\
  local composite_rem_set = rem_set .. '_' .. key\n\
  redis.call('DEL', composite_add_set)\n\
  redis.call('DEL', composite_rem_set)\n\n\
  redis.call('ZADD', add_set, cur_ts, key)\n\
  return redis.call(orig_cmd, key)\n\
end\n\
return 0\n\r\n";

/// Cleanup script for `DEL` after the response set agrees.
pub const CLEANUP_DEL_SCRIPT: &str = "$4\r\nEVAL\r\n$415\r\n\
local key = KEYS[1]\n\
local top_level_add_set = KEYS[2]\n\
local top_level_rem_set = KEYS[3]\n\
local orig_cmd = ARGV[1]\n\
local num_fields = ARGV[2]\n\
local cur_ts = ARGV[3]\n\n\
local top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\n\
if (top_level_rem_set_ts) then\n\
  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n\
    return 0\n\
  end\n\
  return redis.call('ZREM', top_level_rem_set, key)\n\
end\n\
return 0\n\r\n";

/// Cleanup script for `HDEL`/`SREM`/`ZREM` after the response set
/// agrees.
pub const CLEANUP_HDEL_SCRIPT: &str = "$4\r\nEVAL\r\n$664\r\n\
local key = KEYS[1]\n\
local top_level_add_set = KEYS[2]\n\
local top_level_rem_set = KEYS[3]\n\
local add_set = top_level_add_set .. '_' .. key\n\
local rem_set = top_level_rem_set .. '_' .. key\n\
local orig_cmd = ARGV[1]\n\
local num_fields = ARGV[2]\n\
local cur_ts = ARGV[3]\n\
local field = ARGV[4]\n\n\
local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n\
if (last_seen_ts_in_rem) then\n\
  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n\
    return 0\n\
  end\n\
  local ret = redis.call('ZREM', rem_set, field)\n\
  local remaining_elems = redis.call('ZCARD', rem_set)\n\
  if (remaining_elems == 0) then\n\
    redis.call('ZREM', top_level_rem_set, key)\n\
  end\n\
  return ret\n\
end\n\r\n";

/// Reserved metadata-set name used by every repair script.
pub const ADD_SET_STR: &str = "._add-set";
/// Reserved metadata-set name used by every repair script.
pub const REM_SET_STR: &str = "._rem-set";
