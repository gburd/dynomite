//! Lua script templates used by the read-repair engine.
//!
//! Each constant is a complete Redis wire-format `EVAL` invocation
//! shell preceded by the `$<n>\r\n` length prefix. The bytes are
//! treated as opaque payloads by the parser and as fixed templates
//! by the rewrite path; tests in this module pin the declared
//! length prefix against the actual body length so any drift is
//! caught at build time.

/// Top-level write script (`SET`-shaped commands).
pub const SET_SCRIPT: &str = "$4\r\nEVAL\r\n$640\r\nlocal key = KEYS[1]\nlocal add_set = KEYS[2]\nlocal rem_set = KEYS[3]\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\nlocal value = ARGV[4]\n\nlocal last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\nlocal last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\n\nif (last_seen_ts_in_rem) then\n  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n    return -1\n  end\n  redis.call('ZREM', rem_set, key)\nelseif (last_seen_ts_in_add) then\n  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n    return -1\n  end\nend\n\nredis.call('ZADD', add_set, cur_ts, key)\nreturn redis.call(orig_cmd, key, value)\n\r\n";

/// Top-level read script (`GET`-shaped commands).
pub const GET_SCRIPT: &str = "$4\r\nEVAL\r\n$569\r\nlocal key = KEYS[1]\nlocal add_set = KEYS[2]\nlocal rem_set = KEYS[3]\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\n\nlocal value = redis.call(orig_cmd, key)\n\nlocal last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\nif (last_seen_ts_in_add and value) then\n  return {'E', last_seen_ts_in_add, value}\nelseif (last_seen_ts_in_add) then\n  redis.call('ZREM', add_set, key)\nend\n\nlocal last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\nif (last_seen_ts_in_rem) then\n  return {'R', last_seen_ts_in_rem, value}\nend\n\nreturn {'X', 0, value}\n\r\n";

/// Top-level delete script (`DEL`-shaped commands).
pub const DEL_SCRIPT: &str = "$4\r\nEVAL\r\n$793\r\nlocal key = KEYS[1]\nlocal add_set = KEYS[2]\nlocal rem_set = KEYS[3]\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\n\nlocal last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\nlocal last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\nif (last_seen_ts_in_rem) then\n  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n    return 0\n  end\n  redis.call('ZREM', rem_set, key)\nelseif (last_seen_ts_in_add) then\n  return 0\nend\n\nlocal exists = redis.call('EXISTS', key)\nif (exists) then\n  local composite_add_set = add_set .. '_' .. key\n  local composite_rem_set = rem_set .. '_' .. key\n  redis.call('DEL', composite_add_set)\n  redis.call('DEL', composite_rem_set)\n\n  redis.call('ZADD', add_set, cur_ts, key)\n  return redis.call(orig_cmd, key)\nend\nreturn 0\n\r\n";

/// Hash-field write script (`HSET`-shaped commands).
pub const HSET_SCRIPT: &str = "$4\r\nEVAL\r\n$1613\r\nlocal key = KEYS[1]\nlocal top_level_add_set = KEYS[2]\nlocal top_level_rem_set = KEYS[3]\nlocal add_set = top_level_add_set .. '_' .. key\nlocal rem_set = top_level_rem_set .. '_' .. key\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\n\nlocal start_loop = 4\nlocal end_loop = (num_fields * 2) + 3\n\nlocal top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\nif (top_level_rem_set_ts) then\n  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n    return 0\n  end\n  redis.call('ZREM', top_level_rem_set, key)\nend\n\nlocal top_level_add_set_ts = redis.call('ZSCORE', top_level_add_set, key)\nif (top_level_add_set_ts) then\n  if (tonumber(cur_ts) > tonumber(top_level_add_set_ts)) then\n    redis.call('ZADD', top_level_add_set, cur_ts, key)\n  end\nelse\n  redis.call('ZADD', top_level_add_set, cur_ts, key)\nend\n\nlocal skiploop\nlocal ret\nfor i=start_loop,end_loop,2\ndo\n  skiploop = false\n  local field = ARGV[i]\n  local value = ARGV[i+1]\n  local last_seen_ts_in_add = redis.call('ZSCORE', add_set, field)\n  local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n  if (last_seen_ts_in_rem) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n      skiploop = true\n    end\n    redis.call('ZREM', rem_set, field)\n  elseif (last_seen_ts_in_add) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n      skiploop = true\n    end\n  end\n\n  if (skiploop == false) then\n    redis.call('ZADD', add_set, cur_ts, field)\n    ret = redis.call(orig_cmd, key, field, value)\n  end\nend\n\nif tonumber(num_fields) > 1 then\n  return \"OK\"\nelse\n  return ret\nend\n\r\n";

/// Hash-field delete script (`HDEL`-shaped commands).
pub const HDEL_SCRIPT: &str = "$4\r\nEVAL\r\n$1122\r\nlocal key = KEYS[1]\nlocal top_level_add_set = KEYS[2]\nlocal top_level_rem_set = KEYS[3]\nlocal add_set = top_level_add_set .. '_' .. key\nlocal rem_set = top_level_rem_set .. '_' .. key\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\n\nlocal start_loop = 4\nlocal end_loop = num_fields + 3\n\nlocal skiploop\nlocal ret = 0\nfor i=start_loop,end_loop,1\ndo\n  skiploop = false\n  local field = ARGV[i]\n  local last_seen_ts_in_add = redis.call('ZSCORE', add_set, field)\n  local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n  if (last_seen_ts_in_rem) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n      skiploop = true\n    else\n      redis.call('ZREM', rem_set, field)\n    end\n  elseif (last_seen_ts_in_add) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n      skiploop = true\n    end\n  end\n\n  if (skiploop == false) then\n    redis.call('ZADD', add_set, cur_ts, field)\n    ret = ret + redis.call(orig_cmd, key, field)\n  end\nend\n\nlocal card = redis.call('ZCARD', rem_set)\nif (card == 0) then\n  redis.call('ZREM', top_level_rem_set, key)\nend\n\nreturn ret\n\r\n";

/// Hash-field read script (`HGET`-shaped commands).
pub const HGET_SCRIPT: &str = "$4\r\nEVAL\r\n$1055\r\nlocal key = KEYS[1]\nlocal top_level_add_set = KEYS[2]\nlocal top_level_rem_set = KEYS[3]\nlocal add_set = top_level_add_set .. '_' .. key\nlocal rem_set = top_level_rem_set .. '_' .. key\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\nlocal field = ARGV[4]\n\nlocal status_field = 'E'\nlocal ts = 0\n\nlocal tl_removed_ts = redis.call('ZSCORE', top_level_rem_set, key)\nif (tl_removed_ts) then\n  status_field = 'R'\n  ts = tl_removed_ts\nelse\n  local removed_ts = redis.call('ZSCORE', rem_set, field)\n  if (removed_ts) then\n    ts = removed_ts\n    status_field = 'R'\n  end\nend\n\nif (status_field ~= 'R') then\n  local tl_exists = redis.call('ZSCORE', top_level_add_set, key)\n  if (tl_exists) then\n    local exists_ts = redis.call('ZSCORE', add_set, field)\n    if (not exists_ts) then\n      status_field = 'X'\n    else\n      ts = exists_ts\n    end\n  else\n    status_field = 'X'\n  end\nend\n\nlocal value = redis.call(orig_cmd, key, field)\nif (status_field == 'E' and not value) then\n  return {'X', 0, value}\nend\nreturn {status_field, ts, value}\n\r\n";

/// Sorted-set write script (`ZADD`-shaped commands).
pub const ZADD_SCRIPT: &str = "$4\r\nEVAL\r\n$2022\r\nlocal key = KEYS[1]\nlocal top_level_add_set = KEYS[2]\nlocal top_level_rem_set = KEYS[3]\nlocal add_set = top_level_add_set .. '_' .. key\nlocal rem_set = top_level_rem_set .. '_' .. key\nlocal orig_cmd = ARGV[1]\nlocal num_opts = ARGV[2]\nlocal num_fields = ARGV[3]\nlocal cur_ts = ARGV[4]\nlocal start_loop = 5 + num_opts\nlocal end_loop = (num_fields * 2) + 4 + num_opts\nlocal top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\nif (top_level_rem_set_ts) then\n  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n    return 0\n  end\n  redis.call('ZREM', top_level_rem_set, key)\nend\nlocal top_level_add_set_ts = redis.call('ZSCORE', top_level_add_set, key)\nif (top_level_add_set_ts) then\n  if (tonumber(cur_ts) > tonumber(top_level_add_set_ts)) then\n    redis.call('ZADD', top_level_add_set, cur_ts, key)\n  end\nelse\n  redis.call('ZADD', top_level_add_set, cur_ts, key)\nend\nlocal skiploop\nlocal ret\nfor i=start_loop,end_loop,2\ndo\n  skiploop = false\n  local field = ARGV[i]\n  local value = ARGV[i+1]\n  local last_seen_ts_in_add = redis.call('ZSCORE', add_set, field)\n  local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n  if (last_seen_ts_in_rem) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n      skiploop = true\n    end\n    redis.call('ZREM', rem_set, field)\n  elseif (last_seen_ts_in_add) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n      skiploop = true\n    end\n  end\n  if (skiploop == false) then\n    if (num_opts == '0') then\n      ret = redis.call(orig_cmd, key, value, field)\n    elseif (num_opts == '1') then\n      ret = redis.call(orig_cmd, key, ARGV[5], value, field)\n    elseif (num_opts == '2') then\n      ret = redis.call(orig_cmd, key, ARGV[5], ARGV[6], value, field)\n    elseif (num_opts == '3') then\n      ret = redis.call(orig_cmd, key, ARGV[5], ARGV[6], ARGV[7], value, field)\n    else\n      ret = false\n    end\n    if (type(ret) ~= 'boolean') then\n      redis.call('ZADD', add_set, cur_ts, field)\n    end\n  end\nend\nreturn ret\n\r\n";

/// Set write script (`SADD`-shaped commands).
pub const SADD_SCRIPT: &str = "$4\r\nEVAL\r\n$1526\r\nlocal key = KEYS[1]\nlocal top_level_add_set = KEYS[2]\nlocal top_level_rem_set = KEYS[3]\nlocal add_set = top_level_add_set .. '_' .. key\nlocal rem_set = top_level_rem_set .. '_' .. key\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\n\nlocal start_loop = 4\nlocal end_loop = num_fields + 3\n\nlocal top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\nif (top_level_rem_set_ts) then\n  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n    return 0\n  end\n  redis.call('ZREM', top_level_rem_set, key)\nend\n\nlocal top_level_add_set_ts = redis.call('ZSCORE', top_level_add_set, key)\nif (top_level_add_set_ts) then\n  if (tonumber(cur_ts) > tonumber(top_level_add_set_ts)) then\n    redis.call('ZADD', top_level_add_set, cur_ts, key)\n  end\nelse\n  redis.call('ZADD', top_level_add_set, cur_ts, key)\nend\n\nlocal skiploop\nlocal ret = 0\nfor i=start_loop,end_loop,1\ndo\n  skiploop = false\n  local field = ARGV[i]\n  local last_seen_ts_in_add = redis.call('ZSCORE', add_set, field)\n  local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n  if (last_seen_ts_in_rem) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n      skiploop = true\n    end\n    redis.call('ZREM', rem_set, field)\n  elseif (last_seen_ts_in_add) then\n    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n      skiploop = true\n    end\n  end\n\n  if (skiploop == false) then\n    redis.call('ZADD', add_set, cur_ts, field)\n    ret = ret + redis.call(orig_cmd, key, field)\n  end\nend\n\nreturn ret\n\r\n";

/// Companion cleanup script for `DEL`.
pub const CLEANUP_DEL_SCRIPT: &str = "$4\r\nEVAL\r\n$415\r\nlocal key = KEYS[1]\nlocal top_level_add_set = KEYS[2]\nlocal top_level_rem_set = KEYS[3]\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\n\nlocal top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\nif (top_level_rem_set_ts) then\n  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n    return 0\n  end\n  return redis.call('ZREM', top_level_rem_set, key)\nend\nreturn 0\n\r\n";

/// Companion cleanup script for `HDEL`.
pub const CLEANUP_HDEL_SCRIPT: &str = "$4\r\nEVAL\r\n$664\r\nlocal key = KEYS[1]\nlocal top_level_add_set = KEYS[2]\nlocal top_level_rem_set = KEYS[3]\nlocal add_set = top_level_add_set .. '_' .. key\nlocal rem_set = top_level_rem_set .. '_' .. key\nlocal orig_cmd = ARGV[1]\nlocal num_fields = ARGV[2]\nlocal cur_ts = ARGV[3]\nlocal field = ARGV[4]\n\nlocal last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\nif (last_seen_ts_in_rem) then\n  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n    return 0\n  end\n  local ret = redis.call('ZREM', rem_set, field)\n  local remaining_elems = redis.call('ZCARD', rem_set)\n  if (remaining_elems == 0) then\n    redis.call('ZREM', top_level_rem_set, key)\n  end\n  return ret\nend\n\r\n";

/// Reserved metadata-set name used by every repair script.
pub const ADD_SET_STR: &str = "._add-set";
/// Reserved metadata-set name used by every repair script.
pub const REM_SET_STR: &str = "._rem-set";

#[cfg(test)]
mod tests {
    use super::*;

    /// Each script begins with `$4\r\nEVAL\r\n$<n>\r\n` and ends
    /// with `\r\n`. The `<n>` declared in the prefix must equal
    /// the body length between the trailing `\r\n` of the
    /// prefix and the final `\r\n`. This pins the on-the-wire
    /// shape and catches any drift between the constant's
    /// declared length and its actual body.
    fn declared_and_actual_lengths(script: &str) -> (usize, usize) {
        let prefix = "$4\r\nEVAL\r\n$";
        assert!(script.starts_with(prefix), "missing EVAL prefix");
        let after_prefix = &script[prefix.len()..];
        let (digits, rest) = after_prefix
            .split_once("\r\n")
            .expect("no CRLF after declared length");
        let declared: usize = digits.parse().expect("non-numeric length");
        let body = rest.strip_suffix("\r\n").expect("missing trailing CRLF");
        (declared, body.len())
    }

    #[test]
    fn set_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(SET_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn get_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(GET_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn del_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(DEL_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn hset_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(HSET_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn hdel_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(HDEL_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn hget_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(HGET_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn zadd_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(ZADD_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn sadd_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(SADD_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn cleanup_del_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(CLEANUP_DEL_SCRIPT);
        assert_eq!(d, a);
    }

    #[test]
    fn cleanup_hdel_script_length_matches_prefix() {
        let (d, a) = declared_and_actual_lengths(CLEANUP_HDEL_SCRIPT);
        assert_eq!(d, a);
    }
}
