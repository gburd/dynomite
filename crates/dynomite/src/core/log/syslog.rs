//! RFC 5424 (modern) and RFC 3164 (BSD) syslog event formatters.
//!
//! Both formatters serialise a single tracing event onto one
//! newline-terminated line. The shared header is
//!
//! * RFC 5424: `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID SD MSG`
//! * RFC 3164: `<PRI>TIMESTAMP HOSTNAME TAG: MSG`
//!
//! `PRI` is a numeric priority computed as `facility * 8 + severity`.
//! Dynomite uses facility `1` (user-level) and the severity ladder
//! `TRACE/DEBUG=7`, `INFO=6`, `WARN=4`, `ERROR=3`, matching the user
//! brief and the conventional mapping in the syslog literature.
//!
//! The user brief originally said "RFC 3124"; that document is "The
//! Congestion Manager" and is unrelated to logging, so we treat the
//! reference as a typo for RFC 3164 and implement the BSD syslog
//! shape here.

use std::fmt;
use std::process;

use time::format_description::FormatItem;
use time::macros::format_description;
use time::{OffsetDateTime, UtcOffset};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::registry::LookupSpan;

use super::host::local_hostname;

/// Stable structured-data ID for RFC 5424 output.
///
/// `32473` is the documented "example enterprise number" reserved by
/// IANA (RFC 5612), which makes the serialised data parseable by any
/// RFC-5424-aware collector while not falsely claiming a private
/// enterprise number.
pub const STRUCTURED_DATA_ID: &str = "origin@32473";

/// APP-NAME field used by RFC 5424 output.
pub const APP_NAME: &str = "dynomited";

/// TAG field used by RFC 3164 output.
pub const TAG: &str = "dynomited";

const RFC5424_TIMESTAMP: &[FormatItem<'_>] = format_description!(
    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6][offset_hour sign:mandatory]:[offset_minute]"
);

const RFC3164_TIMESTAMP_DAY_PADDED: &[FormatItem<'_>] =
    format_description!("[month repr:short] [day padding:space] [hour]:[minute]:[second]");

fn pri_for(level: tracing::Level) -> u8 {
    // Facility 1 (user-level) << 3 | severity.
    let severity: u8 = match level {
        tracing::Level::ERROR => 3,
        tracing::Level::WARN => 4,
        tracing::Level::INFO => 6,
        tracing::Level::DEBUG | tracing::Level::TRACE => 7,
    };
    8 + severity
}

fn now_local() -> OffsetDateTime {
    // `OffsetDateTime::now_local` can fail in containers that do not
    // expose `/etc/localtime`. Falling back to UTC keeps the formatter
    // total without tripping the `unwrap` interlock.
    OffsetDateTime::now_local()
        .unwrap_or_else(|_| OffsetDateTime::now_utc().to_offset(UtcOffset::UTC))
}

/// Event formatter implementing RFC 5424 syslog.
#[derive(Debug, Clone)]
pub struct Rfc5424Formatter {
    hostname: String,
    pid: u32,
}

impl Default for Rfc5424Formatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Rfc5424Formatter {
    /// Build a formatter snapshotting the current hostname and pid.
    ///
    /// Both values are read once and reused for every event so the
    /// hot path stays lock-free.
    #[must_use]
    pub fn new() -> Self {
        Self {
            hostname: local_hostname(),
            pid: process::id(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for Rfc5424Formatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let pri = pri_for(*metadata.level());

        let timestamp = now_local()
            .format(&RFC5424_TIMESTAMP)
            .unwrap_or_else(|_| "-".to_string());

        let file = metadata.file().unwrap_or("-");
        let line = metadata
            .line()
            .map_or_else(|| "-".to_string(), |n| n.to_string());

        write!(
            writer,
            "<{pri}>1 {timestamp} {host} {app} {pid} - [{sd_id} file=\"{file}\" line=\"{line}\" target=\"{target}\" level=\"{level}\"] ",
            pri = pri,
            timestamp = timestamp,
            host = self.hostname,
            app = APP_NAME,
            pid = self.pid,
            sd_id = STRUCTURED_DATA_ID,
            file = sanitize_sd_value(file),
            line = sanitize_sd_value(&line),
            target = sanitize_sd_value(metadata.target()),
            level = metadata.level(),
        )?;

        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// Event formatter implementing RFC 3164 (BSD) syslog.
#[derive(Debug, Clone)]
pub struct Rfc3164Formatter {
    hostname: String,
    pid: u32,
}

impl Default for Rfc3164Formatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Rfc3164Formatter {
    /// Build a formatter snapshotting the current hostname and pid.
    #[must_use]
    pub fn new() -> Self {
        Self {
            hostname: local_hostname(),
            pid: process::id(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for Rfc3164Formatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let pri = pri_for(*metadata.level());

        let timestamp = now_local()
            .format(&RFC3164_TIMESTAMP_DAY_PADDED)
            .unwrap_or_else(|_| "Jan  1 00:00:00".to_string());

        write!(
            writer,
            "<{pri}>{timestamp} {host} {tag}[{pid}]: {target} ",
            pri = pri,
            timestamp = timestamp,
            host = self.hostname,
            tag = TAG,
            pid = self.pid,
            target = metadata.target(),
        )?;

        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// Escape a structured-data PARAM-VALUE per RFC 5424 section 6.3.3.
///
/// PARAM-VALUE forbids the unescaped trio of `"`, `\`, and `]`; each
/// must be preceded by a backslash. We also strip CR/LF so a stray
/// newline in a path or target cannot split the line.
fn sanitize_sd_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' | '"' | ']' => {
                out.push('\\');
                out.push(c);
            }
            '\n' | '\r' => {}
            other => out.push(other),
        }
    }
    out
}

/// Format helper exposed for the unit tests: write the static prefix of
/// an RFC 5424 line for a given level. Tests use it as a sanity probe
/// when they cannot easily install the global subscriber.
#[cfg(test)]
pub(crate) fn rfc5424_prefix(level: tracing::Level) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    let _ = write!(&mut s, "<{}>1 ", pri_for(level));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pri_table_matches_brief() {
        assert_eq!(pri_for(tracing::Level::TRACE), 15);
        assert_eq!(pri_for(tracing::Level::DEBUG), 15);
        assert_eq!(pri_for(tracing::Level::INFO), 14);
        assert_eq!(pri_for(tracing::Level::WARN), 12);
        assert_eq!(pri_for(tracing::Level::ERROR), 11);
    }

    #[test]
    fn sd_value_escapes_required_chars() {
        assert_eq!(sanitize_sd_value(r#"a "b" c\d ]e"#), r#"a \"b\" c\\d \]e"#);
        assert_eq!(sanitize_sd_value("a\nb\rc"), "abc");
    }

    #[test]
    fn rfc5424_prefix_shape() {
        let s = rfc5424_prefix(tracing::Level::INFO);
        assert_eq!(s, "<14>1 ");
    }
}
