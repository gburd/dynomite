//! Framing for the Riak PBC transport.
//!
//! See the module-level documentation on [`super`] for the wire-shape
//! description. The framer owns only I/O; payload encoding/decoding
//! happens in [`super::codec`].

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::RiakError;

/// Maximum frame length the framer will accept, in bytes.
///
/// Riak's reference framer caps individual messages at 16 MiB. We
/// adopt the same default. The cap excludes the 4-byte length prefix.
///
/// The cap is deliberately liberal: large MapReduce inputs and 2i
/// query results approach this in practice. The follow-up slice will
/// expose the cap as a [`crate::server::ServeConfig`] knob.
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Decoded PBC frame: a message code byte plus its protobuf body.
///
/// # Examples
///
/// ```
/// use dyniak::proto::pb::{Frame, MessageCode};
/// let f = Frame::new(MessageCode::PingReq.as_u8(), Vec::new());
/// assert_eq!(f.code, 1);
/// assert!(f.body.is_empty());
/// assert_eq!(f.wire_len(), 5); // 4 length + 1 code + 0 body
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    /// Riak PBC message code (raw byte).
    pub code: u8,
    /// Protobuf body bytes; may be empty for body-less messages
    /// such as `RpbPingReq`.
    pub body: Vec<u8>,
}

impl Frame {
    /// Build a new frame.
    #[must_use]
    pub fn new(code: u8, body: Vec<u8>) -> Self {
        Self { code, body }
    }

    /// Total wire length including the 4-byte length prefix.
    #[must_use]
    pub fn wire_len(&self) -> usize {
        4 + 1 + self.body.len()
    }
}

/// Read one frame off `r`, applying the [`MAX_FRAME_LEN`] cap.
///
/// Errors:
///
/// * [`RiakError::Io`] if the underlying read fails.
/// * [`RiakError::UnexpectedEof`] if the peer closes mid-frame.
/// * [`RiakError::EmptyFrame`] if the peer announces a zero-length
///   frame, which is not legal in the PBC framing.
/// * [`RiakError::FrameTooLarge`] if the announced length exceeds
///   [`MAX_FRAME_LEN`].
pub async fn read_frame<R>(r: &mut R) -> Result<Frame, RiakError>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    read_exact_or_classify(r, &mut len_buf, 4).await?;
    let announced = u32::from_be_bytes(len_buf);

    if announced == 0 {
        return Err(RiakError::EmptyFrame);
    }
    if announced > MAX_FRAME_LEN {
        return Err(RiakError::FrameTooLarge {
            announced,
            max: MAX_FRAME_LEN,
        });
    }

    let mut code_buf = [0u8; 1];
    read_exact_or_classify(r, &mut code_buf, 1).await?;
    let code = code_buf[0];

    // `announced` covers the message code plus body. We have already
    // consumed one byte for the code, so the body length is
    // `announced - 1`.
    //
    // SAFETY-of-arithmetic: `announced >= 1` was just checked.
    let body_len = (announced - 1) as usize;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        read_exact_or_classify(r, &mut body, body_len).await?;
    }

    Ok(Frame { code, body })
}

/// Write `frame` to `w` and flush.
pub async fn write_frame<W>(w: &mut W, frame: &Frame) -> Result<(), RiakError>
where
    W: AsyncWrite + Unpin,
{
    // Length prefix covers the message-code byte plus the body.
    let announced = u32::try_from(1 + frame.body.len()).map_err(|_| RiakError::FrameTooLarge {
        announced: u32::MAX,
        max: MAX_FRAME_LEN,
    })?;
    if announced > MAX_FRAME_LEN {
        return Err(RiakError::FrameTooLarge {
            announced,
            max: MAX_FRAME_LEN,
        });
    }

    w.write_all(&announced.to_be_bytes()).await?;
    w.write_all(&[frame.code]).await?;
    if !frame.body.is_empty() {
        w.write_all(&frame.body).await?;
    }
    w.flush().await?;
    Ok(())
}

async fn read_exact_or_classify<R>(
    r: &mut R,
    buf: &mut [u8],
    expected: usize,
) -> Result<(), RiakError>
where
    R: AsyncRead + Unpin,
{
    match r.read_exact(buf).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // `read_exact` does not surface partial-read counts on
            // failure. From the framer's perspective the relevant
            // signal is "the peer closed after promising N more
            // bytes"; we report that and let the caller log it.
            Err(RiakError::UnexpectedEof { read: 0, expected })
        }
        Err(e) => Err(RiakError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn ping_frame_round_trips() {
        let (mut a, mut b) = duplex(1024);
        let f = Frame::new(1, Vec::new());
        write_frame(&mut a, &f).await.unwrap();
        let back = read_frame(&mut b).await.unwrap();
        assert_eq!(back, f);
    }

    #[tokio::test]
    async fn body_frame_round_trips() {
        let (mut a, mut b) = duplex(4096);
        let f = Frame::new(11, b"hello".to_vec());
        write_frame(&mut a, &f).await.unwrap();
        let back = read_frame(&mut b).await.unwrap();
        assert_eq!(back, f);
        assert_eq!(back.wire_len(), 4 + 1 + 5);
    }

    #[tokio::test]
    async fn rejects_zero_length_announcement() {
        let (mut a, mut b) = duplex(64);
        a.write_all(&0u32.to_be_bytes()).await.unwrap();
        a.flush().await.unwrap();
        drop(a);
        let err = read_frame(&mut b).await.expect_err("zero length");
        assert!(matches!(err, RiakError::EmptyFrame));
    }

    #[tokio::test]
    async fn rejects_oversized_announcement() {
        let (mut a, mut b) = duplex(64);
        let bad = MAX_FRAME_LEN + 1;
        a.write_all(&bad.to_be_bytes()).await.unwrap();
        a.flush().await.unwrap();
        drop(a);
        let err = read_frame(&mut b).await.expect_err("too big");
        assert!(matches!(
            err,
            RiakError::FrameTooLarge { announced, max }
                if announced == MAX_FRAME_LEN + 1 && max == MAX_FRAME_LEN
        ));
    }

    #[tokio::test]
    async fn unexpected_eof_reports_error() {
        let (a, mut b) = duplex(64);
        drop(a);
        let err = read_frame(&mut b).await.expect_err("eof");
        assert!(matches!(err, RiakError::UnexpectedEof { .. }));
    }

    #[tokio::test]
    async fn truncated_body_reports_unexpected_eof() {
        let (mut a, mut b) = duplex(64);
        // Announce a 5-byte body but send only 2 (plus the code).
        a.write_all(&6u32.to_be_bytes()).await.unwrap();
        a.write_all(&[11]).await.unwrap();
        a.write_all(b"he").await.unwrap();
        a.flush().await.unwrap();
        drop(a);
        let err = read_frame(&mut b).await.expect_err("truncated");
        assert!(matches!(err, RiakError::UnexpectedEof { .. }));
    }
}
