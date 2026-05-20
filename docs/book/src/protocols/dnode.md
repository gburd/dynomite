# DNODE wire protocol

DNODE is the framing Dynomite peers use to talk to one another. Every
peer-to-peer message - datastore requests, datastore responses,
gossip frames, and the AES key handshake - travels inside a DNODE
envelope.

This page documents the framing semantically. The Rust implementation
lives in [`dynomite::proto::dnode`](../../crate/dynomite/proto/dnode/index.html).

## Frame layout

A DNODE frame is an ASCII-only header followed by an opaque payload:

```
   $2014$ <msg_id> <type> <flags> <version> <same_dc> *<mlen> <data> *<plen>\r\n
   <payload of <plen> bytes>
```

* The leading three spaces are part of the magic literal as it appears
  on the wire; the parser tolerates them as initial whitespace.
* `$2014$` is a fixed magic literal that anchors header recovery.
* `<msg_id>` is the 64-bit unsigned message id, in decimal.
* `<type>` is the [`DmsgType`] discriminator, in decimal.
* `<flags>` is the bit field. Bit 0 is the encryption flag, bit 1 is
  the compression flag. The high nibble is reserved.
* `<version>` is the protocol version. The current version is 1.
* `<same_dc>` is `1` when the sender and receiver share a datacenter
  and `0` otherwise.
* `*<mlen>` is the byte length of the inline data field, expressed as
  `*<decimal>`.
* `<data>` is the inline data: either the single-byte placeholder
  `d` (data path) or `a` (gossip path), or the RSA-wrapped AES key
  during the crypto handshake.
* `*<plen>` is the byte length of the payload that follows, expressed
  as `*<decimal>`.
* `\r\n` (CRLF) terminates the header.
* The payload follows immediately after the CRLF and contains exactly
  `plen` bytes.

The header fields are always ASCII-decimal even when they encode
binary values (the encryption flag, the type tag). The inline data
field is the only header location that may contain arbitrary bytes;
its length is fixed by the preceding `*<mlen>` so the parser can
copy `mlen` bytes verbatim.

## Message types

The reference engine and the Rust port share the following set of
type discriminators:

| Value | Name              | Meaning                                  |
|-------|-------------------|------------------------------------------|
| 0     | UNKNOWN           | Unset / unknown                          |
| 1     | DEBUG             | Diagnostic frame (unused on the wire)    |
| 2     | PARSE_ERROR       | Parse-error frame (unused on the wire)   |
| 3     | DMSG_REQ          | Datastore request bound for the local DC |
| 4     | DMSG_REQ_FORWARD  | Datastore request forwarded across DCs   |
| 5     | DMSG_RES          | Datastore response                       |
| 6     | CRYPTO_HANDSHAKE  | AES key handshake                        |
| 7     | GOSSIP_SYN        | Gossip SYN                               |
| 8     | GOSSIP_SYN_REPLY  | Gossip SYN reply                         |
| 9     | GOSSIP_ACK        | Gossip ACK                               |
| 10    | GOSSIP_DIGEST_SYN | Gossip digest SYN                        |
| 11    | GOSSIP_DIGEST_ACK | Gossip digest ACK                        |
| 12    | GOSSIP_DIGEST_ACK2| Gossip digest ACK round 2                |
| 13    | GOSSIP_SHUTDOWN   | Gossip shutdown notice                   |

## Crypto handshake

The first frame on a freshly secured peer connection is a
`CRYPTO_HANDSHAKE` envelope with the encryption flag set. The inline
`<data>` field contains the AES-128 session key wrapped with the
peer's RSA public key. The receiver unwraps the key with its private
RSA key and stores it on the connection state; subsequent frames on
the same connection encrypt their payload with that AES key.

## Parser state machine

The Rust parser exposes a public state alphabet
([`DynParseState`]) that mirrors the reference engine's enum. The
states correspond to the header fields in declaration order plus a
trailing `Done` / `PostDone` / `Unknown` triple used by the
recovery path:

```
Start
  -> MagicString    (after $2014$)
  -> MsgId
  -> TypeId
  -> BitField       (flags)
  -> Version
  -> SameDc
  -> DataLen        (after *)
  -> Data           (mlen bytes copied)
  -> SpacesBeforePayloadLen
  -> PayloadLen     (after *)
  -> CrlfBeforeDone
  -> Done           (after \n)
```

`PostDone` is the state the receiver enters after consuming and
decrypting an encrypted handshake frame; the next bytes on the
connection feed the datastore parser instead of the DNODE parser.
`Unknown` is the recovery state used when the parser hits a byte
that is not valid in the current state.

## Encoding

The encoder produces a single contiguous header. The `dmsg_write`
flavour emits the data-path placeholder (`d`) when no AES key
payload is supplied; the `dmsg_write_mbuf` flavour emits the gossip
placeholder (`a`) instead. Both encoders accept an optional
RSA-wrapped AES key as the inline data field; when supplied, the
encoder writes the wrapped bytes verbatim and updates `<mlen>` to
match the wrapped key length.

[`DmsgType`]: ../../crate/dynomite/proto/dnode/enum.DmsgType.html
[`DynParseState`]: ../../crate/dynomite/proto/dnode/enum.DynParseState.html
