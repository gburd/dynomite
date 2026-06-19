//! Property-based tests for the pure data-structure cores in the
//! engine: the SPSC circular buffer ([`dynomite::io::cbuf::CBuf`])
//! and the owning message queue ([`dynomite::msg::MsgQueue`]).
//!
//! Both are bounded / ordered containers whose invariants are
//! algebraic: a `CBuf` never yields more than was written and
//! preserves FIFO order across arbitrary interleavings of pushes
//! and pops; a `MsgQueue` preserves FIFO order regardless of how
//! enqueue / dequeue operations interleave.

use std::collections::VecDeque;

use dynomite::io::cbuf::CBuf;
use dynomite::msg::{Msg, MsgQueue, MsgType};
use hegel::generators as gs;
use hegel::TestCase;

/// One operation in a randomized cbuf workload.
#[derive(Clone, Copy, Debug)]
enum Op {
    Push(u32),
    Pop,
}

fn draw_ops(tc: &TestCase, max_ops: usize) -> Vec<Op> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(max_ops));
    (0..n)
        .map(|_| {
            if tc.draw(gs::booleans()) {
                Op::Push(tc.draw(gs::integers::<u32>()))
            } else {
                Op::Pop
            }
        })
        .collect()
}

#[hegel::test(test_cases = 256)]
fn cbuf_matches_bounded_fifo_reference(tc: TestCase) {
    // A CBuf of capacity C behaves exactly like a VecDeque capped at
    // C elements: pushes past capacity fail, pops return the oldest
    // surviving element, and the order is FIFO throughout.
    let cap = tc.draw(gs::integers::<usize>().min_value(1).max_value(16));
    let ops = draw_ops(&tc, 64);

    let q: CBuf<u32> = CBuf::new(cap);
    let mut model: VecDeque<u32> = VecDeque::new();

    for op in ops {
        match op {
            Op::Push(v) => {
                let res = q.push(v);
                if model.len() < cap {
                    assert!(res.is_ok(), "push into non-full ring must succeed");
                    model.push_back(v);
                } else {
                    // Full ring rejects and hands the item back.
                    assert_eq!(res, Err(v), "full ring must reject and return item");
                }
            }
            Op::Pop => {
                assert_eq!(q.pop(), model.pop_front(), "pop must match FIFO model");
            }
        }
        // Structural invariants hold after every operation.
        assert_eq!(q.len(), model.len());
        assert_eq!(q.is_empty(), model.is_empty());
        assert_eq!(q.is_full(), model.len() == cap);
        assert!(q.len() <= cap, "ring never exceeds capacity");
    }

    // Draining the ring yields exactly the surviving elements in
    // FIFO order and never more than were written.
    while let Some(v) = q.pop() {
        assert_eq!(Some(v), model.pop_front());
    }
    assert!(model.is_empty());
    assert!(q.is_empty());
}

#[hegel::test(test_cases = 256)]
fn cbuf_never_reads_more_than_written(tc: TestCase) {
    // The total number of successful pops can never exceed the total
    // number of successful pushes, for any op interleaving.
    let cap = tc.draw(gs::integers::<usize>().min_value(1).max_value(8));
    let ops = draw_ops(&tc, 48);
    let q: CBuf<u32> = CBuf::new(cap);
    let mut pushed = 0usize;
    let mut popped = 0usize;
    for op in ops {
        match op {
            Op::Push(v) => {
                if q.push(v).is_ok() {
                    pushed += 1;
                }
            }
            Op::Pop => {
                if q.pop().is_some() {
                    popped += 1;
                }
            }
        }
        assert!(popped <= pushed, "reads never exceed writes");
    }
}

#[hegel::test(test_cases = 256)]
fn msg_queue_preserves_fifo_order(tc: TestCase) {
    // Enqueueing ids then dequeueing them yields them back in
    // arrival order; len/is_empty track the live count throughout.
    let ids = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(0).max_value(1_000_000))
            .min_size(0)
            .max_size(64),
    );
    let mut q = MsgQueue::new();
    let mut model: VecDeque<u64> = VecDeque::new();
    for &id in &ids {
        q.push_back(Msg::new(id, MsgType::ReqRedisGet, true));
        model.push_back(id);
        assert_eq!(q.len(), model.len());
        assert!(!q.is_empty());
    }
    while let Some(expected) = model.pop_front() {
        let got = q.pop_front().expect("queue not empty");
        assert_eq!(got.id(), expected, "dequeue order must be FIFO");
    }
    assert!(q.is_empty());
    assert!(q.pop_front().is_none());
}

#[hegel::test(test_cases = 256)]
fn msg_queue_interleaved_ops_match_reference(tc: TestCase) {
    // Arbitrary interleavings of push_back and pop_front stay in
    // lockstep with a VecDeque reference.
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(64));
    let mut q = MsgQueue::new();
    let mut model: VecDeque<u64> = VecDeque::new();
    let mut next_id = 1u64;
    for _ in 0..n {
        if tc.draw(gs::booleans()) {
            let id = next_id;
            next_id += 1;
            q.push_back(Msg::new(id, MsgType::ReqMcGet, true));
            model.push_back(id);
        } else {
            let got = q.pop_front().map(|m| m.id());
            assert_eq!(got, model.pop_front());
        }
        assert_eq!(q.len(), model.len());
        assert_eq!(q.is_empty(), model.is_empty());
        // front() must agree with the model's head.
        assert_eq!(q.front().map(Msg::id), model.front().copied());
    }
}
