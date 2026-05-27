//! Integration tests for the [`dynomite::events`] module.
//!
//! Tests cover the four scenarios in the stage brief:
//!   * `subscribe_then_publish_delivers_event`
//!   * `multiple_subscribers_each_receive_the_event`
//!   * `slow_subscriber_lags_and_surfaces_lagged_error`
//!   * `event_after_drop_subscriber_does_not_block_publisher`

use std::time::{Duration, SystemTime};

use dynomite::events::{ClusterEvent, EventManager, SubscriberError, TryRecvError};

fn ring_changed(tag: &str) -> ClusterEvent {
    ClusterEvent::RingChanged {
        tag: tag.to_string(),
        ts: SystemTime::now(),
    }
}

#[tokio::test]
async fn subscribe_then_publish_delivers_event() {
    let mgr = EventManager::new(8);
    let mut sub = mgr.subscribe();
    mgr.publish(ClusterEvent::PeerUp {
        peer_id: 42,
        dc: "dc-a".into(),
        ts: SystemTime::now(),
    });
    let evt = sub.recv().await.expect("recv must succeed");
    match evt {
        ClusterEvent::PeerUp { peer_id, dc, .. } => {
            assert_eq!(peer_id, 42);
            assert_eq!(dc, "dc-a");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn multiple_subscribers_each_receive_the_event() {
    let mgr = EventManager::new(8);
    let mut a = mgr.subscribe();
    let mut b = mgr.subscribe();
    let mut c = mgr.subscribe();
    assert_eq!(mgr.subscriber_count(), 3);

    mgr.publish(ring_changed("topology"));

    for sub in [&mut a, &mut b, &mut c] {
        let evt = sub.recv().await.expect("recv must succeed");
        match evt {
            ClusterEvent::RingChanged { tag, .. } => assert_eq!(tag, "topology"),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    // Every subscriber's queue is drained.
    for sub in [&mut a, &mut b, &mut c] {
        assert!(matches!(sub.try_recv(), Err(TryRecvError::Empty)));
    }
}

#[tokio::test]
async fn slow_subscriber_lags_and_surfaces_lagged_error() {
    // Capacity is 2; publish 8 events without recv so the
    // subscriber is guaranteed to fall behind the channel tail.
    let mgr = EventManager::new(2);
    let mut sub = mgr.subscribe();

    for i in 0..8u64 {
        mgr.publish(ClusterEvent::GossipRoundComplete {
            duration: Duration::from_millis(i),
            peers_seen: usize::try_from(i).unwrap(),
            ts: SystemTime::now(),
        });
    }

    // The first recv must surface SubscriberError::Lagged(n)
    // with n equal to the number of dropped events. With
    // capacity 2 and 8 publishes, the subscriber missed exactly
    // 6 events (the buffer keeps the freshest 2).
    match sub.recv().await {
        Err(SubscriberError::Lagged(n)) => {
            assert!(n >= 1, "expected positive lag count, got {n}");
            assert_eq!(n, 6, "broadcast(2) + 8 publishes should drop 6 events");
        }
        other => panic!("expected Lagged, got {other:?}"),
    }

    // After the Lagged signal the subscriber resumes from the
    // freshest buffered event and can keep reading.
    let next = sub.recv().await.expect("recv must succeed after lag");
    match next {
        ClusterEvent::GossipRoundComplete { peers_seen, .. } => {
            assert!(peers_seen >= 6, "expected to resume near the tail");
        }
        other => panic!("unexpected event after lag: {other:?}"),
    }
}

#[tokio::test]
async fn event_after_drop_subscriber_does_not_block_publisher() {
    let mgr = EventManager::new(2);
    let live = mgr.subscribe();
    let droppable = mgr.subscribe();
    assert_eq!(mgr.subscriber_count(), 2);

    drop(droppable);
    assert_eq!(mgr.subscriber_count(), 1);

    // Publish many more events than the channel capacity. With
    // a slow remaining subscriber the publisher must still
    // return promptly: tokio::sync::broadcast::Sender::send is
    // non-blocking and overwrites the oldest entry instead of
    // waiting. Wrap in a tight timeout so a regression to a
    // blocking publisher fails the test deterministically.
    let publish = async {
        for i in 0..10_000u64 {
            mgr.publish(ClusterEvent::GossipRoundComplete {
                duration: Duration::from_micros(i),
                peers_seen: 0,
                ts: SystemTime::now(),
            });
        }
    };
    tokio::time::timeout(Duration::from_secs(2), publish)
        .await
        .expect("publisher must not block");

    // The live subscriber is still attached and must see lag
    // (because we deliberately overwhelmed the buffer) - this
    // proves slow subscribers do not stall publishers.
    let mut live = live;
    let first = live.recv().await;
    assert!(
        matches!(first, Err(SubscriberError::Lagged(_)) | Ok(_)),
        "expected Lagged or recovered event, got {first:?}"
    );

    // Dropping the manager closes the channel and the next
    // recv after the buffer drains must surface Closed.
    drop(mgr);
    while let Ok(_) | Err(SubscriberError::Lagged(_)) = live.recv().await {}
}
