//! Integration tests for `dyntext::postings`.

use dyntext::postings::Postings;

#[test]
fn postings_insert_and_lookup() {
    let mut p = Postings::new();
    p.insert(1, 100);
    p.insert(1, 200);
    p.insert(2, 300);
    let bm = p.lookup(1).expect("trigram 1 present");
    assert!(bm.contains(100));
    assert!(bm.contains(200));
    assert!(!bm.contains(300));
}

#[test]
fn postings_intersection_of_two_trigrams_yields_intersection() {
    let mut p = Postings::new();
    for d in [10_u32, 20, 30] {
        p.insert(1, d);
    }
    for d in [20_u32, 30, 40] {
        p.insert(2, d);
    }
    let r = p.intersect(&[1, 2]);
    assert!(r.contains(20));
    assert!(r.contains(30));
    assert!(!r.contains(10));
    assert!(!r.contains(40));
}

#[test]
fn postings_intersection_of_disjoint_trigrams_is_empty() {
    let mut p = Postings::new();
    p.insert(1, 1);
    p.insert(2, 2);
    assert!(p.intersect(&[1, 2]).is_empty());
}

#[test]
fn postings_remove_clears_doc_id_from_one_trigram() {
    let mut p = Postings::new();
    p.insert(1, 10);
    p.insert(1, 20);
    p.remove(1, 10);
    let bm = p.lookup(1).expect("trigram still has remaining docs");
    assert!(!bm.contains(10));
    assert!(bm.contains(20));
}

#[test]
fn postings_dropping_a_trigram_when_no_docs_left_garbage_collects() {
    let mut p = Postings::new();
    p.insert(7, 42);
    assert_eq!(p.len(), 1);
    p.remove(7, 42);
    assert_eq!(p.len(), 0);
    assert!(p.lookup(7).is_none());
}
