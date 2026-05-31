//! End-to-end demo: vector indexing + search and trigram text
//! search via the dynomite + dyntext libraries.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p dynomite --example demo_vector_text
//! ```
//!
//! This is the LIBRARY demo (the FT.* commands go through the
//! in-process registry, not over the wire). The wire-protocol
//! integration is Phase D; the library API exercised here is
//! exactly what dynomited's dispatcher will use once the
//! parser-side wiring lands.

use dynomite::proto::redis::ft::{self, FtOutcome, InfoValue};
use dynomite::vector::registry::VectorRegistry;
use dyntext::index::TextIndex;
/// Convert a slice of f32 to its little-endian byte
/// representation (the wire format Redis Stack clients send
/// for VECTOR fields).
fn f32_to_le_bytes(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

fn print_section(title: &str) {
    println!();
    println!("============================================================");
    println!(" {title}");
    println!("============================================================");
}

fn print_step(text: &str) {
    println!();
    println!(">> {text}");
}

fn print_resp(label: &str, bytes: &[u8]) {
    let rendered = String::from_utf8_lossy(bytes);
    let oneline: String = rendered.replace('\r', "\\r").replace('\n', "\\n");
    let trimmed = if oneline.len() > 200 {
        format!("{}... ({} bytes)", &oneline[..200], oneline.len())
    } else {
        oneline
    };
    println!("   {label:>16}: {trimmed}");
}

fn as_slices(v: &[Vec<u8>]) -> Vec<&[u8]> {
    v.iter().map(Vec::as_slice).collect()
}

fn vector_demo_create_and_insert(registry: &VectorRegistry) {
    // 1. FT.CREATE: a 4-dim cosine-similarity HNSW index over
    //    keys with the prefix "docs:".
    print_step("FT.CREATE myidx ON HASH PREFIX 1 docs: SCHEMA \\");
    println!("              title TEXT \\");
    println!("              vec VECTOR HNSW 6 TYPE FLOAT32 DIM 4 \\");
    println!("              DISTANCE_METRIC COSINE");
    let create_args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"myidx".to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        b"docs:".to_vec(),
        b"SCHEMA".to_vec(),
        b"title".to_vec(),
        b"TEXT".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"4".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"COSINE".to_vec(),
    ];
    let resp = ft::dispatch(registry, &as_slices(&create_args));
    print_resp("response", &resp);

    // 2. HSET 5 documents. Each has a title and a 4-dim vector.
    print_step("Insert 5 documents (HSET docs:N title \"...\" vec <bytes>)");
    let docs: &[(u32, &str, [f32; 4])] = &[
        (1, "the quick brown fox", [0.10, 0.20, 0.30, 0.40]),
        (2, "jumps over the lazy dog", [0.15, 0.25, 0.35, 0.45]),
        (3, "the rain in spain", [-0.50, 0.60, -0.70, 0.80]),
        (4, "stays mainly on the plain", [-0.45, 0.55, -0.65, 0.75]),
        (
            5,
            "all your base are belong to us",
            [0.90, -0.10, 0.05, -0.95],
        ),
    ];
    for &(id, title, vec) in docs {
        let key = format!("docs:{id}");
        let vec_bytes = f32_to_le_bytes(&vec);
        let hset_args: Vec<Vec<u8>> = vec![
            key.as_bytes().to_vec(),
            b"title".to_vec(),
            title.as_bytes().to_vec(),
            b"vec".to_vec(),
            vec_bytes,
        ];
        match ft::maybe_index_hset(registry, &as_slices(&hset_args)) {
            Ok(Some(idx)) => {
                println!("   inserted docs:{id} title={title:?} -> {idx}");
            }
            Ok(None) => {
                println!("   docs:{id}: no matching index (would store as plain HSET)");
            }
            Err(e) => {
                println!("   docs:{id}: error inserting: {e:?}");
            }
        }
    }
}

fn vector_demo() {
    print_section("Vector index + KNN search (dynomite::vector + ft::*)");

    let registry = VectorRegistry::new();

    vector_demo_create_and_insert(&registry);

    // 3. FT.INFO: introspect the index.
    print_step("FT.INFO myidx");
    let info_args: Vec<Vec<u8>> = vec![b"FT.INFO".to_vec(), b"myidx".to_vec()];
    let cmd = ft::parse_command(&as_slices(&info_args)).expect("parse FT.INFO");
    let outcome = ft::execute(&registry, cmd).expect("execute FT.INFO");
    if let FtOutcome::Info(pairs) = outcome {
        for (k, v) in &pairs {
            let value = match v {
                InfoValue::String(s) => s.clone(),
                InfoValue::Integer(n) => n.to_string(),
                InfoValue::Array(items) => format!("{items:?}"),
            };
            println!("   {k:<20}= {value}");
        }
    } else {
        println!("   unexpected outcome: {outcome:?}");
    }

    // 4. FT.SEARCH: find the 3 nearest neighbours to a query vector.
    print_step("FT.SEARCH myidx \"*=>[KNN 3 @vec $blob]\" \\\n    PARAMS 2 blob <bytes>");
    let query: [f32; 4] = [0.12, 0.22, 0.32, 0.42];
    let blob = f32_to_le_bytes(&query);
    let search_args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"myidx".to_vec(),
        b"*=>[KNN 3 @vec $blob]".to_vec(),
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        b"blob".to_vec(),
        blob,
    ];
    let cmd = ft::parse_command(&as_slices(&search_args)).expect("parse FT.SEARCH");
    let outcome = ft::execute(&registry, cmd).expect("execute FT.SEARCH");
    if let FtOutcome::Search { total, hits } = outcome {
        println!("   query vector: {query:?}");
        println!("   total candidates returned: {total}");
        for (rank, hit) in hits.iter().enumerate() {
            let title = hit
                .fields
                .iter()
                .find(|(k, _)| k == "title")
                .map_or("<unknown>".to_string(), |(_, v)| {
                    String::from_utf8_lossy(v).into_owned()
                });
            let key = String::from_utf8_lossy(&hit.doc_id);
            println!(
                "   rank {rank}: id={key:?} score={score:.6} title={title}",
                key = key,
                score = hit.score,
                title = title,
            );
        }
    }

    // 5. FT.LIST.
    print_step("FT.LIST");
    let list_args: Vec<Vec<u8>> = vec![b"FT.LIST".to_vec()];
    let resp = ft::dispatch(&registry, &as_slices(&list_args));
    print_resp("response", &resp);

    // 6. FT.DROPINDEX (without DD; keeps underlying keys).
    print_step("FT.DROPINDEX myidx");
    let drop_args: Vec<Vec<u8>> = vec![b"FT.DROPINDEX".to_vec(), b"myidx".to_vec()];
    let resp = ft::dispatch(&registry, &as_slices(&drop_args));
    print_resp("response", &resp);
}

fn text_demo_approx_regex(idx: &TextIndex, corpus: &[&str], doc_ids: &[u32]) {
    print_section("Approximate-regex search via dyntext::search_regex_approx (Phase 3, TRE FFI)");

    let approx_queries: &[(&str, u16, &str)] = &[
        ("errno: \\w+ refused", 0, "K=0 exact: must literally match"),
        (
            "errno: \\w+ refsed",
            1,
            "K=1 one transposition tolerated ('refsed' vs 'refused')",
        ),
        (
            "errnno: \\w+ refused",
            1,
            "K=1 one extra char tolerated ('errnno' vs 'errno')",
        ),
        (
            "errno: \\w+ rfusd",
            2,
            "K=2 two typos tolerated ('rfusd' vs 'refused')",
        ),
        (
            "errno: \\w+ rfusd",
            0,
            "K=0 same query: no match because exact only",
        ),
    ];
    for (pat, k, label) in approx_queries {
        print_step(&format!("search_regex_approx({pat:?}, K={k}) -- {label}"));
        match idx.search_regex_approx(pat, *k) {
            Ok(hits) if hits.is_empty() => println!("   no hits"),
            Ok(hits) => {
                for h in &hits {
                    let pos = doc_ids.iter().position(|x| x == h).unwrap_or(usize::MAX);
                    let body = corpus.get(pos).copied().unwrap_or("?");
                    println!("   doc_id={h}: {body}");
                }
            }
            Err(e) => println!("   error: {e:?}"),
        }
    }
}

fn text_demo() {
    print_section("Text index + substring search (dyntext + pg_tre-like trigrams)");

    let mut idx = TextIndex::new();

    // 1. Insert a small corpus.
    print_step("Insert 8 documents");
    let corpus: &[&str] = &[
        "the quick brown fox jumps over the lazy dog",
        "errno: connection refused",
        "errno: no route to host",
        "the rain in spain stays mainly on the plain",
        "she sells seashells by the seashore",
        "regex: error: missing closing brace",
        "trigram extraction with byte-level padding",
        "all happy families are alike",
    ];
    let doc_ids: Vec<u32> = corpus
        .iter()
        .map(|t| {
            let id = idx.insert(t.as_bytes().to_vec());
            println!("   doc_id={id}: {t:?}");
            id
        })
        .collect();

    // 2. Substring search via trigram intersection + bloom +
    //    real-substring recheck.
    let queries: &[&str] = &["errno", "rain", "trigram", "missing closing", "happy", "x"];
    for q in queries {
        print_step(&format!(
            "search_substring({q:?}) -- tier-2 trigram intersect, tier-3 bloom, tier-4 substring recheck",
        ));
        let hits = idx.search_substring(q.as_bytes());
        if hits.is_empty() {
            println!("   no hits");
        } else {
            for h in &hits {
                let pos = doc_ids.iter().position(|x| x == h).unwrap_or(usize::MAX);
                let body = corpus.get(pos).copied().unwrap_or("?");
                println!("   doc_id={h}: {body}");
            }
        }
    }

    // 3. Stats.
    print_step("Index statistics");
    println!("   docs   : {}", idx.doc_count());
    println!("   trigrams in postings: {}", idx.postings().len());

    // 4. Demonstrate REGEX-style filtering today (Phase 1 only
    //    has substring; the regex AST + TRE FFI come in Phase
    //    2 + 3). Use the std `regex` crate as a stand-in to
    //    simulate Phase 4 on the substring-narrowed candidate
    //    set.
    print_section("Stand-in regex search via dyntext + std regex");
    print_step(
        "Two-stage approximate-regex: \\\n    1. trigrams the regex MUST contain narrow the candidate set;\\\n    2. std `regex` crate runs the full match against survivors.",
    );
    println!("   query regex: '^errno: \\w+ refused'");
    let must_contain = "errno";
    let candidates = idx.search_substring(must_contain.as_bytes());
    let re = ::regex::bytes::Regex::new(r"^errno: \w+ refused").unwrap();
    let mut shown = 0;
    for cand_id in &candidates {
        let pos = doc_ids
            .iter()
            .position(|x| x == cand_id)
            .unwrap_or(usize::MAX);
        let body = corpus.get(pos).copied().unwrap_or("");
        if re.is_match(body.as_bytes()) {
            println!("   doc_id={cand_id}: {body}");
            shown += 1;
        }
    }
    if shown == 0 {
        println!("   no hits");
    }
    println!();
    println!(
        "   (Phase 3 below uses the real TRE FFI for approximate-regex matching;\n    \
                 K=1 tolerates one typo, K=2 tolerates two.)",
    );

    text_demo_approx_regex(&idx, corpus, &doc_ids);
}

fn main() {
    println!();
    println!("============================================================");
    println!(" dynomite demo: vector + text search through the library API");
    println!("============================================================");
    println!();
    println!("This program exercises both surfaces in-process via the");
    println!("public Rust API. The same primitives sit behind the wire");
    println!("FT.* commands once Phase D wiring lands; the demo's");
    println!("dispatch points are exactly the ones the Redis parser will");
    println!("call.");

    vector_demo();
    text_demo();

    println!();
    println!("============================================================");
    println!(" demo complete");
    println!("============================================================");
    println!();
}
