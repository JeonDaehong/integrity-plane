//! Large-table mode (`--scale N`): one table of N rows in files of `--file-rows` rows, then the
//! commit shapes that matter at scale, each measured through the Plane: onboarding, a copy-on-write
//! delete of one row, merge-on-read deletes as delete files accumulate, and compaction.

use std::time::Instant;

use serde_json::json;

use super::{Bench, BoxError, Change, Metrics, Options, mib, seconds};

fn delta(after: &Metrics, before: &Metrics, name: &str) -> f64 {
    after.get(name) - before.get(name)
}

/// The current manifests of `table` (through the Plane).
async fn current(b: &Bench, table: &str) -> Result<Vec<String>, BoxError> {
    let head = b.head(&b.gateway, table).await?.ok_or("empty table")?;
    Ok(b.files
        .manifests_of
        .lock()
        .map_err(|_| "lock")?
        .get(&head)
        .cloned()
        .unwrap_or_default())
}

/// One measured commit through the Plane: (status, seconds, validation seconds, bytes read).
async fn measured(
    b: &Bench,
    table: &str,
    change: Change,
) -> Result<(u16, f64, f64, f64), BoxError> {
    let before = b.metrics().await?;
    let t = Instant::now();
    let status = b.commit(&b.gateway, table, &change).await?;
    let took = t.elapsed().as_secs_f64();
    let after = b.metrics().await?;
    Ok((
        status,
        took,
        delta(&after, &before, "integrity_validation_seconds_sum"),
        delta(&after, &before, "integrity_bytes_read_total"),
    ))
}

pub async fn run(b: &Bench, o: &Options) -> Result<String, BoxError> {
    let mut out = String::new();
    let mut line = |s: String| {
        eprintln!("{s}");
        out.push_str(&s);
        out.push('\n');
    };
    let rows = o.scale;
    let per = o.file_rows;
    let files = (rows + per - 1) / per;

    // Data written before the Plane enforces anything, one file per commit.
    let t = Instant::now();
    for k in 0..files {
        let ids: Vec<i64> = (k * per..((k + 1) * per).min(rows)).collect();
        let refs = vec![None; ids.len()];
        let status = b
            .commit(&b.upstream, "big", &Change::Append(ids, refs))
            .await?;
        assert_eq!(status, 200, "load");
    }
    line(format!(
        "| Load {rows} rows in {files} files of {per} rows (no Plane) | {} | | |",
        seconds(t.elapsed().as_secs_f64())
    ));

    // Onboarding: register the PRIMARY KEY.
    let t = Instant::now();
    let r = b
        .http
        .post(format!("{}/v1/integrity/constraints", b.gateway))
        .json(&json!({"table": "bench.big", "name": "pk_big", "type": "PRIMARY_KEY", "columns": ["id"]}))
        .send()
        .await?;
    let status = r.status();
    let body = r.text().await?;
    if !status.is_success() {
        line(format!(
            "| Onboarding {rows} keys | FAILED {status} | | {} |",
            body.chars().take(200).collect::<String>()
        ));
        return Ok(out);
    }
    line(format!(
        "| Onboarding scan of {rows} keys | {} | {:.0} keys/s | |",
        seconds(t.elapsed().as_secs_f64()),
        rows as f64 / t.elapsed().as_secs_f64()
    ));

    // Copy-on-write delete of one row: the whole file is rewritten without it.
    let list = current(b, "big").await?;
    let victim = list[0].clone();
    let ids: Vec<i64> = (1..per.min(rows)).collect(); // file 0 without id 0
    let rewritten = b.files.manifest(&ids, &vec![None; ids.len()])?;
    let new_list: Vec<String> = std::iter::once(rewritten)
        .chain(list.iter().filter(|m| **m != victim).cloned())
        .collect();
    let (status, took, validation, read) =
        measured(b, "big", Change::Raw(new_list, "overwrite")).await?;
    line(format!(
        "| Copy-on-write delete of 1 row (rewrites a {per}-row file) | end-to-end {}; validation {} | read {} | status {status} |",
        seconds(took),
        seconds(validation),
        mib(read)
    ));

    // Compaction of 10 untouched files into one.
    let list = current(b, "big").await?;
    // File k of the load holds ids k*per .. (k+1)*per; skip file 0 (rewritten above).
    let all_data: Vec<String> = {
        let map = b.files.data_of.lock().map_err(|_| "lock")?;
        let mut v: Vec<(i64, String)> = map
            .values()
            .filter_map(|d| {
                let n: i64 = d
                    .rsplit('/')
                    .next()?
                    .strip_prefix('d')?
                    .strip_suffix(".parquet")?
                    .parse()
                    .ok()?;
                Some((n, d.clone()))
            })
            .collect();
        v.sort();
        v.into_iter().map(|(_, d)| d).collect()
    };
    let (rows_of, data_of) = (
        b.files.rows_of.lock().map_err(|_| "lock")?.clone(),
        b.files.data_of.lock().map_err(|_| "lock")?.clone(),
    );
    let pick: Vec<String> = list
        .iter()
        .filter(|m| data_of.contains_key(*m))
        .filter(|m| rows_of.get(*m) == Some(&per))
        .skip(1)
        .take(10)
        .cloned()
        .collect();
    if pick.len() == 10 {
        let ranges: Vec<i64> = pick
            .iter()
            .filter_map(|m| {
                let d = data_of.get(m)?;
                all_data.iter().position(|x| x == d)
            })
            .map(|k| k as i64)
            .collect();
        let mut ids = Vec::new();
        for k in &ranges {
            ids.extend(k * per..(k + 1) * per);
        }
        let merged = b.files.manifest(&ids, &vec![None; ids.len()])?;
        let new_list: Vec<String> = list
            .iter()
            .filter(|m| !pick.contains(m))
            .cloned()
            .chain(std::iter::once(merged))
            .collect();
        let (status, took, validation, read) =
            measured(b, "big", Change::Raw(new_list, "replace")).await?;
        line(format!(
            "| Compaction of 10 files ({} rows) into one | end-to-end {}; validation {} | read {} | status {status} |",
            10 * per,
            seconds(took),
            seconds(validation),
            mib(read)
        ));
    } else {
        line("| Compaction | skipped: fewer than 10 untouched files | | |".into());
    }
    // Merge-on-read deletes, one row each, accumulating delete files.
    let manifests = current(b, "big").await?;
    let data: Vec<String> = {
        let map = b.files.data_of.lock().map_err(|_| "lock")?;
        manifests
            .iter()
            .filter_map(|m| map.get(m).cloned())
            .collect()
    };
    let mut timings = Vec::new();
    for i in 0..o.mor_deletes {
        let target = data[1 + i % (data.len() - 1).max(1)].clone();
        let dm = b
            .files
            .position_deletes(&[(target, (i as i64 / data.len() as i64) + 1)])?;
        let mut list = current(b, "big").await?;
        list.push(dm);
        let (status, took, validation, read) =
            measured(b, "big", Change::Raw(list, "delete")).await?;
        assert_eq!(status, 200, "merge-on-read delete {i}");
        timings.push((took, validation, read));
    }
    if let (Some(first), Some(last)) = (timings.first(), timings.last()) {
        line(format!(
            "| Merge-on-read delete of 1 row, {} times (delete files accumulate) | first: end-to-end {} / validation {}; last: {} / {} | read first {}, last {} | |",
            o.mor_deletes,
            seconds(first.0),
            seconds(first.1),
            seconds(last.0),
            seconds(last.1),
            mib(first.2),
            mib(last.2)
        ));
    }

    Ok(out)
}
