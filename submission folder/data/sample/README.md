# sample fixture

`topic1_sample.jsonl` holds 10 **hand-written, synthetic** messages (source = `synthetic_fixture`).
It exists only so teammates can develop against the topic1 schema before live data is collected.
It is **not** real streamed data and must not be used for any reported result.

`topic2_sample.jsonl` contains ten actual entity-count records from run
`tulasi-entities-20261116`, which processed 146 real Bluesky messages. These are
cumulative totals for that run, not deltas or input messages. See
`evidence/tulasi_results.json` for the verified counts and limitations.
