# CS 4371 GP1 | TEAM CONTRACT

## source of truth
Read `Group_Project_1_CS4371.pdf` before coding. Assignment requirements override this contract. All agents must inspect the repository and existing interfaces before edits. Do not fabricate results, screenshots, counts, metrics, or experiments. If a requirement cannot be completed, report it explicitly. This contract applies to all four teammates and both code and report prompts.

## repository and collaboration
- Ashi creates one PRIVATE GitHub repository, `cs4371-group-project-1`, invites Tulasi, Anthika, and Sristi as collaborators, and pushes the scaffold to `main` before teammates begin. Do not assume GitHub accounts or credentials exist; humans complete invitations/authentication.
- `main` is protected by convention: no direct commits after initial scaffold. Branches: `feature/ashi-ingestion`, `feature/tulasi-entities`, `feature/anthika-visualization`, `feature/sristi-classification`; use pull requests, one reviewer, and merge after tests. Never force-push shared branches.
- If a GitHub repository already exists, reuse it and avoid overwriting work. Each person pulls latest `main` before branching and rebases/merges regularly. Only edit owned paths unless a PR explicitly coordinates a shared-file change.
- Any shared interface change requires a PR, an update to this contract, and approval from affected teammates before merge. Communicate schema changes before implementation.
- Use issues or a shared task board for blockers and decisions. Each PR includes: changed files, run commands, test evidence, assumptions, and known limitations.

## agreed layout
```
project/
  README.md
  requirements.txt
  .env.example
  .gitignore
  docker-compose.yml
  config/
    settings.py
  src/
    common/
      schemas.py
      spark_utils.py
    ingestion/
      producer.py
    streaming/
      entity_stream.py
      classification_stream.py
    ml/
      train.py
      batch_predict.py
      manual_review.py
  infrastructure/
    logstash/
      pipeline.conf
    elasticsearch/
    kibana/
  tests/
  data/
    sample/                # small synthetic/non-sensitive fixtures only
  outputs/                 # generated; do not commit large outputs
  evidence/                # screenshots, summaries, run metadata
  report/
    sections/
    figures/
    final_report.pdf
  ai_transcripts/          # final PDFs of complete AI chat transcripts
```
The supplied prompt package is planning material, not a finished implementation. Code agents may scaffold these paths. Do not silently rename them.

## shared data contract
- Kafka bootstrap: `KAFKA_BOOTSTRAP_SERVERS`, default `localhost:9092` (overrideable).
- Input topic: `KAFKA_INPUT_TOPIC`, default `topic1`.
- Entity output topic: `KAFKA_ENTITY_TOPIC`, default `topic2`.
- Optional classification topic: `KAFKA_CLASS_TOPIC`, default `topic3`; console output is sufficient for assignment.
- `topic1` Kafka VALUE is UTF-8 JSON with `message_id` (string), `text` (string), `source` (string), `event_time` (UTC ISO-8601 string). `message_id` must be stable across retries. The text field must be nonempty after trimming. Do not include API keys or secrets.
- `topic2` Kafka VALUE is UTF-8 JSON with `entity` (string), `entity_type` (string), `count` (nonnegative integer), `window_end` (UTC ISO-8601 string), `run_id` (string). Semantics: cumulative count as of snapshot trigger for each `(entity, entity_type)` in this run; **not a delta**. Kibana must show latest snapshot per entity for a run, not sum snapshots (which would double count). If implementing a different safe snapshot method, coordinate and update contract first.
- `topic3` optional VALUE: `class_name` (World/Sports/Business/Sci/Tech), `count` (integer), `window_end`, `run_id`.
- AG News labels: 0=World, 1=Sports, 2=Business, 3=Sci/Tech. Manual review additionally allows `None of these`. Preserve numeric and human-readable labels.
- ML prediction CSV/Parquet contains `message_id`, `text`, `predicted_label`, `predicted_class`, `predicted_probability` (probability of predicted class), with optional full probability vector. Review output adds `manual_label`, `is_in_scope`, `is_correct` (nullable for None of these).
- All local paths are CLI arguments or environment-configured, never hardcoded absolute local paths. Spark model path, AG News parquet, checkpoint dirs, output dirs, and data files are configurable.
- Both Spark streaming queries must read `topic1` independently; classification query must load the SAME saved PipelineModel as batch inference.

## code and documentation style
- Python 3, four spaces, snake_case variables/functions/modules, PascalCase classes, UPPER_SNAKE_CASE constants. Prefer clear names (`entity_count`, `message_id`) over abbreviations.
- Use type hints where practical, docstrings for public functions (docstrings may be grammatical), `pathlib` for paths, `argparse` for CLIs, logging rather than print except intentional Spark console output.
- Comments: **lowercase first character only; fragments, not full sentences; no terminal period**. Example: `# avoid duplicate snapshots across triggers`. Do not comment obvious assignments/imports/loops. Add comments only for tricky algorithms, surprising Spark/Kafka semantics, concurrency, state, or non-obvious workarounds. Never add sentence-like or capitalized comments.
- Use double quotes for strings, consistent imports (standard library, third party, local), short cohesive functions, no unexplained magic constants, no wildcard imports, no unrelated refactors.
- Fail clearly for missing configuration, missing services, absent model/data, malformed messages, and missing dependencies. Log useful counts without logging secrets.
- Use `.env.example` with placeholders only. `.env`, API keys, credentials, local datasets, checkpoints, generated model artifacts, and large outputs belong in `.gitignore`.
- Pin compatible dependency versions after a working integration test; do not blindly mix incompatible Spark/Kafka/Elastic versions. Kafka Spark connector must match Spark and Scala build; validate versions in README.
- Tests should use small deterministic fixtures; never require live external API for unit tests. Keep runtime commands and results reproducible.

## dependencies and readiness gates
- Gate 0 (Ashi): GitHub repo, scaffold, contract, topic schema, environment instructions, basic Docker/Kafka connectivity. All can start planning/report outlines before gate 0; code integration waits for scaffold.
- Gate 1 (Ashi): producer emits valid `topic1` JSON and saves/replays >=1,000 real streamed messages (after collection); share sample fixture immediately, live data later.
- Gate 2 (Tulasi): entity stream publishes `topic2` snapshots; Anthika can configure infrastructure from contract before this but needs real `topic2` messages to validate dashboards.
- Gate 3 (Sristi): model trained/saved and batch predictions verified; integrate second streaming query with Tulasi after agreeing on Spark session/config/checkpoints. Sristi can train on AG News before Gate 1, but needs live messages for evaluation.
- Gate 4 (All): end-to-end demo, 15/30/45/60-minute evidence if feasible, manual labels, merged PRs, report, video, transcript PDFs, ZIP.

## report and submission contract
- Report first page: team number, all four full names and NetIDs, contribution table, accessible narrated demo link. Humans supply missing NetIDs/team number and upload/permissions.
- Report includes source, ingestion counts, entity plots at multiple intervals, result interpretation, AG News per-class counts, pipeline and parameters, batch class counts, five examples per predicted class with probabilities, 50 genuinely hand-labeled random messages, in-scope accuracy and None-of-these count, streaming class counts, errors and limitations.
- Every member narrates their own work in one screen recording, total **8 minutes maximum**. Video must be accessible without approval; put link on report page 1; do not submit video file.
- AI appendix lists each assistant/tool, user, purpose. Submit **full chat transcripts as PDFs** in `ai_transcripts/`. Do not claim a transcript is complete if it is not. Human authors review and attest to their own work.
- One member submits `<TeamNumber>-GP1.zip` by **November 15, 2026 at 11:59 PM**, including both parts' code, README, plots, output prediction and 50-label files, report PDF, ai_transcripts. Avoid including `.env`, secrets, huge checkpoints, or unnecessary runtime data; include reproducible run instructions.
