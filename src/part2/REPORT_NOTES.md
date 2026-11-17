# Part 2 report notes

## AG News class counts

| Label | Class | Article count |
|---:|---|---:|
| 0 | World | <FILL FROM OUTPUT> |
| 1 | Sports | <FILL FROM OUTPUT> |
| 2 | Business | <FILL FROM OUTPUT> |
| 3 | Sci/Tech | <FILL FROM OUTPUT> |

## Streamed-data cleaning counts

- Raw capture/Kafka messages: <FILL FROM OUTPUT>
- Remaining after null, empty, whitespace-only, and duplicate-text removal: <FILL FROM OUTPUT>

## Pipeline stages and parameters

1. `RegexTokenizer`: lowercase; splits on `\W+`; minimum token length 2.
2. `StopWordsRemover`: Spark's built-in English stop-word list.
3. `HashingTF`: `numFeatures=262144` (`2^18`).
4. `IDF`: `minDocFreq=2`.
5. `LogisticRegression`: `maxIter=20`, `regParam=0.01`.

- Fixed split seed: `4371`; training/holdout split: 90%/10%.
- Holdout accuracy: <FILL FROM OUTPUT>

## Batch results per class

| Predicted class | Count |
|---|---:|
| World | <FILL FROM OUTPUT> |
| Sports | <FILL FROM OUTPUT> |
| Business | <FILL FROM OUTPUT> |
| Sci/Tech | <FILL FROM OUTPUT> |

## 5 examples per class

Copy five actual rows per class from batch-prediction output. Do not invent example messages or confidence values.

### World

<FILL FROM OUTPUT>

### Sports

<FILL FROM OUTPUT>

### Business

<FILL FROM OUTPUT>

### Sci/Tech

<FILL FROM OUTPUT>

## 50 hand-label results

- Number labeled `None of these`: <FILL FROM OUTPUT>
- Number in-scope: <FILL FROM OUTPUT>
- Accuracy on in-scope labels only: <FILL FROM OUTPUT>
- Wrong in-scope labels and comparison file: <FILL FROM OUTPUT>

## Live classification query + screenshot

- Query start time and run/checkpoint details: <FILL FROM OUTPUT>
- Per-class counts observed: <FILL FROM OUTPUT>
- Screenshot path: <FILL FROM OUTPUT>

## Summary

- Do predictions make sense for Bluesky? <FILL FROM OUTPUT>
- Which messages are handled well or badly, and why? <FILL FROM OUTPUT>
- Three to five actual wrong examples from the hand-label results: <FILL FROM OUTPUT>
