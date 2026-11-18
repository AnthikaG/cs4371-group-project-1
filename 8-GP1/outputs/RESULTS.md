# Additional classification results

The supplied models/ag_lr PipelineModel was loaded and applied to outputs/demo-live.jsonl. There were 4,394 input messages and 4,335 unique nonempty texts. outputs/predictions.csv records a class and predicted-class probability for every cleaned message. Five examples per class are in outputs/prediction_examples.csv.

| Predicted class | Count |
| --- | ---: |
| Business | 1351 |
| Sci/Tech | 1793 |
| Sports | 667 |
| World | 524 |

The 50-row comparison supplied from report Table 5.5 is saved separately as hand_labels_vs_model_manual.csv. It contains 35 None-of-these labels and 15 in-scope labels. Two in-scope predictions are correct, giving 2/15 = 13.3333%. These are the human-provided original report labels, not AI-generated labels or a new evaluation of the 4,335-message dataset. Original sampled text/IDs were not supplied with that table.

The four-class AG News classifier must assign a news category even to informal conversation outside those categories. The 35 out-of-scope labels demonstrate this mismatch within the supplied sample; the sample does not establish accuracy for all Bluesky posts.

Examples of mistakes reported in Sristi's original REPORT_NOTES.md include "Ummm, I think you just did!" (human World, model Business), a Richmond Centre Christmas-tree post (human Business, model Sci/Tech), and a housing/consumer-advocacy post (human World, model Sci/Tech). These examples are attributed to those original notes; they were not reconstructed as new message IDs.

The live classifier used the same saved model and showed counts increasing after six real collected messages were replayed into Kafka. Full console output is under evidence/classification/. No training, inference, manual-label or timed-screenshot result has been fabricated. The interval bar plots are explicitly retrospective from retained Kafka snapshots; repeated 45/60-minute plots reflect a pause in incoming data.
