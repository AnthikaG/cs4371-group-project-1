# Part 2 report notes

## AG News class counts

| Label | Class | Article count |
|---:|---|---:|
| 0 | World | 30000 |
| 1 | Sports | 30000 |
| 2 | Business | 30000 |
| 3 | Sci/Tech | 30000 |

## Streamed-data cleaning counts

- Raw capture/Kafka messages: 3000
- Remaining after null, empty, whitespace-only, and duplicate-text removal: 2958
- Prediction CSV data rows: 2958 (matches the cleaned count)

## Pipeline stages and parameters

1. `RegexTokenizer`: lowercase; splits on `\W+`; minimum token length 2.
2. `StopWordsRemover`: Spark's built-in English stop-word list.
3. `HashingTF`: `numFeatures=262144` (`2^18`).
4. `IDF`: `minDocFreq=2`.
5. `LogisticRegression`: `maxIter=20`, `regParam=0.01`.

- Fixed split seed: `4371`; training/holdout split: 90%/10%.
- Holdout accuracy: 0.892341

## Batch results per class

| Predicted class | Count |
|---|---:|
| World | 399 |
| Sports | 449 |
| Business | 888 |
| Sci/Tech | 1222 |
| **Total** | **2958** |

## 5 examples per class

Five rows per class as printed by `batch_predict`, with the model's confidence (max class probability). Copied from the actual output, not edited.

### World

1. confidence=0.800820: "No relief for locked boys. Join my collection? 🔑😈" Meredith Stout Model: @rigid3d.bsky.social
2. confidence=0.345109: #History fans! New Reading the Past today! [www.youtube.com/watch?v=IelG](https://www.youtube.com/watch?v=IelG)... Richard II: The First Renaissance Prince? @katrina-marchant.bsky.social
3. confidence=0.601423: #Nullvember All taped up for work 🖤🔒
4. confidence=0.544373: #POE #PathOfExile Nothing to write home about, yet here we are :)
5. confidence=0.431373: 19:00 - 11m³/s (-1.0)

### Sports

1. confidence=0.564676: !!! U gotta give it a try when you have the chance! They sometimes put them on curry too 🤤🤤
2. confidence=0.947444: "Depp captures the essence of Scrooge while bringing his own distinct flavor to a role that we’ve seen portrayed in so many different forms before. He delivers the perfect blend of serious emotions and heartfelt comedy." [www.theouterhaven.net/ebenezer-rev](https://www.theouterhaven.net/ebenezer-rev)...
3. confidence=0.336973: "Hm. Another... champion."
4. confidence=0.542677: "Pope Leo XIV spent Thursday afternoon in Lima sitting with six people the Church had failed. That evening, he stood before 60,000 young Peruvians at the National Stadium and told them they were the hope their country had been waiting for."
5. confidence=0.393609: "sure it happened to everyone else, but to me? never. I'm the most special boy." - dailygrumbles, probably

### Business

1. confidence=0.910110: "AND the (cascading and global) shareholder lawsuits are gonna be lit . . . " "IF SHE'S A TEN AND YOU ARE AN ELDERLY CENTI-BILLIONAIRE WEARING SHINY TOM FORD SHE'S PROBABLY A SPY . . . " "THAT IS JUST MATH . . . " MADE IN USA // 2026 ANOTHER NOVEMBER [www.wired.com/story/chinas](https://www.wired.com/story/chinas)...
2. confidence=0.457219: "Extree extree! Read alla bout it! Geoff Keighley does nothing to elevate the video game industry for yet another year!"
3. confidence=0.344006: "Let's finish setting up your PC!"
4. confidence=0.321924: "The American people dont want policies that help the American people." --stupid asshole
5. confidence=0.495698: "Trying" isn't enough. Voters want real accountability—prosecuting corruption, taxing extreme wealth to invest in the working class, and dismantling every harmful policy Trump put in place. Don't settle for half-measures.

### Sci/Tech

1. confidence=0.526610: "It's these people's fault for catching and punishing him because if they never did that he wouldn't have been caught" lookin ass Would he like to join him?
2. confidence=0.757395: "Whoever walks on his head, ladies and gentlemen, whoever walks on his head has heaven beneath him as an abyss." - Paul Celan
3. confidence=0.321411: #CYS issues Area Forecast Discussion (AFD) at Nov 13, 11:11 AM MST Link
4. confidence=0.387788: #Canadian alternatives✅
5. confidence=0.872143: #EcksteinsMonthOfMusic 🎼 #MusicChallenge 🔥 #Music 🎵 #MusicSky 🎶 Day 13 - earth/dirt Siouxsie And The Banshees - Cities In Dust youtu.be/wsOHvP1XnRg?...

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