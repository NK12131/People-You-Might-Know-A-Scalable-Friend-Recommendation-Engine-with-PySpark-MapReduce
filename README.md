# People You Might Know: Friend Recommendation with PySpark

A scalable **"People You Might Know"** recommender built on the MapReduce model with **PySpark**. The core idea is simple: if two people have many mutual friends but aren't connected yet, they should probably meet. The system scans the LiveJournal social graph and gives each user up to **10 friend suggestions**, ranked by how many mutual friends they share.

---

## Table of Contents
- [How It Works](#how-it-works)
- [Dataset](#dataset)
- [Getting Started](#getting-started)
- [Sample Output](#sample-output)
- [Project Structure](#project-structure)
- [Possible Improvements](#possible-improvements)

---

## How It Works

The pipeline is a chain of map and reduce steps over Spark RDDs:

1. **Parse.** Each line `<user>\t<friend1>,<friend2>,...` is mapped to `(user_id, [friend_ids])`.
2. **Emit pairs (flatMap).** For every user:
   - each direct friendship `(user, friend)` is emitted with value **0** (already friends)
   - each pair of that user's friends `(friend_a, friend_b)` is emitted with value **1** (one mutual friend)

   Pairs are stored in sorted order `(smaller_id, larger_id)` so that `(a, b)` and `(b, a)` share a key.
3. **Reduce (groupByKey).** Group by pair, **drop any pair that contains a 0** (they're already friends), and sum the remaining 1s to get the **mutual-friend count**.
4. **Fan out.** Each `((a, b), count)` becomes two records, `(a, (b, count))` and `(b, (a, count))`.
5. **Rank.** For each user, sort candidates by **mutual-friend count (descending)**, break ties by **user ID (ascending)**, and keep the top 10.

```
user → friends ──flatMap──► ((u,v), 0|1) ──groupByKey + filter──► ((u,v), mutual_count)
        ──flatMap──► (u, (v, count)) ──groupByKey + sort──► (u, [top 10 recommendations])
```

## Dataset

The project uses `soc-LiveJournal1Adj.txt`, an adjacency list built from the [SNAP LiveJournal social network](https://snap.stanford.edu/data/soc-LiveJournal1.html).

Each line has this format:

```
<UserID><TAB><FriendID1>,<FriendID2>,...
```

A user can have an empty friend list.

## Getting Started

### Prerequisites
- Python 3.x
- Apache Spark / PySpark

```bash
pip install pyspark
```

### Run

1. Clone the repo:
   ```bash
   git clone https://github.com/parthjain99/Friend-recommendation-System.git
   cd Friend-recommendation-System
   ```
2. Download `soc-LiveJournal1Adj.txt` and change the input path in `Answer.py`. It's set to `/content/` by default, which is where Google Colab keeps files:
   ```python
   people = sc.textFile('/path/to/soc-LiveJournal1Adj.txt')
   ```
3. Run the script:
   ```bash
   spark-submit Answer.py
   ```
   You can also paste it into a Jupyter or Colab notebook with PySpark installed.

By default the script prints recommendations for this sample of users:
`924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993`.
To see other users, edit the filter list in the last lines of `Answer.py`.

## Sample Output

```
(924,  [439, 2409, 6995, 11860, 15416, 43748, 45881])
(8941, [8943, 8944, 8940])
(8942, [8939, 8940, 8943, 8944])
(9019, [9022, 317, 9023])
(9020, [9021, 9016, 9017, 9022, 317, 9023])
(9021, [9020, 9016, 9017, 9022, 317, 9023])
(9022, [9019, 9020, 9021, 317, 9016, 9017, 9023])
(9990, [13134, 13478, 13877, 34299, 34485, 34642, 37941])
(9992, [9987, 9989, 35667, 9991])
(9993, [9991, 13134, 13478, 13877, 34299, 34485, 34642, 37941])
```

Users with fewer than 10 candidates who share at least one mutual friend get shorter lists.

## Project Structure

```
├── Answer.py                  # PySpark MapReduce implementation
├── Algorithmic Approach.pdf   # Write-up of the algorithm and results
└── README.md
```

## Possible Improvements
- Swap `groupByKey` for `reduceByKey` / `aggregateByKey` to cut shuffle size and memory use.
- Pass the input path, the user IDs and the top-N value as command-line arguments.
- Write the full set of recommendations to disk (`saveAsTextFile`) instead of printing a sample.
- Port the job to the Spark DataFrame API for Catalyst optimizations.
- Try weighted similarity (for example Jaccard or Adamic–Adar) instead of raw mutual-friend counts.

---

**Tech:** Python · PySpark · MapReduce · Graph Analytics