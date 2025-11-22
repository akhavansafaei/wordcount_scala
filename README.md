# Word Co-Occurrence Analysis with Spark Streaming

A Scala-based Apache Spark Streaming application that performs real-time word counting and co-occurrence frequency analysis on text data streams.

## Overview

This project implements a distributed stream processing application using Apache Spark that monitors an input directory for new text files and performs three types of analysis:

- **Task A**: Word frequency counting
- **Task B**: Word pair co-occurrence frequency (per batch)
- **Task C**: Cumulative word pair co-occurrence frequency (stateful)

## Project Structure

```
wordcount_scala/
├── hdcount.scala              # Main streaming application
├── rmit/
│   ├── assinment3.scala      # Alternative implementation
│   ├── assignment-3.jar      # Compiled JAR file
│   └── readme2.txt           # Deployment instructions
├── task3.jar                  # Compiled JAR file
├── 1.txt                      # Sample test data
├── 2.txt                      # Sample test data
├── test.txt                   # Sample test data
└── readme.txt                 # Original readme
```

## Features

### Task A: Word Frequency Count
- Filters words to include only alphabetic characters with length ≥ 3
- Counts word occurrences in each streaming batch
- Outputs results with sequential numbering (taskA-001, taskA-002, etc.)

### Task B: Co-Occurrence Frequency (Batch)
- Identifies word pairs that appear together in the same line
- Calculates co-occurrence frequency for each batch
- Creates bidirectional pairs (both (word1, word2) and (word2, word1))
- Outputs results with sequential numbering (taskB-001, taskB-002, etc.)

### Task C: Cumulative Co-Occurrence Frequency (Stateful)
- Maintains cumulative state across all batches using `updateStateByKey`
- Tracks word pair co-occurrence across the entire stream history
- Requires checkpoint directory for fault tolerance
- Outputs results with sequential numbering (taskC-001, taskC-002, etc.)

## Requirements

- Apache Spark 2.x or higher
- Scala 2.11 or 2.12
- Hadoop/HDFS (for distributed deployment)
- AWS EMR (optional, for cloud deployment)

## Building the Project

To compile the Scala source code into a JAR file:

```bash
scalac -classpath "path/to/spark-jars/*" hdcount.scala
jar cf wordcount.jar streaming/*.class
```

Or use the pre-compiled JAR files:
- `task3.jar`
- `rmit/assignment-3.jar`

## Running Locally

```bash
spark-submit \
  --master local[*] \
  --class streaming.hdcount \
  task3.jar \
  /path/to/input/directory \
  /path/to/output/directory
```

or

```bash
spark-submit \
  --master local[*] \
  --class streaming.assinment3 \
  rmit/assignment-3.jar \
  /path/to/input/directory \
  /path/to/output/directory
```

## Running on AWS EMR

### Step 1: Initialize AWS EMR Cluster
1. Log in to AWS Management Console
2. Navigate to AWS EMR service
3. Create a new EMR cluster with Spark

### Step 2: Create HDFS Directories
SSH into the EMR cluster and create required directories:

```bash
hdfs dfs -mkdir hdfs:///input
hdfs dfs -mkdir hdfs:///output
```

### Step 3: Submit the Spark Job
Upload the JAR file to the cluster and execute:

```bash
spark-submit \
  --master yarn \
  --class streaming.assinment3 \
  assignment-3.jar \
  hdfs:///input \
  hdfs:///output
```

### Step 4: Verify Output
Check the output directory for results:
```bash
hdfs dfs -ls hdfs:///output/taskA-*
hdfs dfs -ls hdfs:///output/taskB-*
hdfs dfs -ls hdfs:///output/taskC-*
```

## Configuration

### Streaming Parameters
- **Batch Interval**: 3 seconds
- **Checkpoint Directory**: Current directory (`.`)
- **Master**: Local mode with all cores (`local[*]`)

### Word Filtering Rules
- Only alphabetic characters (a-z, A-Z)
- Minimum word length: 3 characters
- Non-matching words are filtered out

## Input Data Format

The application monitors an input directory for new text files. Example test data:

**1.txt:**
```
I*** like pig latin I like hive too2 I don't like hive too.
```

**2.txt:**
```
like pig like hive
```

After filtering, only valid words are processed (e.g., "like", "pig", "latin", "hive").

## Output Format

### Task A Output
```
(word, count)
```
Example:
```
(like, 4)
(pig, 2)
(hive, 2)
```

### Task B Output
```
((word1, word2), count)
```
Example:
```
((like, pig), 2)
((pig, like), 2)
((pig, hive), 1)
```

### Task C Output
Cumulative counts across all batches:
```
((word1, word2), cumulative_count)
```

## Implementation Details

- **Language**: Scala
- **Framework**: Apache Spark Streaming
- **Streaming Window**: 3 seconds
- **State Management**: updateStateByKey for Task C
- **Checkpointing**: Required for stateful operations

## Notes

- The application uses `textFileStream` to monitor the input directory
- Empty RDDs are skipped during processing
- Output files are saved with sequential numbering
- Checkpoint directory must be writable for Task C to function

## Authors

RMIT University Assignment

## License

Academic/Educational Use
