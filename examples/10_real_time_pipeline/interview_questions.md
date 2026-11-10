# Top Interview Questions & Expert Answers: Real-Time Streaming & Fraud Detection

### Q1: What is the difference between Tumbling Windows, Sliding Windows, and Session Windows in Spark Structured Streaming?
**Answer:**
- **Tumbling Windows**: Fixed-duration, non-overlapping time windows (e.g., every 10 minutes: 10:00–10:10, 10:10–10:20). Every event belongs to exactly one window.
- **Sliding Windows**: Fixed-duration windows that slide by a specified interval smaller than the duration (e.g., a 10-minute window sliding every 2 minutes: 10:00–10:10, 10:02–10:12, 10:04–10:14). An event can belong to multiple overlapping windows. Essential for velocity detection (e.g., 3 swipes in the last 10 minutes).
- **Session Windows**: Dynamic windows defined by periods of user activity separated by a specified gap of inactivity (e.g., 30 minutes of idle time closes the user session).

---

### Q2: How does Spark's `withWatermark()` function work, and what determines when state store memory is cleaned up?
**Answer:**
In stateful stream processing (windowed aggregations, stream-stream joins), Spark maintains intermediate aggregate state in memory. Without watermarking, state would grow indefinitely until executor memory crashes (`OutOfMemoryError`).

**Watermark Mechanics**:
`df.withWatermark("event_time", "10 minutes")`
Spark tracks the maximum event time observed so far: $T_{max}$.
The watermark threshold is calculated as: $W = T_{max} - 10\text{ minutes}$.
1. **Late Data Acceptance**: Any event with `event_time >= W` is accepted and updates the window aggregate.
2. **Late Data Dropping**: Any event with `event_time < W` is discarded immediately as too late.
3. **State Cleanup**: When the watermark passes the end time of a window ($W > \text{Window End}$), Spark finalizes the window output and purges that window's state from the executor state store.

---

### Q3: Why should production streaming jobs on Databricks use RocksDB as the state store provider rather than the default HDFS backed state store?
**Answer:**
- **Default HDFS State Store**: Stores Java objects on the JVM Heap. When state grows to millions of keys (e.g., active cardholders), JVM Garbage Collection pauses (Stop-The-World GC) cause micro-batches to stall, introducing huge latency spikes.
- **RocksDB State Store (`spark.sql.streaming.stateStore.providerClass`)**:
  - Stores state off-heap in native memory using RocksDB's fast C++ key-value engine.
  - Spills cleanly to local NVMe SSDs when state exceeds RAM.
  - Eliminates JVM garbage collection pauses completely, delivering predictable sub-second latency for millions of streaming keys.

---

### Q4: What is the difference between Micro-Batch Execution and Continuous Processing mode in Structured Streaming?
**Answer:**
- **Micro-Batch Mode (Default, e.g., `trigger(processingTime="1 second")`)**:
  - Spark batches incoming records every second, executing an optimized Spark engine job.
  - Latency: 100 milliseconds to 1 second.
  - Supports full SQL syntax, aggregations, joins, and Delta Lake ACID sinks.
- **Continuous Processing Mode (`trigger(continuous="10 milliseconds")`)**:
  - Long-running executor threads poll Kafka continuously without creating discrete batches.
  - Latency: Sub-millisecond (1ms - 5ms).
  - Severe limitations: Only supports map-like operations (projections, filters); does not support windowed aggregations or complex joins.

---

### Q5: How do you prevent out-of-order Kafka message partitions from producing false positive fraud alerts?
**Answer:**
Because Kafka topics are distributed across multiple partitions, messages from the same user could arrive out-of-order if partitions are assigned randomly.
1. **Partition by Key at the Producer**: Kafka producers must key messages by `card_id` (`producer.send(topic, key=card_id, value=payload)`). This guarantees all transactions for a specific cardholder arrive on the exact same Kafka partition in strict temporal order.
2. **Event-Time Processing**: Always aggregate on the original transactional event timestamp generated at the terminal (`event_time`), never on the ingestion wall-clock time (`processing_time`).
