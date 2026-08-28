---
title: "TIMESTAMP_NS"
description: "Timestamp with nanosecond precision"
---

# TIMESTAMP_NS


| Property | Value |
|----------|-------|
| Storage | Timestamp with nanosecond precision (without timezone) |
| Use case | High-precision event logging, IoT sensor data |

```sql
CREATE TABLE high_freq_events (id BIGINT, event_time TIMESTAMP_NS);
```
