# Redis Stream Workflow

This project demonstrates a producer-consumer pattern using Redis Streams and consumer groups, including automatic workload distribution and fault-tolerance mechanisms.

> **Note:** Focus primarily on these four projects: `BaseRedis`, `Producer`, `Consumer`, and `Consumer2`. Other projects in the repository are experimental and can be ignored.

## Project Structure

### 1. Producer
The producer application runs two parallel tasks:
- **DX Connector Simulator**
- **TL Connector Simulator**

Both simulators continuously push data entries into a Redis Stream every **100 milliseconds**, simulating real-time data ingestion.

### 2. Consumers
Two consumers (`Consumer` and `Consumer2`) represent services within an autoscaling group. They share workload by joining the same Redis consumer group.

#### Each consumer consists of two main tasks:

- **Listen Task**:
    - Continuously listens to new messages via `XREADGROUP` with a configurable blocking timeout.
    - Uses `XAUTOCLAIM` to periodically reclaim and process pending messages from inactive consumers to ensure messages are reliably processed.

- **Monitor Task**:
    - Periodically updates the consumer’s status in a Redis sorted set to signal health and activity.
    - If a consumer becomes inactive or crashes, the monitor task's absence triggers message reassignment to active consumers.

## Fault Tolerance and Load Balancing

- Messages not acknowledged due to consumer failure are automatically claimed by active consumers (`XAUTOCLAIM`).
- Consumers dynamically balance the message processing load, ensuring high availability and reliability.

## Redis Structure

- **Stream**: Holds messages from producers.
- **Consumer Group**: Manages the distribution of stream messages among consumers.
- **Sorted Set**: Tracks consumer status for monitoring and failover logic.

