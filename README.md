# TelemetryIngestion

A small TCP ingestion service for device telemetry, written in C#/.NET 8.

Devices connect over TCP and send messages in a binary protocol:

```
[Sync Word 0xAA55][Device Id 4][Message Counter 2][Message Type 1][Payload Length 2][Payload]
```

Multi-byte fields are big-endian. The service reassembles messages from the byte stream, skips invalid data, drops duplicates, and routes each message by type to one of two in-memory destinations:

- **Device Message**: types 2, 11, 13
- **Device Event**: types 1, 3, 12, 14

## Flow

```
TCP connection -> FrameParser -> routing / duplicate check -> bounded Channel -> consumer
```

- `TcpServer` accepts connections and handles each one on its own task.
- `ConnectionHandler` reads with `NetworkStream.ReadAsync` and feeds the bytes to a per-connection `FrameParser`.
- `FrameParser` finds the sync word, waits for complete frames, and resyncs after garbage or an invalid header.
- `MessageRouter` picks the destination by message type. Unsupported types are rejected here.
- `DuplicateFilter` drops messages already seen for the same Device Id and Message Counter.
- `MessageQueue` is a bounded `Channel`. Each destination has its own queue and its own `DestinationConsumer`, which currently just logs the message.

## Build, test, run

Requires the .NET 8 SDK or later. The projects target `net8.0`.

```bash
dotnet build
```

```bash
dotnet test
```

```bash
dotnet run --project src/TelemetryIngestion
```

The service listens on port 9000 by default. Stop it with Ctrl+C.

## Configuration

Settings are in `src/TelemetryIngestion/appsettings.json` under `Ingestion`:

| Setting | Default | Meaning |
|---|---|---|
| `Port` | 9000 | TCP port to listen on |
| `MaxPayloadLength` | 1024 | Longest payload accepted. A longer length is treated as a corrupt header. |
| `QueueCapacity` | 1000 | Capacity of each destination queue |

They can also be overridden on the command line, for example:

```bash
dotnet run --project src/TelemetryIngestion -- --Ingestion:Port=9100
```

## Duplicate detection

A duplicate is a message with the same Device Id and Message Counter. For each device, the service remembers its last 64 counters. Because only recent counters are kept, the 16-bit counter can wrap around without new messages being dropped as duplicates. Devices that haven't sent anything for an hour are forgotten, so memory depends on how many devices are currently active, not on the total number of messages. Tracking is shared across connections, so a message resent on a new connection is still detected.

## Backpressure

Destination queues are bounded. When a queue is full, the connection handler waits on the channel before reading more, which applies backpressure to the TCP connection. Messages aren't dropped, and the in-memory queue can't grow without bound.

## Assumptions and limitations

- Duplicate tracking is in memory only, so it is lost on restart.
- Duplicates are only detected within the 64-counter window and the one-hour idle expiry. A resend that arrives outside them is accepted again. If a device resets its counter (e.g. after a reboot), its new messages can be dropped as duplicates until the counter moves past the window.
- Destinations are in-memory. Queued messages are not durable and are lost on shutdown.
- Unknown message types are treated as valid frames and rejected at routing, not treated as corrupt data.
- `MaxPayloadLength` is an implementation safety limit, not part of the protocol. It stops a corrupt length field from making the service buffer up to 64 KB or wait for data that never arrives.
- The protocol has no checksum. Garbage that happens to look like a valid header can be parsed as a frame, or make the parser treat the following real frames as its payload.
- Backpressure is per destination and shared by every device using it. A slow consumer, or a device flooding its connection, slows down all devices sending that type of message.
- Rate limiting, idle-connection timeouts, a connection limit and metrics were left out on purpose to keep the scope small. A silent or flooding client can hold a connection open indefinitely.
- ACKs are not sent, as allowed by the assignment.
