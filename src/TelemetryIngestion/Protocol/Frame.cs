namespace TelemetryIngestion.Protocol;

public sealed record Frame(uint DeviceId, ushort MessageCounter, byte MessageType, byte[] Payload);
