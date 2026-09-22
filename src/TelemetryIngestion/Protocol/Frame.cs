namespace TelemetryIngestion.Protocol;

public sealed class Frame
{
    public Frame(uint deviceId, ushort messageCounter, byte messageType, byte[] payload)
    {
        DeviceId = deviceId;
        MessageCounter = messageCounter;
        MessageType = messageType;
        Payload = payload;
    }

    public uint DeviceId { get; }

    public ushort MessageCounter { get; }

    public byte MessageType { get; }

    public byte[] Payload { get; }
}
