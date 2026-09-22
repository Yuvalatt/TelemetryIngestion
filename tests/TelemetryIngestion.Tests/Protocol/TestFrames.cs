using System.Buffers.Binary;
using TelemetryIngestion.Protocol;

namespace TelemetryIngestion.Tests.Protocol;

internal static class TestFrames
{
    public static byte[] Build(uint deviceId, ushort counter, byte type, byte[]? payload = null)
    {
        payload ??= [0x01, 0x02];
        return Concat(Header(deviceId, counter, type, (ushort)payload.Length), payload);
    }

    public static byte[] Header(uint deviceId, ushort counter, byte type, ushort payloadLength)
    {
        var header = new byte[ProtocolConstants.HeaderSize];
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(ProtocolConstants.SyncWordOffset), ProtocolConstants.SyncWord);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(ProtocolConstants.DeviceIdOffset), deviceId);
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(ProtocolConstants.MessageCounterOffset), counter);
        header[ProtocolConstants.MessageTypeOffset] = type;
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(ProtocolConstants.PayloadLengthOffset), payloadLength);
        return header;
    }

    public static byte[] Concat(params byte[][] parts) => parts.SelectMany(p => p).ToArray();
}
