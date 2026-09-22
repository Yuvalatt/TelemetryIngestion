namespace TelemetryIngestion.Protocol;

// [Sync Word 2][Device Id 4][Message Counter 2][Message Type 1][Payload Length 2][Payload N], big-endian.
public static class ProtocolConstants
{
    public const ushort SyncWord = 0xAA55;
    public const byte SyncByte1 = 0xAA;
    public const byte SyncByte2 = 0x55;

    public const int SyncWordOffset = 0;
    public const int DeviceIdOffset = 2;
    public const int MessageCounterOffset = 6;
    public const int MessageTypeOffset = 8;
    public const int PayloadLengthOffset = 9;

    public const int HeaderSize = 11;

    // Low-power devices send small payloads; a larger length is treated as a corrupt header.
    public const int DefaultMaxPayloadLength = 1024;
}
