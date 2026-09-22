using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using static TelemetryIngestion.Protocol.ProtocolConstants;

namespace TelemetryIngestion.Protocol;

// One per connection, not thread-safe. Only checks frame structure; message types are checked when routing.
public sealed class FrameParser
{
    private static ReadOnlySpan<byte> SyncWordBytes => new byte[] { SyncByte1, SyncByte2 };

    private readonly int _maxPayloadLength;

    // Unread bytes are _buffer[_start.._end).
    private byte[] _buffer;
    private int _start;
    private int _end;

    public FrameParser(int maxPayloadLength = DefaultMaxPayloadLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxPayloadLength);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxPayloadLength, (int)ushort.MaxValue);

        _maxPayloadLength = maxPayloadLength;
        _buffer = new byte[HeaderSize + maxPayloadLength];
    }

    public int BufferedByteCount => _end - _start;

    public long DiscardedByteCount { get; private set; }

    // Callers drain TryReadFrame after each Append, which keeps the buffer to one partial frame.
    public void Append(ReadOnlySpan<byte> data)
    {
        EnsureFreeSpace(data.Length);
        data.CopyTo(_buffer.AsSpan(_end));
        _end += data.Length;
    }

    public bool TryReadFrame([NotNullWhen(true)] out Frame? frame)
    {
        frame = null;

        while (true)
        {
            if (!SkipToSyncWord())
                return false;

            if (BufferedByteCount < HeaderSize)
                return false;

            var header = _buffer.AsSpan(_start, HeaderSize);
            int payloadLength = BinaryPrimitives.ReadUInt16BigEndian(header[PayloadLengthOffset..]);

            if (payloadLength > _maxPayloadLength)
            {
                // Likely a false sync word. Skip one byte only: a real frame may start inside this header.
                Discard(1);
                continue;
            }

            int frameLength = HeaderSize + payloadLength;
            if (BufferedByteCount < frameLength)
                return false;

            frame = new Frame(
                deviceId: BinaryPrimitives.ReadUInt32BigEndian(header[DeviceIdOffset..]),
                messageCounter: BinaryPrimitives.ReadUInt16BigEndian(header[MessageCounterOffset..]),
                messageType: header[MessageTypeOffset],
                payload: _buffer.AsSpan(_start + HeaderSize, payloadLength).ToArray()); // copied: the buffer is reused

            _start += frameLength;
            return true;
        }
    }

    private bool SkipToSyncWord()
    {
        var unread = _buffer.AsSpan(_start, BufferedByteCount);

        int index = unread.IndexOf(SyncWordBytes);
        if (index >= 0)
        {
            Discard(index);
            return true;
        }

        // Keep a trailing 0xAA: the next append may complete the sync word.
        int keep = unread.Length > 0 && unread[^1] == SyncByte1 ? 1 : 0;
        Discard(unread.Length - keep);
        return false;
    }

    private void Discard(int count)
    {
        _start += count;
        DiscardedByteCount += count;
    }

    private void EnsureFreeSpace(int count)
    {
        if (_buffer.Length - _end >= count)
            return;

        int buffered = BufferedByteCount;
        _buffer.AsSpan(_start, buffered).CopyTo(_buffer);
        _start = 0;
        _end = buffered;

        if (_buffer.Length - _end < count)
            Array.Resize(ref _buffer, Math.Max(_buffer.Length * 2, buffered + count));
    }
}
