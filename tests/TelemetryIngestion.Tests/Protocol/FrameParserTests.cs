using TelemetryIngestion.Protocol;

namespace TelemetryIngestion.Tests.Protocol;

public class FrameParserTests
{
    [Fact]
    public void Parses_big_endian_fields_from_raw_bytes()
    {
        byte[] bytes =
        [
            0xAA, 0x55,             // sync word
            0x01, 0x02, 0x03, 0x04, // device id
            0x05, 0x06,             // message counter
            0x0B,                   // message type 11
            0x00, 0x03,             // payload length
            0x09, 0x08, 0x07,       // payload
        ];
        var parser = new FrameParser();

        parser.Append(bytes);

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal(0x01020304u, frame.DeviceId);
        Assert.Equal((ushort)0x0506, frame.MessageCounter);
        Assert.Equal((byte)11, frame.MessageType);
        Assert.Equal(new byte[] { 0x09, 0x08, 0x07 }, frame.Payload);
        Assert.Equal(0, parser.BufferedByteCount);
        Assert.Equal(0, parser.DiscardedByteCount);
    }

    [Fact]
    public void Returns_false_when_nothing_is_buffered()
    {
        var parser = new FrameParser();

        Assert.False(parser.TryReadFrame(out var frame));
        Assert.Null(frame);
    }

    [Fact]
    public void Waits_for_the_rest_of_a_frame_fed_one_byte_at_a_time()
    {
        var bytes = TestFrames.Build(deviceId: 7, counter: 1, type: 2, payload: [1, 2, 3, 4]);
        var parser = new FrameParser();

        for (int i = 0; i < bytes.Length - 1; i++)
        {
            parser.Append(bytes.AsSpan(i, 1));
            Assert.False(parser.TryReadFrame(out _));
        }

        parser.Append(bytes.AsSpan(bytes.Length - 1, 1));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, frame.Payload);
    }

    [Fact]
    public void Reads_multiple_frames_from_a_single_append()
    {
        var parser = new FrameParser();

        parser.Append(TestFrames.Concat(
            TestFrames.Build(deviceId: 1, counter: 1, type: 2),
            TestFrames.Build(deviceId: 1, counter: 2, type: 3),
            TestFrames.Build(deviceId: 2, counter: 1, type: 11)));

        var frames = ReadAll(parser);

        Assert.Equal(new[] { (1u, (ushort)1), (1u, (ushort)2), (2u, (ushort)1) },
            frames.Select(f => (f.DeviceId, f.MessageCounter)));
        Assert.Equal(0, parser.BufferedByteCount);
    }

    [Fact]
    public void Discards_garbage_before_a_frame()
    {
        byte[] garbage = [0x00, 0x13, 0xAA, 0x37, 0x55];
        var parser = new FrameParser();

        parser.Append(TestFrames.Concat(garbage, TestFrames.Build(deviceId: 9, counter: 4, type: 12)));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal(9u, frame.DeviceId);
        Assert.Equal(garbage.Length, parser.DiscardedByteCount);
    }

    [Fact]
    public void Discards_garbage_between_frames()
    {
        var parser = new FrameParser();

        parser.Append(TestFrames.Concat(
            TestFrames.Build(deviceId: 1, counter: 1, type: 2),
            [0xFF, 0xFE, 0x00],
            TestFrames.Build(deviceId: 1, counter: 2, type: 2)));

        var frames = ReadAll(parser);

        Assert.Equal(new ushort[] { 1, 2 }, frames.Select(f => f.MessageCounter));
        Assert.Equal(3, parser.DiscardedByteCount);
    }

    [Fact]
    public void Keeps_a_sync_word_split_across_two_appends()
    {
        var bytes = TestFrames.Build(deviceId: 5, counter: 5, type: 13);
        var parser = new FrameParser();

        parser.Append([0x01, 0x02, 0x03, bytes[0]]);
        Assert.False(parser.TryReadFrame(out _));
        Assert.Equal(1, parser.BufferedByteCount);

        parser.Append(bytes.AsSpan(1));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal(5u, frame.DeviceId);
        Assert.Equal(3, parser.DiscardedByteCount);
    }

    [Fact]
    public void Parses_frames_with_an_unsupported_message_type()
    {
        var parser = new FrameParser();

        parser.Append(TestFrames.Build(deviceId: 1, counter: 1, type: 200, payload: [42]));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal((byte)200, frame.MessageType);
        Assert.Equal(0, parser.DiscardedByteCount);
    }

    [Fact]
    public void Treats_a_header_with_an_oversized_length_as_garbage_and_resyncs()
    {
        var oversizedHeader = TestFrames.Header(deviceId: 1, counter: 1, type: 2, payloadLength: 101);
        var parser = new FrameParser(maxPayloadLength: 100);

        parser.Append(TestFrames.Concat(oversizedHeader, TestFrames.Build(deviceId: 2, counter: 1, type: 2)));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal(2u, frame.DeviceId);
        Assert.Equal(oversizedHeader.Length, parser.DiscardedByteCount);
    }

    [Fact]
    public void Resyncs_to_a_real_frame_that_starts_inside_a_false_header()
    {
        // Read from the stray AA 55, the length field is the real frame's counter
        // low byte and type (0x1002), which is over the limit.
        var real = TestFrames.Build(deviceId: 0x01020304, counter: 0x0010, type: 2);
        var parser = new FrameParser();

        parser.Append(TestFrames.Concat([0xAA, 0x55], real));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal(0x01020304u, frame.DeviceId);
        Assert.Equal(2, parser.DiscardedByteCount);
    }

    [Fact]
    public void Parses_a_frame_with_an_empty_payload()
    {
        var parser = new FrameParser();

        parser.Append(TestFrames.Build(deviceId: 1, counter: 1, type: 1, payload: []));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Empty(frame.Payload);
    }

    [Fact]
    public void Parses_a_frame_with_the_maximum_payload_length()
    {
        var payload = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();
        var parser = new FrameParser(maxPayloadLength: 100);

        parser.Append(TestFrames.Build(deviceId: 1, counter: 1, type: 2, payload: payload));

        Assert.True(parser.TryReadFrame(out var frame));
        Assert.Equal(payload, frame.Payload);
    }

    [Fact]
    public void Keeps_a_truncated_frame_buffered()
    {
        var bytes = TestFrames.Build(deviceId: 1, counter: 1, type: 2, payload: [1, 2, 3]);
        var parser = new FrameParser();

        parser.Append(bytes.AsSpan(0, bytes.Length - 1));

        Assert.False(parser.TryReadFrame(out _));
        Assert.Equal(bytes.Length - 1, parser.BufferedByteCount);
        Assert.Equal(0, parser.DiscardedByteCount);
    }

    [Fact]
    public void Does_not_buffer_a_stream_of_pure_garbage()
    {
        var parser = new FrameParser();

        for (int i = 0; i < 100; i++)
        {
            parser.Append(new byte[4096]);
            Assert.False(parser.TryReadFrame(out _));
        }

        Assert.Equal(0, parser.BufferedByteCount);
        Assert.Equal(100 * 4096, parser.DiscardedByteCount);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(4096)]
    public void Reads_a_long_stream_in_order_regardless_of_chunk_size(int chunkSize)
    {
        var expected = Enumerable.Range(0, 300)
            .Select(i => TestFrames.Build(
                deviceId: (uint)(i % 5),
                counter: (ushort)i,
                type: 2,
                payload: Enumerable.Repeat((byte)i, i % 50).ToArray()))
            .ToArray();
        var stream = TestFrames.Concat(expected);
        var parser = new FrameParser();
        var frames = new List<Frame>();

        for (int offset = 0; offset < stream.Length; offset += chunkSize)
        {
            parser.Append(stream.AsSpan(offset, Math.Min(chunkSize, stream.Length - offset)));
            frames.AddRange(ReadAll(parser));
        }

        // Checked after all appends, so buffer reuse would corrupt earlier payloads.
        Assert.Equal(300, frames.Count);
        for (int i = 0; i < frames.Count; i++)
        {
            Assert.Equal((ushort)i, frames[i].MessageCounter);
            Assert.Equal(Enumerable.Repeat((byte)i, i % 50), frames[i].Payload);
        }
        Assert.Equal(0, parser.BufferedByteCount);
        Assert.Equal(0, parser.DiscardedByteCount);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(65_536)]
    public void Rejects_a_max_payload_length_the_protocol_cannot_express(int maxPayloadLength)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FrameParser(maxPayloadLength));
    }

    private static List<Frame> ReadAll(FrameParser parser)
    {
        var frames = new List<Frame>();
        while (parser.TryReadFrame(out var frame))
            frames.Add(frame);
        return frames;
    }
}
