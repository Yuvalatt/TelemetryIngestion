namespace TelemetryIngestion.Tests.Networking;

// Returns one chunk per read, then end of stream or the given exception.
internal sealed class ScriptedStream(IEnumerable<byte[]> chunks, Exception? failAtEnd = null) : Stream
{
    private readonly Queue<byte[]> _chunks = new(chunks);

    public static byte[][] Split(byte[] data, int chunkSize) => data.Chunk(chunkSize).ToArray();

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_chunks.TryDequeue(out var chunk))
        {
            chunk.CopyTo(buffer);
            return ValueTask.FromResult(chunk.Length);
        }

        return failAtEnd is null ? ValueTask.FromResult(0) : ValueTask.FromException<int>(failAtEnd);
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
