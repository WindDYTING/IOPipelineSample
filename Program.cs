using System.Buffers;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace PipeStreamSample;

public class Program
{
    public static async Task Main(string[] args)
    {
        await Task.Run(async () =>
        {
            await using var stream = await new HttpClient().GetStreamAsync("https://raw.githubusercontent.com/dwyl/english-words/master/words.txt");
            await using var stream2 = await new HttpClient().GetStreamAsync("https://raw.githubusercontent.com/dwyl/english-words/master/words.txt");

            var tester1 = JudgeAsync(StreamReaderMethod, stream, "streamReader");
            var tester2 = JudgeAsync(PipelineSample, stream2, "pipeline");

            await Task.WhenAll(tester2, tester1);
        }).ConfigureAwait(false);
    }

    private static async Task JudgeAsync(Func<Stream, Task> action, Stream stream, string name)
    {
        var watch = Stopwatch.StartNew();
        await action(stream).ConfigureAwait(false);
        watch.Stop();
        Console.WriteLine($"name: {name}, {watch.Elapsed}");
    }

    private static async Task StreamReaderMethod(Stream stream)
    {
        using var sr = new StreamReader(stream);
        await using var file = new StreamWriter("./lowercase-words2.txt");
        while (!sr.EndOfStream)
        {
            var txt = await sr.ReadLineAsync();
            await file.WriteLineAsync(txt!.ToLower()).ConfigureAwait(false);
        }
    }

    private static async Task PipelineSample(Stream stream)
    {
        await using var fileStream = new FileStream("./lowercase-words.txt", FileMode.Create);
        await using var lowerCaseStream = new LowerCaseStream(fileStream);

        var reader = PipeReader.Create(stream);
        var writer = PipeWriter.Create(lowerCaseStream, new StreamPipeWriterOptions(minimumBufferSize: 1 << 20, leaveOpen:true));

        await ProcessMessagesAsync(reader, writer);
    }

    static async Task ProcessMessagesAsync(
        PipeReader reader,
        PipeWriter writer)
    {
        try
        {
            while (true)
            {
                ReadResult readResult = await reader.ReadAsync().ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = readResult.Buffer;

                try
                {
                    if (readResult.IsCanceled)
                    {
                        break;
                    }

                    if (TryParseLines(ref buffer, out string message))
                    {
                        FlushResult flushResult =
                            await WriteMessagesAsync(writer, message).ConfigureAwait(false);

                        if (flushResult.IsCanceled || flushResult.IsCompleted)
                        {
                            break;
                        }
                    }

                    if (readResult.IsCompleted)
                    {
                        if (!buffer.IsEmpty)
                        {
                            throw new InvalidDataException("Incomplete message.");
                        }
                        break;
                    }
                }
                finally
                {
                    reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
        }
        finally
        {
            await reader.CompleteAsync();
            await writer.CompleteAsync();
        }
    }

    static bool TryParseLines(
        ref ReadOnlySequence<byte> buffer,
        out string message)
    {
        SequencePosition? position;
        StringBuilder outputMessage = new();

        while(true)
        {
            position = buffer.PositionOf((byte)'\n');

            if (!position.HasValue)
                break;

            outputMessage.Append(Encoding.ASCII.GetString(buffer.Slice(buffer.Start, position.Value)))
                        .AppendLine();

            buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
        };

        message = outputMessage.ToString();
        return message.Length != 0;
    }

    static ValueTask<FlushResult> WriteMessagesAsync(
        PipeWriter writer,
        string message) =>
        writer.WriteAsync(Encoding.ASCII.GetBytes(message));
}


public class LowerCaseStream : Stream
{
    private readonly Stream _stream;

    public LowerCaseStream(Stream stream)
    {
        _stream = stream;
    }

    public override void Flush()
    {
        _stream.Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var array = ConvertToLower(buffer);

        return _stream.Read(array, offset, count);
    }

    private static byte[] ConvertToLower(byte[] buffer)
    {
        var array = buffer.Select(x => (char)x)
            .Select(char.ToLower)
            .Select(c => (byte)c)
            .ToArray();
        return array;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        return _stream.Seek(offset, origin);
    }

    public override void SetLength(long value)
    {
        _stream.SetLength(value);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        var array = ConvertToLower(buffer);
        _stream.Write(array, offset, count);
    }

    public override bool CanRead => _stream.CanRead;
    public override bool CanSeek => _stream.CanSeek;
    public override bool CanWrite => _stream.CanWrite;
    public override long Length => _stream.Length;

    public override long Position
    {
        get => _stream.Position;
        set => _stream.Position = value;
    }

    public override async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync();
        await base.DisposeAsync();
    }
}