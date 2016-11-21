using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MessageIo
{
    public class MessageStream : Stream
    {
        private readonly Stream _stream;
        private readonly bool _leaveOpen;
        private readonly MessageReader _reader;
        private readonly MessageWriter _writer;

        public MessageStream(Stream stream, bool leaveOpen = false, LengthPrefixStyle lps = LengthPrefixStyle.Varint,
            Endianess endianess = Endianess.Big)
        {
            _stream = stream;
            _leaveOpen = leaveOpen;

            _reader = _stream.CanRead ? new MessageReader(_stream, leaveOpen, lps, endianess) : null;
            _writer = _stream.CanWrite ? new MessageWriter(_stream, leaveOpen, lps, endianess) : null;
        }

        protected MessageStream(MessageReader reader, MessageWriter writer)
        {
            _reader = reader;
            _writer = writer;
        }

        public static MessageStream Combine(MessageReader reader, MessageWriter writer) => new MessageStream(reader, writer);

        public override void Flush() => _writer?.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _writer.FlushAsync(cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => _stream?.Seek(offset, origin) ?? -1;
        public override void SetLength(long value) => _stream?.SetLength(value);

        public override int Read(byte[] buffer, int offset, int count) => _reader?.Read(buffer, offset, count) ?? 0;
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _reader?.ReadAsync(buffer, offset, count, cancellationToken);
        public override int ReadByte() => _stream?.ReadByte() ?? -1;
        public byte[] ReadMessage() => _reader?.ReadMessage();
        public Task<byte[]> ReadMessageAsync(CancellationToken cancellationToken)=> _reader?.ReadMessageAsync(cancellationToken);

        public override void Write(byte[] buffer, int offset, int count) => _writer?.Write(buffer, offset, count);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _writer?.WriteAsync(buffer, offset, count, cancellationToken);
        public override void WriteByte(byte value) => _stream?.WriteByte(value);
        public void WriteMessage(byte[] message) => _writer?.WriteMessage(message);
        public Task WriteMessageAsync(byte[] message, CancellationToken cancellationToken) => _writer?.WriteMessageAsync(message, cancellationToken);

        public override bool CanRead => _reader != null;
        public override bool CanSeek => _stream?.CanSeek ?? false;
        public override bool CanWrite => _writer != null;
        public override long Length => _stream?.Length ?? 0;

        public override long Position
        {
            get { return _stream?.Position ?? 0; }
            set { if (_stream != null) _stream.Position = value; }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _reader?.Dispose();
                _writer?.Dispose();

                if (!_leaveOpen)
                    _stream?.Dispose();
            }
        }
    }
}
