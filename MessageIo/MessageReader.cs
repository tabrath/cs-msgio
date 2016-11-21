using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BinaryEncoding;

namespace MessageIo
{
    public class MessageReader : IDisposable
    {
        protected readonly Stream _stream;
        protected readonly bool _leaveOpen;
        protected readonly LengthPrefixStyle _lps;
        protected readonly Endianess _endianess;
        protected int _next;
        protected readonly Binary.EndianCodec _codec;
        protected readonly SemaphoreSlim _semaphore;

        public MessageReader(Stream stream, bool leaveOpen = false, LengthPrefixStyle lps = LengthPrefixStyle.Varint,
            Endianess endianess = Endianess.Big)
        {
            _stream = stream;
            _leaveOpen = leaveOpen;
            _lps = lps;
            _endianess = endianess;
            _next = -1;
            _semaphore = new SemaphoreSlim(1, 1);
            _codec = (_lps != LengthPrefixStyle.UVarint && _lps != LengthPrefixStyle.Varint) ? GetCodec() : null;
        }

        private Binary.EndianCodec GetCodec()
        {
            switch (_endianess)
            {
                case Endianess.Big:
                    return Binary.BigEndian;
                case Endianess.Little:
                    return Binary.LittleEndian;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public async Task<int> ReadNextMessageLengthAsync(CancellationToken cancellationToken)
        {
            int length = 0;
            try
            {
                await _semaphore.WaitAsync(cancellationToken);
                length = await NextMessageLengthAsync(cancellationToken);
            }
            finally
            {
                _semaphore.Release();
            }
            return length;
        }

        public int ReadNextMessageLength()
        {
            var length = 0;
            try
            {
                _semaphore.Wait();
                length = NextMessageLength();
            }
            finally
            {
                _semaphore.Release();
            }
            return length;
        }

        private async Task<int> NextMessageLengthAsync(CancellationToken cancellationToken)
        {
            if (_next != -1)
                return _next;

            byte[] bytes = new byte[1];
            switch (_lps)
            {
                case LengthPrefixStyle.Int8:
                    await _stream.ReadAsync(bytes, 0, bytes.Length, cancellationToken);
                    return _next = (sbyte)bytes[0];
                case LengthPrefixStyle.Int16:
                    return _next = await _codec.ReadInt16Async(_stream);
                case LengthPrefixStyle.Int32:
                    return _next = await _codec.ReadInt32Async(_stream);
                case LengthPrefixStyle.Int64:
                    return _next = (int) await _codec.ReadInt64Async(_stream);
                case LengthPrefixStyle.Varint:
                    return _next = (int) await Binary.Varint.ReadInt64Async(_stream);
                case LengthPrefixStyle.UInt8:
                    await _stream.ReadAsync(bytes, 0, bytes.Length, cancellationToken);
                    return _next = bytes[0];
                case LengthPrefixStyle.UInt16:
                    return _next = await _codec.ReadUInt16Async(_stream);
                case LengthPrefixStyle.UInt32:
                    return _next = (int) await _codec.ReadUInt32Async(_stream);
                case LengthPrefixStyle.UInt64:
                    return _next = (int) await _codec.ReadUInt64Async(_stream);
                case LengthPrefixStyle.UVarint:
                    return _next = (int) await Binary.Varint.ReadUInt64Async(_stream);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        protected int NextMessageLength()
        {
            if (_next != -1)
                return _next;

            var bytes = new byte[1];
            switch (_lps)
            {
                case LengthPrefixStyle.Int8:
                    _stream.Read(bytes, 0, 1);
                    return _next = (sbyte) bytes[0];
                case LengthPrefixStyle.Int16:
                    return _next = _codec.ReadInt16(_stream);
                case LengthPrefixStyle.Int32:
                    return _next = _codec.ReadInt32(_stream);
                case LengthPrefixStyle.Int64:
                    return _next = (int) _codec.ReadInt64(_stream);
                case LengthPrefixStyle.Varint:
                    long proxy = 0;
                    Binary.Varint.Read(_stream, out proxy);
                    return _next = (int) proxy;
                case LengthPrefixStyle.UInt8:
                    _stream.Read(bytes, 0, 1);
                    return _next = bytes[0];
                case LengthPrefixStyle.UInt16:
                    return _next = _codec.ReadUInt16(_stream);
                case LengthPrefixStyle.UInt32:
                    return _next = (int) _codec.ReadUInt32(_stream);
                case LengthPrefixStyle.UInt64:
                    return _next = (int) _codec.ReadUInt64(_stream);
                case LengthPrefixStyle.UVarint:
                    ulong uproxy = 0;
                    Binary.Varint.Read(_stream, out uproxy);
                    return _next = (int) uproxy;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public virtual async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int result = 0;
            try
            {
                await _semaphore.WaitAsync(cancellationToken);

                var length = await NextMessageLengthAsync(cancellationToken);

                if (length > count - offset)
                    throw new Exception("Buffer is too short");

                result = await _stream.ReadAsync(buffer, offset, length, cancellationToken);

                _next = -1;
            }
            finally
            {
                _semaphore.Release();
            }
            return result;
        }

        public virtual int Read(byte[] buffer, int offset, int count)
        {
            var length = 0;
            try
            {
                _semaphore.Wait();
                length = NextMessageLength();

                if (length > count - offset)
                    throw new Exception("Buffer is too short");

                if (_stream.Read(buffer, offset, length) != length)
                    throw new Exception("Could not read entire message");

                _next = -1;
            }
            finally
            {
                _semaphore.Release();
            }
            return length;
        }

        public virtual async Task<byte[]> ReadMessageAsync(CancellationToken cancellationToken)
        {
            byte[] buffer = null;
            try
            {
                await _semaphore.WaitAsync(cancellationToken);

                var length = await NextMessageLengthAsync(cancellationToken);
                buffer = new byte[length];
                var offset = 0;
                while (offset < buffer.Length)
                {
                    var bytesRead = await _stream.ReadAsync(buffer, offset, buffer.Length - offset, cancellationToken);
                    if (bytesRead == 0)
                        throw new EndOfStreamException();

                    offset += bytesRead;
                }

                _next = -1;
            }
            finally
            {
                _semaphore.Release();
            }
            return buffer;
        }

        public virtual byte[] ReadMessage()
        {
            byte[] buffer = null;
            try
            {
                _semaphore.Wait();
                var length = NextMessageLength();
                buffer = new byte[length];

                var offset = 0;
                while (offset < buffer.Length)
                {
                    var bytesRead = _stream.Read(buffer, offset, buffer.Length - offset);
                    if (bytesRead == 0)
                        throw new EndOfStreamException();

                    offset += bytesRead;

                }

                _next = -1;
            }
            finally
            {
                _semaphore.Release();
            }
            return buffer;
        }

        ~MessageReader()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            _semaphore?.Dispose();

            if (!_leaveOpen)
                _stream?.Dispose();
        }
    }
}
