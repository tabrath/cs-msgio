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
    public class MessageWriter : IDisposable
    {
        protected readonly Stream _stream;
        protected readonly bool _leaveOpen;
        protected readonly LengthPrefixStyle _lps;
        protected readonly Endianess _endianess;
        protected readonly Binary.EndianCodec _codec;
        protected readonly SemaphoreSlim _semaphore;

        public MessageWriter(Stream stream, bool leaveOpen = false, LengthPrefixStyle lps = LengthPrefixStyle.Varint,
            Endianess endianess = Endianess.Big)
        {
            _stream = stream;
            _leaveOpen = leaveOpen;
            _lps = lps;
            _endianess = endianess;
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

        public virtual void WriteByte(int b)
        {
            WriteMessage(new [] { (byte)b });
        }

        public virtual async Task<int> WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var max = MaxMessageLength;
            if (max == 0 || count <= max)
            {
                await WriteMessageAsync(buffer.Slice(offset, count), cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var missing = count;
                while (missing > 0)
                {
                    var msg = buffer.Slice(offset, Math.Min(max, missing));
                    await WriteMessageAsync(msg, cancellationToken).ConfigureAwait(false);
                    missing -= msg.Length;
                    offset += msg.Length;
                }
            }
            return count;
        }

        public virtual int Write(byte[] buffer, int offset, int count)
        {
            var max = MaxMessageLength;
            if (max == 0 || count <= max)
            {
                WriteMessage(buffer.Slice(offset, count));
            }
            else
            {
                var missing = count;
                while (missing > 0)
                {
                    var msg = buffer.Slice(offset, Math.Min(max, missing));
                    WriteMessage(msg);
                    missing -= msg.Length;
                    offset += msg.Length;
                }
            }
            return count;
        }

        private int MaxMessageLength
        {
            get
            {
                switch (_lps)
                {
                    case LengthPrefixStyle.Int8:
                        return sbyte.MaxValue;
                    case LengthPrefixStyle.Int16:
                        return short.MaxValue;
                    case LengthPrefixStyle.Int32:
                        return int.MaxValue;
                    case LengthPrefixStyle.Int64:
                        return int.MaxValue;
                    case LengthPrefixStyle.UInt8:
                        return byte.MaxValue;
                    case LengthPrefixStyle.UInt16:
                        return ushort.MaxValue;
                    case LengthPrefixStyle.UInt32:
                        return int.MaxValue;
                    case LengthPrefixStyle.UInt64:
                        return int.MaxValue;
                    default:
                        return 0;
                }
            }
        }

        public virtual async Task WriteMessageAsync(byte[] message, CancellationToken cancellationToken)
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                await WriteMessageLengthAsync(message.Length, cancellationToken);
                await _stream.WriteAsync(message, 0, message.Length, cancellationToken);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public virtual void WriteMessage(byte[] message)
        {
            _semaphore.Wait();
            try
            {
                WriteMessageLength(message.Length);
                _stream.Write(message, 0, message.Length);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        protected Task WriteMessageLengthAsync(int length, CancellationToken cancellationToken)
        {
            if (MaxMessageLength > 0 && length > MaxMessageLength)
                throw new Exception("Length exceeds maximum length prefix size.");

            byte[] bytes;
            switch (_lps)
            {
                case LengthPrefixStyle.Int8:
                    bytes = BitConverter.GetBytes((sbyte) length);
                    return _stream.WriteAsync(bytes, 0, 1, cancellationToken);
                case LengthPrefixStyle.Int16:
                    return _codec.WriteAsync(_stream, (short)length);
                case LengthPrefixStyle.Int32:
                    return _codec.WriteAsync(_stream, (int)length);
                case LengthPrefixStyle.Int64:
                    return _codec.WriteAsync(_stream, (long)length);
                case LengthPrefixStyle.Varint:
                    bytes = Binary.Varint.GetBytes((long)length);
                    return _stream.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
                case LengthPrefixStyle.UInt8:
                    bytes = BitConverter.GetBytes((byte)length);
                    return _stream.WriteAsync(bytes, 0, 1, cancellationToken);
                case LengthPrefixStyle.UInt16:
                    return _codec.WriteAsync(_stream, (ushort)length);
                case LengthPrefixStyle.UInt32:
                    return _codec.WriteAsync(_stream, (uint)length);
                case LengthPrefixStyle.UInt64:
                    return _codec.WriteAsync(_stream, (ulong)length);
                case LengthPrefixStyle.UVarint:
                    bytes = Binary.Varint.GetBytes((ulong)length);
                    return _stream.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        protected void WriteMessageLength(int length)
        {
            if (MaxMessageLength > 0 && length > MaxMessageLength)
                throw new Exception("Length exceeds maximum length prefix size.");

            byte[] bytes;
            switch (_lps)
            {
                case LengthPrefixStyle.Int8:
                    bytes = new[] {Convert.ToByte((sbyte)length)};
                    _stream.Write(bytes, 0, bytes.Length);
                    break;
                case LengthPrefixStyle.Int16:
                    _codec.Write(_stream, (short) length);
                    break;
                case LengthPrefixStyle.Int32:
                    _codec.Write(_stream, (int)length);
                    break;
                case LengthPrefixStyle.Int64:
                    _codec.Write(_stream, (long) length);
                    break;
                case LengthPrefixStyle.Varint:
                    Binary.Varint.Write(_stream, (long) length);
                    break;
                case LengthPrefixStyle.UInt8:
                    bytes = new[] { (byte)length };
                    _stream.Write(bytes, 0, bytes.Length);
                    break;
                case LengthPrefixStyle.UInt16:
                    _codec.Write(_stream, (ushort) length);
                    break;
                case LengthPrefixStyle.UInt32:
                    _codec.Write(_stream, (uint) length);
                    break;
                case LengthPrefixStyle.UInt64:
                    _codec.Write(_stream, (ulong) length);
                    break;
                case LengthPrefixStyle.UVarint:
                    Binary.Varint.Write(_stream, (ulong) length);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public virtual void Flush() => _stream?.Flush();
        public virtual Task FlushAsync(CancellationToken cancellationToken) => _stream?.FlushAsync(cancellationToken);

        ~MessageWriter()
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
