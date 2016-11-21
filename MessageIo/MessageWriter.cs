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

        public virtual Task<int> WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return WriteMessageAsync(buffer.Skip(offset).Take(count).ToArray(), cancellationToken)
                .ContinueWith(_ => count, cancellationToken);
        }

        public virtual int Write(byte[] buffer, int offset, int count)
        {
            WriteMessage(buffer.Skip(offset).Take(count).ToArray());
            return count;
        }

        public virtual async Task WriteMessageAsync(byte[] message, CancellationToken cancellationToken)
        {
            try
            {
                await _semaphore.WaitAsync(cancellationToken);
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
            try
            {
                _semaphore.Wait();

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
            byte[] bytes;
            switch (_lps)
            {
                case LengthPrefixStyle.Int8:
                    bytes = BitConverter.GetBytes((sbyte) length);
                    return _stream.WriteAsync(bytes, 0, 1, cancellationToken);
                case LengthPrefixStyle.Int16:
                    return _codec.WriteAsync(_stream, (short)length);
                case LengthPrefixStyle.Int32:
                    return _codec.WriteAsync(_stream, length);
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
                    _codec.Write(_stream, length);
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
