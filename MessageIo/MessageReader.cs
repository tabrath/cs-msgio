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
        protected readonly MessageBuffer _buffer;

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
            _buffer = new MessageBuffer(4096);
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
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await NextMessageLengthAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public int ReadNextMessageLength()
        {
            _semaphore.Wait();
            try
            {
                return NextMessageLength();
            }
            finally
            {
                _semaphore.Release();
            }
        }

        private async Task<int> NextMessageLengthAsync(CancellationToken cancellationToken)
        {
            if (_next != -1)
                return _next;

            try
            {
                var bytes = new byte[1];
                switch (_lps)
                {
                    case LengthPrefixStyle.Int8:
                        await _stream.ReadAsync(bytes, 0, bytes.Length, cancellationToken).ConfigureAwait(false);
                        return _next = (sbyte) bytes[0];
                    case LengthPrefixStyle.Int16:
                        return _next = await _codec.ReadInt16Async(_stream).ConfigureAwait(false);
                    case LengthPrefixStyle.Int32:
                        return _next = await _codec.ReadInt32Async(_stream).ConfigureAwait(false);
                    case LengthPrefixStyle.Int64:
                        return _next = (int) await _codec.ReadInt64Async(_stream).ConfigureAwait(false);
                    case LengthPrefixStyle.Varint:
                        return _next = (int) await Binary.Varint.ReadInt64Async(_stream).ConfigureAwait(false);
                    case LengthPrefixStyle.UInt8:
                        await _stream.ReadAsync(bytes, 0, bytes.Length, cancellationToken).ConfigureAwait(false);
                        return _next = bytes[0];
                    case LengthPrefixStyle.UInt16:
                        return _next = await _codec.ReadUInt16Async(_stream).ConfigureAwait(false);
                    case LengthPrefixStyle.UInt32:
                        return _next = (int) await _codec.ReadUInt32Async(_stream).ConfigureAwait(false);
                    case LengthPrefixStyle.UInt64:
                        return _next = (int) await _codec.ReadUInt64Async(_stream).ConfigureAwait(false);
                    case LengthPrefixStyle.UVarint:
                        return _next = (int) await Binary.Varint.ReadUInt64Async(_stream).ConfigureAwait(false);
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch
            {
                return -1;
            }
        }

        protected int NextMessageLength()
        {
            if (_next != -1)
                return _next;

            try
            {
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
            catch
            {
                return -1;
            }
        }

        public virtual async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var drained = _buffer.Drain(buffer, offset, count);
            if (drained == count)
                return drained;

            var msg = await ReadNextMessageAsync(cancellationToken).ConfigureAwait(false);
            if (msg.Length == 0)
                return drained > 0 ? drained : -1;

            _buffer.Fill(msg, 0, msg.Length);
            return drained + _buffer.Drain(buffer, offset + drained, count - drained);
        }

        public virtual int Read(byte[] buffer, int offset, int count)
        {
            var drained = _buffer.Drain(buffer, offset, count);
            if (drained == count)
                return drained;

            var msg = ReadNextMessage();
            if (msg.Length == 0)
                return drained > 0 ? drained : -1;

            _buffer.Fill(msg, 0, msg.Length);
            return drained + _buffer.Drain(buffer, offset + drained, count - drained);
        }

        public virtual int ReadByte()
        {
            var buffer = new byte[1];
            if (_buffer.Drain(buffer, 0, 1) == 1)
                return buffer[0];

            var msg = ReadNextMessage();
            if (msg.Length == 0)
                return -1;

            _buffer.Fill(msg, 0, msg.Length);
            return _buffer.Drain(buffer, 0, 1);
        }

        public Task<byte[]> ReadMessageAsync(CancellationToken cancellationToken) => ReadNextMessageAsync(cancellationToken);

        protected virtual async Task<byte[]> ReadNextMessageAsync(CancellationToken cancellationToken)
        {
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var length = await NextMessageLengthAsync(cancellationToken).ConfigureAwait(false);
                if (length < 1)
                    return Array.Empty<byte>();

                byte[] buffer = new byte[length];

                var offset = 0;
                while (offset < buffer.Length)
                {
                    var bytesRead = await _stream.ReadAsync(buffer, offset, buffer.Length - offset, cancellationToken).ConfigureAwait(false);
                    if (bytesRead <= 0)
                        break;

                    offset += bytesRead;
                }

                if (offset != length)
                    throw new Exception("Unable to read whole message.");

                return buffer;
            }
            catch (EndOfStreamException)
            {
                return Array.Empty<byte>();
            }
            finally
            {
                _next = -1;
                _semaphore.Release();
            }
        }

        public byte[] ReadMessage()
        {
            var drained = _buffer.DrainMessage();
            if (drained.Length > 0)
                return drained;

            var msg = ReadNextMessage();
            if (msg.Length == 0)
                return drained;

            _buffer.FillMessage(msg);
            return drained.Concat(_buffer.DrainMessage()).ToArray();
        }
        
        protected virtual byte[] ReadNextMessage()
        {
            _semaphore.Wait();
            try
            {
                var length = NextMessageLength();
                if (length < 1)
                    return Array.Empty<byte>();

                byte[] buffer = new byte[length];

                var offset = 0;
                while (offset < buffer.Length)
                {
                    var bytesRead = _stream.Read(buffer, offset, buffer.Length - offset);
                    if (bytesRead <= 0)
                        break;

                    offset += bytesRead;
                }

                if (offset != length)
                    throw new Exception("Unable to read whole message.");

                return buffer;
            }
            catch (EndOfStreamException)
            {
                return Array.Empty<byte>();
            }
            finally
            {
                _next = -1;
                _semaphore.Release();
            }
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
            _buffer.Dispose();

            if (!_leaveOpen)
                _stream?.Dispose();
        }
    }
}
