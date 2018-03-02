using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace MessageIo
{
    public class MessageBuffer : IDisposable
    {
        private readonly SemaphoreSlim _lock;
        private readonly byte[] _buffer;
        private int _start;
        private int _end;
        private bool _disposed;
        private readonly List<Tuple<int, int>> _messages;

        public int Length
        {
            get
            {
                _lock.Wait();
                try
                {
                    return _end - _start;
                }
                finally
                {
                    _lock.Release();
                }
            }
        }

        public MessageBuffer(int capacity)
        {
            _lock = new SemaphoreSlim(1, 1);
            _buffer = new byte[capacity];
            _start = 0;
            _end = 0;
            _messages = new List<Tuple<int, int>>();
        }

        ~MessageBuffer()
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
            if (!_disposed)
            {
                if (disposing)
                    _lock.Dispose();

                _disposed = true;
            }
        }

        public void FillMessage(byte[] buffer)
        {
            _lock.Wait();
            try
            {
                if (_end + buffer.Length > _buffer.Length)
                    throw new Exception("Buffer is full");

                _messages.Add(Tuple.Create(_end, buffer.Length));

                Buffer.BlockCopy(buffer, 0, _buffer, _end, buffer.Length);
                _end += buffer.Length;
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Fill(byte[] buffer, int offset, int count)
        {
            _lock.Wait();
            try
            {
                if (_end + count > _buffer.Length)
                    throw new Exception("Buffer is full");

                Buffer.BlockCopy(buffer, offset, _buffer, _end, count);
                _end += count;
            }
            finally
            {
                _lock.Release();
            }
        }

        public byte[] DrainMessage()
        {
            _lock.Wait();
            try
            {
                if (_messages.Count == 0)
                    return Array.Empty<byte>();

                var prop = _messages.First();
                _messages.Remove(prop);

                var msg = new byte[prop.Item2];
                Buffer.BlockCopy(_buffer, prop.Item1, msg, 0, prop.Item2);
                _start += msg.Length;

                if (_start == _end)
                {
                    // empty
                    _start = 0;
                    _end = 0;
                }

                return msg;
            }
            finally
            {
                _lock.Release();
            }
        }

        public int Drain(byte[] buffer, int offset, int count)
        {
            if (count == 0)
                return 0;

            _lock.Wait();
            try
            {
                if (_start == _end)
                    return 0;

                count = Math.Min(count, _end - _start);
                Buffer.BlockCopy(_buffer, _start, buffer, offset, count);

                var msg = _messages.FirstOrDefault();
                if (msg != null)
                {
                    if (msg.Item1 <= _start)
                    {
                        _messages.Remove(msg);

                        if (msg.Item2 < count)
                        {
                            _messages.Add(Tuple.Create(_start + count, (_start + count) - msg.Item2));
                        }
                    }
                }

                _start += count;
                if (_start == _end)
                {
                    // empty
                    _start = 0;
                    _end = 0;
                }

                return count;
            }
            finally
            {
                _lock.Release();
            }
        }
    }
}
