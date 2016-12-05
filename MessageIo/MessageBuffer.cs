using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MessageIo
{
    public class MessageBuffer : IDisposable
    {
        private readonly SemaphoreSlim _lock;
        private readonly byte[] _buffer;
        private int _start;
        private int _end;
        private bool _disposed;

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
