using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace MessageIo.Tests
{
    public class ReaderWriterTests
    {
        [Theory]
        [InlineData(LengthPrefixStyle.Int8)]
        [InlineData(LengthPrefixStyle.UInt8)]
        [InlineData(LengthPrefixStyle.Int16)]
        [InlineData(LengthPrefixStyle.UInt16)]
        [InlineData(LengthPrefixStyle.Int32)]
        [InlineData(LengthPrefixStyle.UInt32)]
        [InlineData(LengthPrefixStyle.Int64)]
        [InlineData(LengthPrefixStyle.UInt64)]
        [InlineData(LengthPrefixStyle.Varint)]
        [InlineData(LengthPrefixStyle.UVarint)]
        public void TestReadWrite(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    SubtestReadWrite(writer, reader, lps, () =>
                    {
                        stream?.Flush();
                        stream?.ToArray();
                        stream?.Seek(0, SeekOrigin.Begin);
                    });
                }
            }
        }

        private static void SubtestReadWrite(MessageWriter w, MessageReader r, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            msgs.ForEach(msg => w.Write(msg, 0, msg.Length));

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = new byte[msg.Length];
                r.Read(msg2, 0, msg2.Length);

                Assert.Equal(msg, msg2);
            }
        }

        [Theory]
        [InlineData(LengthPrefixStyle.Int8)]
        [InlineData(LengthPrefixStyle.UInt8)]
        [InlineData(LengthPrefixStyle.Int16)]
        [InlineData(LengthPrefixStyle.UInt16)]
        [InlineData(LengthPrefixStyle.Int32)]
        [InlineData(LengthPrefixStyle.UInt32)]
        [InlineData(LengthPrefixStyle.Int64)]
        [InlineData(LengthPrefixStyle.UInt64)]
        [InlineData(LengthPrefixStyle.Varint)]
        [InlineData(LengthPrefixStyle.UVarint)]
        public void TestReadWriteRandomLengths(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    SubtestReadWriteRandomLengths(writer, reader, lps, () => stream?.Seek(0, SeekOrigin.Begin));
                }
            }
        }

        private static void SubtestReadWriteRandomLengths(MessageWriter w, MessageReader r, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 32).Select(_ =>
            {
                var msg = new byte[GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            msgs.ForEach(msg => w.Write(msg, 0, msg.Length));

            reset();

            var expected = msgs.SelectMany(msg => msg).ToArray();

            var result = new byte[expected.Length];
            var total = 0;
            while (total < expected.Length)
            {
                var bytesRead = r.Read(result, total, rand.Next(4, 64));
                if (bytesRead < 1)
                    break;

                total += bytesRead;
            }

            Assert.Equal(expected, result);
        }

        [Theory]
        [InlineData(LengthPrefixStyle.Int8)]
        [InlineData(LengthPrefixStyle.UInt8)]
        [InlineData(LengthPrefixStyle.Int16)]
        [InlineData(LengthPrefixStyle.UInt16)]
        [InlineData(LengthPrefixStyle.Int32)]
        [InlineData(LengthPrefixStyle.UInt32)]
        [InlineData(LengthPrefixStyle.Int64)]
        [InlineData(LengthPrefixStyle.UInt64)]
        [InlineData(LengthPrefixStyle.Varint)]
        [InlineData(LengthPrefixStyle.UVarint)]
        public async Task TestReadWriteRandomLengthsAsync(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    await SubtestReadWriteRandomLengthsAsync(writer, reader, lps, () => stream?.Seek(0, SeekOrigin.Begin));
                }
            }
        }

        private static async Task SubtestReadWriteRandomLengthsAsync(MessageWriter w, MessageReader r, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 32).Select(_ =>
            {
                var msg = new byte[GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            var tasks = msgs.Select(msg => w.WriteAsync(msg, 0, msg.Length, CancellationToken.None));
            await Task.WhenAll(tasks).ConfigureAwait(false);

            reset();

            var expected = msgs.SelectMany(msg => msg).ToArray();

            var result = new byte[expected.Length];
            var total = 0;
            while (total < expected.Length)
            {
                var bytesRead = await r.ReadAsync(result, total, rand.Next(4, 64), CancellationToken.None).ConfigureAwait(false);
                if (bytesRead < 1)
                    break;

                total += bytesRead;
            }

            Assert.Equal(expected, result);
        }

        [Theory]
        [InlineData(LengthPrefixStyle.Int8)]
        [InlineData(LengthPrefixStyle.UInt8)]
        [InlineData(LengthPrefixStyle.Int16)]
        [InlineData(LengthPrefixStyle.UInt16)]
        [InlineData(LengthPrefixStyle.Int32)]
        [InlineData(LengthPrefixStyle.UInt32)]
        [InlineData(LengthPrefixStyle.Int64)]
        [InlineData(LengthPrefixStyle.UInt64)]
        [InlineData(LengthPrefixStyle.Varint)]
        [InlineData(LengthPrefixStyle.UVarint)]
        public async Task TestReadWriteAsync(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    await SubtestReadWriteAsync(writer, reader, lps, () => stream?.Seek(0, SeekOrigin.Begin));
                }
            }
        }

        private static async Task SubtestReadWriteAsync(MessageWriter w, MessageReader r, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            var tasks = msgs.Select(msg => w.WriteAsync(msg, 0, msg.Length, CancellationToken.None));
            await Task.WhenAll(tasks);

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = new byte[msg.Length];
                await r.ReadAsync(msg2, 0, msg2.Length, CancellationToken.None);

                Assert.Equal(msg, msg2);
            }
        }

        [Theory]
        [InlineData(LengthPrefixStyle.Int8)]
        [InlineData(LengthPrefixStyle.UInt8)]
        [InlineData(LengthPrefixStyle.Int16)]
        [InlineData(LengthPrefixStyle.UInt16)]
        [InlineData(LengthPrefixStyle.Int32)]
        [InlineData(LengthPrefixStyle.UInt32)]
        [InlineData(LengthPrefixStyle.Int64)]
        [InlineData(LengthPrefixStyle.UInt64)]
        [InlineData(LengthPrefixStyle.Varint)]
        [InlineData(LengthPrefixStyle.UVarint)]
        public void TestReadWriteMessage(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    SubtestReadWriteMessage(writer, reader, lps, () => stream?.Seek(0, SeekOrigin.Begin));
                }
            }
        }

        private static void SubtestReadWriteMessage(MessageWriter w, MessageReader r, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            msgs.ForEach(w.WriteMessage);

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = r.ReadMessage();

                Assert.Equal(msg, msg2);
            }
        }

        /*[Theory]
        [InlineData(LengthPrefixStyle.Int8)]
        [InlineData(LengthPrefixStyle.UInt8)]
        [InlineData(LengthPrefixStyle.Int16)]
        [InlineData(LengthPrefixStyle.UInt16)]
        [InlineData(LengthPrefixStyle.Int32)]
        [InlineData(LengthPrefixStyle.UInt32)]
        [InlineData(LengthPrefixStyle.Int64)]
        [InlineData(LengthPrefixStyle.UInt64)]
        [InlineData(LengthPrefixStyle.Varint)]
        [InlineData(LengthPrefixStyle.UVarint)]*/
        public void TestReadWriteMessageMixed(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    SubtestReadWriteMessageMixed(writer, reader, lps, () => stream?.Seek(0, SeekOrigin.Begin));
                }
            }
        }
        private static void SubtestReadWriteMessageMixed(MessageWriter w, MessageReader r, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            msgs.Take(32).ToList().ForEach(w.WriteMessage);
            msgs.Skip(32).ToList().ForEach(msg => w.Write(msg, 0, msg.Length));

            reset();

            var expected = msgs.Take(32).SelectMany(m => m).ToArray();
            var result = new byte[expected.Length];
            var total = 0;
            while (total < expected.Length)
            {
                var bytesRead = r.Read(result, total, 64);
                if (bytesRead <= 0)
                    break;

                total += bytesRead;
            }

            foreach (var msg in msgs.Skip(32))
            {
                var msg2 = r.ReadMessage();

                Assert.Equal(msg, msg2);
            }

            Assert.Equal(expected, result);
        }

        [Theory]
        [InlineData(LengthPrefixStyle.Int8)]
        [InlineData(LengthPrefixStyle.UInt8)]
        [InlineData(LengthPrefixStyle.Int16)]
        [InlineData(LengthPrefixStyle.UInt16)]
        [InlineData(LengthPrefixStyle.Int32)]
        [InlineData(LengthPrefixStyle.UInt32)]
        [InlineData(LengthPrefixStyle.Int64)]
        [InlineData(LengthPrefixStyle.UInt64)]
        [InlineData(LengthPrefixStyle.Varint)]
        [InlineData(LengthPrefixStyle.UVarint)]
        public async Task TestReadWriteMessageAsync(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    await SubtestReadWriteMessageAsync(writer, reader, lps, () => stream?.Seek(0, SeekOrigin.Begin));
                }
            }
        }

        private static async Task SubtestReadWriteMessageAsync(MessageWriter w, MessageReader r, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            var tasks = msgs.Select(msg => w.WriteMessageAsync(msg, CancellationToken.None));
            await Task.WhenAll(tasks);

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = await r.ReadMessageAsync(CancellationToken.None);

                Assert.Equal(msg, msg2);
            }
        }

        internal static int GetMaxLength(LengthPrefixStyle lps, int length)
        {
            switch (lps)
            {
                case LengthPrefixStyle.Int8:
                    return Math.Min(length, sbyte.MaxValue);
                case LengthPrefixStyle.Int16:
                    return Math.Min(length, short.MaxValue);
                case LengthPrefixStyle.Int32:
                case LengthPrefixStyle.Int64:
                case LengthPrefixStyle.Varint:
                    return Math.Min(length, int.MaxValue);
                case LengthPrefixStyle.UInt8:
                    return Math.Min(length, byte.MaxValue);
                case LengthPrefixStyle.UInt16:
                    return Math.Min(length, ushort.MaxValue);
                case LengthPrefixStyle.UInt32:
                case LengthPrefixStyle.UInt64:
                case LengthPrefixStyle.UVarint:
                    return Math.Min(length, int.MaxValue);
                default:
                    throw new ArgumentOutOfRangeException(nameof(lps), lps, null);
            }
        }
    }
}
