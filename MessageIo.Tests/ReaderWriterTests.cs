using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MessageIo.Tests
{
    [TestFixture]
    public class ReaderWriterTests
    {
        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
        public void TestReadWrite(LengthPrefixStyle lps)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageWriter(stream, true, lps))
                using (var reader = new MessageReader(stream, true, lps))
                {
                    SubtestReadWrite(writer, reader, lps, () => stream?.Seek(0, SeekOrigin.Begin));
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

                Assert.That(msg2, Is.EqualTo(msg));
            }
        }

        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
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

                Assert.That(msg2, Is.EqualTo(msg));
            }
        }

        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
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

                Assert.That(msg2, Is.EqualTo(msg));
            }
        }

        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
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

                Assert.That(msg2, Is.EqualTo(msg));
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
