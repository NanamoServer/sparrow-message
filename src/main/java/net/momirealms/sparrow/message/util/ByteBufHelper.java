package net.momirealms.sparrow.message.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;

public final class ByteBufHelper {
    private static final int DEFAULT_MAX_STRING_LENGTH = 32767;

    private ByteBufHelper() {}

    /**
     * 写入一个字符串, 最多 32767 个字符.
     *
     * @param buffer 接收数据的缓冲区
     * @param string 要写入的字符串
     * @throws EncoderException 当字符串超过 32767 个字符时
     */
    public static void writeUtf8(@NotNull ByteBuf buffer, @NotNull String string) {
        writeUtf8(buffer, string, DEFAULT_MAX_STRING_LENGTH);
    }

    /**
     * 写入一个字符串, 格式是变长整数表示的 UTF-8 字节数加上 UTF-8 内容.
     *
     * @param buffer 接收数据的缓冲区
     * @param string 要写入的字符串
     * @param maxLength 允许的最大字符数
     * @throws EncoderException 当字符串超过 maxLength 个字符时
     */
    public static void writeUtf8(@NotNull ByteBuf buffer, @NotNull String string, int maxLength) {
        if (string.length() > maxLength) {
            throw new EncoderException("String length " + string.length() + " exceeds maximum allowed " + maxLength);
        }
        // 先算出准确的字节数写在前面, 再把内容直接编码进目标缓冲区
        int bytes = ByteBufUtil.utf8Bytes(string);
        writeCompactInt(buffer, bytes);
        ByteBufUtil.reserveAndWriteUtf8(buffer, string, bytes);
    }

    /**
     * 读取一个字符串, 最多 32767 个字符.
     *
     * @param buffer 提供数据的缓冲区
     * @return 读出的字符串
     * @throws DecoderException 当字符串超过 32767 个字符时
     */
    @NotNull
    public static String readUtf8(@NotNull ByteBuf buffer) {
        return readUtf8(buffer, DEFAULT_MAX_STRING_LENGTH);
    }

    /**
     * 读取一个由 {@link #writeUtf8(ByteBuf, String, int)} 写入的字符串.
     *
     * @param buffer 提供数据的缓冲区
     * @param maxLength 允许的最大字符数
     * @return 读出的字符串
     * @throws DecoderException 当声明的字节数或解码后的字符数超过 maxLength 对应的上限时
     */
    @NotNull
    public static String readUtf8(@NotNull ByteBuf buffer, int maxLength) {
        // 解码前先按字节数拦一次, 超长的数据不会被分配成字符串
        int bytes = readCompactInt(buffer);
        int maxBytes = ByteBufUtil.utf8MaxBytes(maxLength);
        if (bytes > maxBytes) {
            throw new DecoderException("Encoded string size " + bytes + " exceeds maximum allowed " + maxBytes + " bytes");
        }
        String string = buffer.toString(buffer.readerIndex(), bytes, StandardCharsets.UTF_8);
        buffer.skipBytes(bytes);
        // 一个字符最多占 3 个字节, 字节数合格的数据字符数仍可能超限
        if (string.length() > maxLength) {
            throw new DecoderException("String length " + string.length() + " exceeds maximum allowed " + maxLength);
        }
        return string;
    }

    /**
     * 写入一个字节数组, 格式是变长整数表示的长度加上数组内容.
     *
     * @param buffer 接收数据的缓冲区
     * @param bytes 要写入的字节数组
     */
    public static void writeByteArray(@NotNull ByteBuf buffer, byte @NotNull [] bytes) {
        writeCompactInt(buffer, bytes.length);
        buffer.writeBytes(bytes);
    }

    /**
     * 读取一个由 {@link #writeByteArray(ByteBuf, byte[])} 写入的字节数组.
     *
     * @param buffer 提供数据的缓冲区
     * @param maxSize 允许的最大长度
     * @return 读出的字节数组
     * @throws DecoderException 当声明的长度超过 maxSize 时
     */
    public static byte @NotNull [] readByteArray(@NotNull ByteBuf buffer, int maxSize) {
        int size = readCompactInt(buffer);
        if (size > maxSize) {
            throw new DecoderException("ByteArray with size " + size + " is bigger than allowed " + maxSize);
        }
        byte[] bytes = new byte[size];
        buffer.readBytes(bytes);
        return bytes;
    }

    /**
     * 写入一个 VarInt, 占 1 到 5 个字节, 数值越小占用越少.
     *
     * <p>负数的高位全是 1, 固定占满 5 个字节.
     *
     * @param buffer 接收数据的缓冲区
     * @param value 要写入的值
     */
    public static void writeCompactInt(@NotNull ByteBuf buffer, int value) {
        // 还有超出低 7 位的数据时, 写出低 7 位并把最高位置 1
        while ((value & -128) != 0) {
            buffer.writeByte((value & 127) | 128);
            value >>>= 7;
        }
        buffer.writeByte(value & 127);
    }

    /**
     * 读取一个由 {@link #writeCompactInt(ByteBuf, int)} 写入的 VarInt.
     *
     * @param buffer 提供数据的缓冲区
     * @return 读出的值
     * @throws DecoderException 当数据超过 5 个字节仍未结束时
     */
    public static int readCompactInt(@NotNull ByteBuf buffer) {
        int value = 0;
        int shift = 0;
        byte current;
        // 每个字节贡献 7 位数据, 最高位为 1 表示后面还有字节
        do {
            current = buffer.readByte();
            value |= (current & 127) << shift++ * 7;
            if (shift > 5) {
                throw new DecoderException("CompactInt is bigger than expected(5)");
            }
        } while ((current & 128) == 128);
        return value;
    }

    /**
     * 写入一个 VarLong, 占 1 到 10 个字节, 数值越小占用越少.
     *
     * <p>负数的高位全是 1, 固定占满 10 个字节.
     *
     * @param buffer 接收数据的缓冲区
     * @param value 要写入的值
     */
    public static void writeCompactLong(@NotNull ByteBuf buffer, long value) {
        while ((value & -128L) != 0L) {
            buffer.writeByte((int) (value & 127L) | 128);
            value >>>= 7;
        }
        buffer.writeByte((int) (value & 127L));
    }

    /**
     * 读取一个由 {@link #writeCompactLong(ByteBuf, long)} 写入的 VarLong.
     *
     * @param buffer 提供数据的缓冲区
     * @return 读出的值
     * @throws DecoderException 当数据超过 10 个字节仍未结束时
     */
    public static long readCompactLong(@NotNull ByteBuf buffer) {
        long value = 0L;
        int shift = 0;
        byte current;
        do {
            current = buffer.readByte();
            value |= (long) (current & 127) << shift++ * 7;
            if (shift > 10) {
                throw new DecoderException("CompactLong is bigger than expected(10)");
            }
        } while ((current & 128) == 128);
        return value;
    }
}