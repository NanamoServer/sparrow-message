package net.momirealms.sparrow.message;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.message.registry.RedisMessageRegistry;
import net.momirealms.sparrow.message.util.ByteBufHelper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// 每条消息开头由库读写的部分, 后面紧跟消息体. 布局:
//   byte    标记位, bit 0-1 是消息种类, bit 2 表示带 tag, bit 3 表示排除来源
//   int     类型 ID, 错误响应没有. 由注册表读写, IDENTIFIER_HASH 下定长 4 字节, REGISTRATION_ORDER 下是 varint
//   utf8    tag, bit 2 置位时才有
//   utf8    来源服务器 ID, bit 3 置位或种类为请求时才有
//   long    关联 ID, 单向消息没有
// 定向消息靠频道路由, 消息头里没有目标服务器.
record MessageHeader(int kind, int typeId, @Nullable String tag, @Nullable String source, long correlationId) {
    static final int ONE_WAY = 0;
    static final int REQUEST = 1;
    static final int RESPONSE = 2;
    static final int ERROR = 3; // 请求在对端处理失败, 消息体是异常类名和消息文本

    private static final int KIND_MASK = 0b0011;
    private static final int FLAG_TAG = 0b0100;
    private static final int FLAG_EXCLUDE_SOURCE = 0b1000;

    /**
     * 写出单向消息的消息头.
     *
     * @param buffer 接收数据的缓冲区
     * @param registry 本 broker 的注册表, 类型 ID 由它写出
     * @param typeId 消息的类型 ID
     * @param tag 只投递给带这个 tag 的服务器, 不按 tag 筛选时为 {@code null}
     * @param excludedSource 发送方的服务器 ID, 收到消息的服务器发现是自己时丢弃, 不排除发送方时为 {@code null}
     */
    static void writeOneWay(@NotNull ByteBuf buffer, @NotNull RedisMessageRegistry registry, int typeId, @Nullable String tag, @Nullable String excludedSource) {
        int flags = ONE_WAY;
        if (tag != null) flags |= FLAG_TAG;
        if (excludedSource != null) flags |= FLAG_EXCLUDE_SOURCE;
        buffer.writeByte(flags);
        registry.writeTypeId(buffer, typeId);
        if (tag != null) ByteBufHelper.writeUtf8(buffer, tag);
        if (excludedSource != null) ByteBufHelper.writeUtf8(buffer, excludedSource);
    }

    static void writeRequest(@NotNull ByteBuf buffer, @NotNull RedisMessageRegistry registry, int typeId, @NotNull String source, long correlationId) {
        buffer.writeByte(REQUEST);
        registry.writeTypeId(buffer, typeId);
        ByteBufHelper.writeUtf8(buffer, source);
        buffer.writeLong(correlationId);
    }

    static void writeResponse(@NotNull ByteBuf buffer, @NotNull RedisMessageRegistry registry, int typeId, long correlationId) {
        buffer.writeByte(RESPONSE);
        registry.writeTypeId(buffer, typeId);
        buffer.writeLong(correlationId);
    }

    static void writeError(@NotNull ByteBuf buffer, long correlationId) {
        buffer.writeByte(ERROR);
        buffer.writeLong(correlationId);
    }

    /**
     * 读出消息头, 读完后缓冲区的读取位置正好停在消息体开头.
     * <p>这条消息没有的字段取默认值: 类型 ID 为 -1, tag 和来源为 {@code null}, 关联 ID 为 0.
     *
     * @param buffer 提供数据的缓冲区
     * @param registry 本 broker 的注册表, 类型 ID 由它读出
     * @return 读出的消息头
     */
    @NotNull
    static MessageHeader read(@NotNull ByteBuf buffer, @NotNull RedisMessageRegistry registry) {
        int flags = buffer.readByte();
        int kind = flags & KIND_MASK;
        int typeId = kind == ERROR ? -1 : registry.readTypeId(buffer);
        String tag = (flags & FLAG_TAG) != 0 ? ByteBufHelper.readUtf8(buffer) : null;
        String source = ((flags & FLAG_EXCLUDE_SOURCE) != 0 || kind == REQUEST) ? ByteBufHelper.readUtf8(buffer) : null;
        long correlationId = kind == ONE_WAY ? 0L : buffer.readLong();
        return new MessageHeader(kind, typeId, tag, source, correlationId);
    }
}