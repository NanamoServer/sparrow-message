package net.momirealms.sparrow.message.registry;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.RedisMessage;
import net.momirealms.sparrow.message.codec.MessageCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RedisMessageRegistry {

    <M extends RedisMessage> void register(@NotNull MessageIdentifier id, @NotNull MessageCodec<M> codec);

    @Nullable
    RegisteredRedisMessage<RedisMessage> byId(@NotNull MessageIdentifier id);

    @Nullable
    RegisteredRedisMessage<RedisMessage> byId(int id);

    boolean contains(@NotNull MessageIdentifier id);

    /**
     * 把类型 ID 写进消息头, 占几个字节由这个注册表分配类型 ID 的方式决定.
     *
     * @param buffer 接收数据的缓冲区
     * @param id 要写入的类型 ID
     */
    void writeTypeId(@NotNull ByteBuf buffer, int id);

    /**
     * 读出由 {@link #writeTypeId(ByteBuf, int)} 写入的类型 ID.
     *
     * @param buffer 提供数据的缓冲区
     * @return 读出的类型 ID
     */
    int readTypeId(@NotNull ByteBuf buffer);
}