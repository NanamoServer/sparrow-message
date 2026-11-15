package net.momirealms.sparrow.message.registry;

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
}