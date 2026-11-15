package net.momirealms.sparrow.message.registry;

import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.RedisMessage;
import net.momirealms.sparrow.message.codec.MessageCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DefaultMessageRegistry implements RedisMessageRegistry {
    private final List<RegisteredRedisMessage<RedisMessage>> byId = new ArrayList<>();                  // 下标就是类型 ID
    private final Map<MessageIdentifier, RegisteredRedisMessage<RedisMessage>> byKey = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <M extends RedisMessage> void register(@NotNull MessageIdentifier id, @NotNull MessageCodec<M> codec) {
        if (this.byKey.containsKey(id)) {
            throw new IllegalStateException("Message already registered: " + id);
        }
        // 注册表按 RedisMessage 统一存放, 取用方靠消息自己的标识或消息头的类型 ID 保证拿到的 codec 类型匹配
        RegisteredRedisMessage<RedisMessage> registered = new RegisteredRedisMessage<>(this.byId.size(), id, (MessageCodec<RedisMessage>) codec);
        this.byKey.put(id, registered);
        this.byId.add(registered);
    }

    @Nullable
    @Override
    public RegisteredRedisMessage<RedisMessage> byId(@NotNull MessageIdentifier id) {
        return this.byKey.get(id);
    }

    @Nullable
    @Override
    public RegisteredRedisMessage<RedisMessage> byId(int id) {
        if (id >= this.byId.size()) return null;
        return this.byId.get(id);
    }

    @Override
    public boolean contains(@NotNull MessageIdentifier id) {
        return this.byKey.containsKey(id);
    }
}