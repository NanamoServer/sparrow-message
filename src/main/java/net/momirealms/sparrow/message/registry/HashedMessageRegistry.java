package net.momirealms.sparrow.message.registry;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.RedisMessage;
import net.momirealms.sparrow.message.codec.MessageCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class HashedMessageRegistry implements RedisMessageRegistry {
    private static final int FNV_OFFSET_BASIS = 0x811C9DC5;
    private static final int FNV_PRIME = 0x01000193;
    private static final int SLOT_MULTIPLIER = 0x9E3779B9; // 2^32 除以黄金分割比, 乘上它再取高位能把类型 ID 打散到各个槽位
    private static final int INITIAL_CAPACITY = 16;

    private final Map<MessageIdentifier, RegisteredRedisMessage<RedisMessage>> byKey = new HashMap<>();
    // 按类型 ID 查找用的开放寻址表. 长度是 2 的幂, 至少空着一半, 查找总能停在空槽上
    private RegisteredRedisMessage<RedisMessage>[] table = newTable(INITIAL_CAPACITY);
    private int shift = Integer.numberOfLeadingZeros(INITIAL_CAPACITY) + 1; // 把乘积右移这么多位, 剩下的高位就是槽位下标

    /**
     * 注册一种消息, 类型 ID 取消息标识的 hash.
     *
     * @param id 消息标识
     * @param codec 这种消息的编解码器
     * @throws IllegalStateException 当这个标识已经注册过, 或者它的类型 ID 和另一种已注册的消息相同时. 后一种情况给其中一种消息换个标识即可
     */
    @Override
    @SuppressWarnings("unchecked")
    public <M extends RedisMessage> void register(@NotNull MessageIdentifier id, @NotNull MessageCodec<M> codec) {
        int typeId = typeId(id);
        RegisteredRedisMessage<RedisMessage> collided = this.byId(typeId);
        if (collided != null) {
            // 查到的是同一个标识说明重复注册了, 否则是两个不同的标识算出了同一个类型 ID
            if (collided.key().equals(id)) {
                throw new IllegalStateException("Message already registered: " + id);
            }
            throw new IllegalStateException(
                    "Type ID collision: message \"" + id + "\" and already registered message \"" + collided.key() + "\" both hash to 0x" + Integer.toHexString(typeId) + ". Rename one of them on every server."
            );
        }
        // 注册表按 RedisMessage 统一存放, 取用方靠消息自己的标识或消息头的类型 ID 保证拿到的 codec 类型匹配
        RegisteredRedisMessage<RedisMessage> registered = new RegisteredRedisMessage<>(typeId, id, (MessageCodec<RedisMessage>) codec);
        this.byKey.put(id, registered);
        if (this.byKey.size() * 2 > this.table.length) {
            this.grow();
        }
        this.insert(registered);
    }

    @Nullable
    @Override
    public RegisteredRedisMessage<RedisMessage> byId(@NotNull MessageIdentifier id) {
        return this.byKey.get(id);
    }

    @Nullable
    @Override
    public RegisteredRedisMessage<RedisMessage> byId(int id) {
        RegisteredRedisMessage<RedisMessage>[] table = this.table;
        int mask = table.length - 1;
        int index = (id * SLOT_MULTIPLIER) >>> this.shift;
        // 从算出的槽位往后找, 遇到空槽说明没有注册
        RegisteredRedisMessage<RedisMessage> registered;
        while ((registered = table[index]) != null) {
            if (registered.id() == id) return registered;
            index = (index + 1) & mask;
        }
        return null;
    }

    @Override
    public boolean contains(@NotNull MessageIdentifier id) {
        return this.byKey.containsKey(id);
    }

    @Override
    public void writeTypeId(@NotNull ByteBuf buffer, int id) {
        buffer.writeInt(id);
    }

    @Override
    public int readTypeId(@NotNull ByteBuf buffer) {
        return buffer.readInt();
    }

    // 表长翻倍, 把已有的记录重新放进新表
    private void grow() {
        RegisteredRedisMessage<RedisMessage>[] old = this.table;
        this.table = newTable(old.length * 2);
        this.shift--;
        for (RegisteredRedisMessage<RedisMessage> registered : old) {
            if (registered != null) this.insert(registered);
        }
    }

    private void insert(RegisteredRedisMessage<RedisMessage> registered) {
        int mask = this.table.length - 1;
        int index = (registered.id() * SLOT_MULTIPLIER) >>> this.shift;
        while (this.table[index] != null) {
            index = (index + 1) & mask;
        }
        this.table[index] = registered;
    }

    // 对 "namespace:value" 的 UTF-8 字节算 FNV-1a 32 位值. 这个算法是线上格式的一部分
    private static int typeId(MessageIdentifier id) {
        int hash = FNV_OFFSET_BASIS;
        for (byte b : id.toString().getBytes(StandardCharsets.UTF_8)) {
            hash ^= b & 0xFF;
            hash *= FNV_PRIME;
        }
        return hash;
    }

    @SuppressWarnings("unchecked")
    private static RegisteredRedisMessage<RedisMessage>[] newTable(int capacity) {
        return new RegisteredRedisMessage[capacity];
    }
}