package net.momirealms.sparrow.message.connection;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public final class LettuceRedisConnection implements RedisConnection {
    private static final RedisCodec<String, byte[]> CODEC = RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE); // 频道名按字符串, 消息内容按原始字节

    private final StatefulRedisConnection<String, byte[]> publishConnection;
    private final RedisAsyncCommands<String, byte[]> publishCommands;
    private final StatefulRedisPubSubConnection<String, byte[]> subscribeConnection;
    private final RedisPubSubCommands<String, byte[]> subscribeCommands;
    private final Map<String, Consumer<byte[]>> listeners = new ConcurrentHashMap<>(); // 频道名 -> 监听器, I/O 线程读, 订阅方写

    /**
     * 在给定的客户端上打开一条发布连接和一条订阅连接, 同一个应用的多个 broker 共用这一个实例.
     * <p>客户端归调用方所有, {@link #close()} 只关闭这里打开的两条连接.
     *
     * @param client 已配置好的 Lettuce 客户端
     */
    public LettuceRedisConnection(@NotNull RedisClient client) {
        this.publishConnection = client.connect(CODEC);
        this.publishCommands = this.publishConnection.async();
        this.subscribeConnection = client.connectPubSub(CODEC);
        this.subscribeCommands = this.subscribeConnection.sync();
        this.subscribeConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, byte[] message) {
                Consumer<byte[]> listener = LettuceRedisConnection.this.listeners.get(channel);
                // 退订命令发出后, 已经在路上的消息仍可能到达
                if (listener != null) listener.accept(message);
            }
        });
    }

    @NotNull
    @Override
    public CompletableFuture<Long> publish(@NotNull String channel, byte @NotNull [] message) {
        return this.publishCommands.publish(channel, message).toCompletableFuture();
    }

    @Override
    public void subscribe(@NotNull String channel, @NotNull Consumer<byte[]> listener) {
        // 先登记监听器再订阅, 订阅生效后的第一条消息就能找到它
        this.listeners.put(channel, listener);
        this.subscribeCommands.subscribe(channel);
    }

    @Override
    public void unsubscribe(@NotNull String channel) {
        this.listeners.remove(channel);
        this.subscribeCommands.unsubscribe(channel);
    }

    @Override
    public boolean isOpen() {
        return this.publishConnection.isOpen() && this.subscribeConnection.isOpen();
    }

    @Override
    public void close() {
        this.subscribeConnection.close();
        this.publishConnection.close();
        this.listeners.clear();
    }
}