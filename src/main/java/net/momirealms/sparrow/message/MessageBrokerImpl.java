package net.momirealms.sparrow.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import net.momirealms.sparrow.message.connection.RedisConnection;
import net.momirealms.sparrow.message.executors.MessageExecutors;
import net.momirealms.sparrow.message.message.OneWayMessage;
import net.momirealms.sparrow.message.registry.DefaultMessageRegistry;
import net.momirealms.sparrow.message.registry.RedisMessageRegistry;
import net.momirealms.sparrow.message.registry.RegisteredRedisMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

final class MessageBrokerImpl implements MessageBroker {
    private static final int HEADER_ESTIMATE = 16; // 给消息头预留的缓冲区字节数

    private final String appId;
    private final String serverId;
    private final Set<String> tags;
    private final RedisConnection connection;
    private final Logger logger;
    private final RedisMessageRegistry registry = new DefaultMessageRegistry();
    private final String broadcastChannel; // 广播与 tag 消息走这个频道
    private final String serverChannel;    // 定向发给本服的消息走这个频道

    MessageBrokerImpl(String appId, String serverId, Set<String> tags, RedisConnection connection, Logger logger) {
        this.appId = appId;
        this.serverId = serverId;
        this.tags = tags;
        this.connection = connection;
        this.logger = logger;
        this.broadcastChannel = appId + ":all";
        this.serverChannel = this.serverChannel(serverId);
    }

    @NotNull
    @Override
    public String appId() {
        return this.appId;
    }

    @NotNull
    @Override
    public String serverId() {
        return this.serverId;
    }

    @NotNull
    @Override
    public Set<String> tags() {
        return this.tags;
    }

    @NotNull
    @Override
    public RedisMessageRegistry registry() {
        return this.registry;
    }

    @Override
    public void subscribe() {
        this.connection.subscribe(this.broadcastChannel, this::receive);
        this.connection.subscribe(this.serverChannel, this::receive);
    }

    @Override
    public void unsubscribe() {
        this.connection.unsubscribe(this.broadcastChannel);
        this.connection.unsubscribe(this.serverChannel);
    }

    @NotNull
    @Override
    public CompletableFuture<Long> publish(@NotNull OneWayMessage message, @NotNull Target target) {
        String channel = this.broadcastChannel;
        String tag = null;
        String excludedSource = null;
        switch (target.kind()) {
            case ALL -> {}
            case OTHERS -> excludedSource = this.serverId;
            case TAG -> tag = target.value();
            case SERVER -> {
                // 发给本服的消息不经过 Redis, 直接交给它的执行器
                if (target.value().equals(this.serverId) && message.shortCircuitLocally()) {
                    message.executor().execute(() -> this.handle(message));
                    return CompletableFuture.completedFuture(1L);
                }
                channel = this.serverChannel(target.value());
            }
        }
        if (!this.connection.isOpen()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Redis is not connected"));
        }
        byte[] payload = this.encodeOneWay(message, tag, excludedSource);
        // Redis 的结果在 I/O 线程上到达, 换到虚拟线程再完成对外的 Future, 调用方的回调可以阻塞
        CompletableFuture<Long> result = new CompletableFuture<>();
        this.connection.publish(channel, payload).whenComplete((receivers, error) -> MessageExecutors.VIRTUAL.execute(() -> {
            if (error != null) {
                result.completeExceptionally(error);
            } else {
                result.complete(receivers);
            }
        }));
        return result;
    }

    private byte[] encodeOneWay(OneWayMessage message, @Nullable String tag, @Nullable String excludedSource) {
        RegisteredRedisMessage<RedisMessage> registered = this.registry.byId(message.identifier());
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(HEADER_ESTIMATE + message.estimateSize());
        try {
            MessageHeader.writeOneWay(buffer, registered.id(), tag, excludedSource);
            registered.codec().encode(message, buffer);
            return ByteBufUtil.getBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    // 订阅频道收到消息时的入口, 运行在 Redis 客户端的 I/O 线程上.
    // 解码留在这条线程上完成, 消息按到达顺序交给各自的执行器, 串行执行器才能保序.
    private void receive(byte[] payload) {
        OneWayMessage message;
        try {
            ByteBuf buffer = Unpooled.wrappedBuffer(payload);
            MessageHeader header = MessageHeader.read(buffer);
            if (header.kind() != MessageHeader.ONE_WAY) return;
            // 不是发给本服的消息到此为止, 不解码消息体
            if (header.tag() != null && !this.tags.contains(header.tag())) return;
            if (this.serverId.equals(header.source())) return;
            // 对端注册的消息比本端多时会查不到
            RegisteredRedisMessage<RedisMessage> registered = this.registry.byId(header.typeId());
            if (registered == null) return;
            message = (OneWayMessage) registered.codec().decode(buffer);
        } catch (Throwable error) {
            this.logger.error("Failed to decode message on " + this.appId, error);
            return;
        }
        message.executor().execute(() -> this.handle(message));
    }

    private void handle(OneWayMessage message) {
        try {
            message.handle();
        } catch (Throwable error) {
            this.logger.error("Failed to handle message " + message.identifier(), error);
        }
    }

    private String serverChannel(String serverId) {
        return this.appId + ":server:" + serverId;
    }
}