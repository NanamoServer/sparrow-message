package net.momirealms.sparrow.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import net.momirealms.sparrow.message.connection.RedisConnection;
import net.momirealms.sparrow.message.exception.NoSubscriberException;
import net.momirealms.sparrow.message.exception.RemoteMessageException;
import net.momirealms.sparrow.message.executors.MessageExecutors;
import net.momirealms.sparrow.message.message.OneWayMessage;
import net.momirealms.sparrow.message.message.TwoWayRequestMessage;
import net.momirealms.sparrow.message.message.TwoWayResponseMessage;
import net.momirealms.sparrow.message.registry.HashedMessageRegistry;
import net.momirealms.sparrow.message.registry.OrderedMessageRegistry;
import net.momirealms.sparrow.message.registry.RedisMessageRegistry;
import net.momirealms.sparrow.message.registry.RegisteredRedisMessage;
import net.momirealms.sparrow.message.registry.TypeIdStrategy;
import net.momirealms.sparrow.message.util.ByteBufHelper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

final class MessageBrokerImpl implements MessageBroker {
    // 给消息头预留的缓冲区字节数
    private static final int HEADER_ESTIMATE = 32;
    // 错误响应里异常消息文本的最大字符数, 超出的部分截掉
    private static final int MAX_ERROR_TEXT_LENGTH = 32767;
    // 所有 broker 共用的请求超时计时器. 它只负责到点触发, 让 Future 失败的动作仍然换到虚拟线程上做
    private static final ScheduledExecutorService TIMEOUT_TIMER = new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().name("sparrow-message-timeout").daemon(true).factory()) {{
        this.setRemoveOnCancelPolicy(true);
    }};

    private final String appId;
    private final String serverId;
    private final Set<String> tags;
    private final RedisConnection connection;
    private final Logger logger;
    private final Duration requestTimeout;
    private final RedisMessageRegistry registry;
    private final String broadcastChannel; // 广播与 tag 消息走这个频道
    private final String serverChannel;    // 定向发给本服的消息, 请求和响应走这个频道

    // 关联 ID -> 还在等响应的请求
    private final Map<Long, CompletableFuture<TwoWayResponseMessage>> pending = new ConcurrentHashMap<>();
    // 关联 ID 从随机位置起步. 两个进程短暂共用同一个 serverId 时会收到彼此的响应, 随机起点让它们的关联 ID 对不上号
    private final AtomicLong nextCorrelationId = new AtomicLong(ThreadLocalRandom.current().nextLong());

    MessageBrokerImpl(String appId, String serverId, Set<String> tags, RedisConnection connection, Logger logger, Duration requestTimeout, TypeIdStrategy typeIdStrategy) {
        this.appId = appId;
        this.serverId = serverId;
        this.tags = tags;
        this.connection = connection;
        this.logger = logger;
        this.requestTimeout = requestTimeout;
        this.registry = typeIdStrategy == TypeIdStrategy.IDENTIFIER_HASH ? new HashedMessageRegistry() : new OrderedMessageRegistry();
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
        // 响应再也收不到了, 等待中的请求不必耗到各自超时
        IllegalStateException unsubscribed = new IllegalStateException("Message broker is unsubscribed");
        for (CompletableFuture<TwoWayResponseMessage> future : this.pending.values()) {
            this.fail(future, unsubscribed);
        }
    }

    @NotNull
    @Override
    public CompletableFuture<Long> publish(@NotNull OneWayMessage message, @NotNull Target target) {
        // 发给本服的消息不经过 Redis, 直接交给它的执行器
        if (target.kind() == Target.Kind.SERVER && target.value().equals(this.serverId) && message.shortCircuitLocally()) {
            message.executor().execute(() -> this.handle(message));
            return CompletableFuture.completedFuture(1L);
        }
        PreparedMessage prepared = this.prepare(message, target);
        return this.connection.publish(prepared.channel(), prepared.payload());
    }

    @NotNull
    @Override
    public PreparedMessage prepare(@NotNull OneWayMessage message, @NotNull Target target) {
        String channel = this.broadcastChannel;
        String tag = null;
        String excludedSource = null;
        switch (target.kind()) {
            case ALL -> {}
            case OTHERS -> excludedSource = this.serverId;
            case TAG -> tag = target.value();
            case SERVER -> channel = this.serverChannel(target.value());
        }
        return new PreparedMessage(channel, this.encode(message, MessageHeader.ONE_WAY, tag, excludedSource, 0L));
    }

    @NotNull
    @Override
    public <R extends TwoWayResponseMessage> CompletableFuture<R> publishTwoWay(@NotNull TwoWayRequestMessage<R> request, @NotNull String serverId) {
        return this.publishTwoWay(request, serverId, this.requestTimeout);
    }

    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public <R extends TwoWayResponseMessage> CompletableFuture<R> publishTwoWay(@NotNull TwoWayRequestMessage<R> request, @NotNull String serverId, @NotNull Duration timeout) {
        boolean local = serverId.equals(this.serverId) && request.shortCircuitLocally();
        // 编码放在登记之前, 编码失败时不会留下等待记录
        long correlationId = this.nextCorrelationId.getAndIncrement();
        byte[] payload = local ? null : this.encode(request, MessageHeader.REQUEST, null, this.serverId, correlationId);
        // 登记等待记录并从现在开始计时
        CompletableFuture<R> result = new CompletableFuture<>();
        this.pending.put(correlationId, (CompletableFuture<TwoWayResponseMessage>) result);
        MessageIdentifier identifier = request.identifier();
        ScheduledFuture<?> timer = TIMEOUT_TIMER.schedule(
                () -> this.fail(result, new TimeoutException("Request " + identifier + " to " + serverId + " timed out after " + timeout.toMillis() + " ms")),
                timeout.toNanos(),
                TimeUnit.NANOSECONDS
        );
        // 无论以响应, 超时, 取消还是失败结束, 都在 Future 完成时清掉等待记录并撤掉计时任务
        result.whenComplete((response, error) -> {
            this.pending.remove(correlationId);
            timer.cancel(false);
        });
        // 发给本服的请求不经过 Redis, 直接交给它的执行器
        if (local) {
            request.executor().execute(() -> this.invoke(request, this.serverId).whenComplete((response, error) -> {
                if (error != null) {
                    Throwable cause = unwrap(error);
                    this.logger.error("Failed to handle request " + request.identifier() + " from " + this.serverId, cause);
                    this.fail(result, remoteException(cause));
                } else if (response != null) {
                    this.complete(result, response);
                }
            }));
            return result;
        }
        this.connection.publish(this.serverChannel(serverId), payload).whenComplete((receivers, error) -> {
            if (error != null) {
                this.fail(result, error);
            } else if (receivers == 0L) {
                // 目标没有订阅自己的频道, 不会有人回应
                this.fail(result, new NoSubscriberException(serverId));
            }
        });
        return result;
    }

    private byte[] encode(RedisMessage message, int kind, @Nullable String tag, @Nullable String source, long correlationId) {
        RegisteredRedisMessage<RedisMessage> registered = this.registry.byId(message.identifier());
        // 用不走内存池的堆缓冲区, 发布常常发生在一次性的虚拟线程上
        ByteBuf buffer = Unpooled.buffer(HEADER_ESTIMATE + message.estimateSize());
        switch (kind) {
            case MessageHeader.ONE_WAY -> MessageHeader.writeOneWay(buffer, this.registry, registered.id(), tag, source);
            case MessageHeader.REQUEST -> MessageHeader.writeRequest(buffer, this.registry, registered.id(), source, correlationId);
            case MessageHeader.RESPONSE -> MessageHeader.writeResponse(buffer, this.registry, registered.id(), correlationId);
        }
        registered.codec().encode(message, buffer);
        return ByteBufUtil.getBytes(buffer, 0, buffer.writerIndex(), false);
    }

    private byte[] encodeError(long correlationId, Throwable cause) {
        String text = cause.getMessage() == null ? "" : cause.getMessage();
        if (text.length() > MAX_ERROR_TEXT_LENGTH) text = text.substring(0, MAX_ERROR_TEXT_LENGTH);
        ByteBuf buffer = Unpooled.buffer(HEADER_ESTIMATE + text.length());
        MessageHeader.writeError(buffer, correlationId);
        ByteBufHelper.writeUtf8(buffer, cause.getClass().getName());
        ByteBufHelper.writeUtf8(buffer, text);
        return ByteBufUtil.getBytes(buffer, 0, buffer.writerIndex(), false);
    }

    // 订阅频道收到消息时的入口, 运行在 Redis 客户端的 I/O 线程上.
    // 解码留在这条线程上完成, 消息按到达顺序交给各自的执行器, 串行执行器才能保序.
    private void receive(byte[] payload) {
        try {
            ByteBuf buffer = Unpooled.wrappedBuffer(payload);
            MessageHeader header = MessageHeader.read(buffer, this.registry);
            switch (header.kind()) {
                case MessageHeader.ONE_WAY -> this.receiveOneWay(header, buffer);
                case MessageHeader.REQUEST -> this.receiveRequest(header, buffer);
                case MessageHeader.RESPONSE -> this.receiveResponse(header, buffer);
                case MessageHeader.ERROR -> this.receiveError(header, buffer);
            }
        } catch (Throwable error) {
            this.logger.error("Failed to decode message on " + this.appId, error);
        }
    }

    private void receiveOneWay(MessageHeader header, ByteBuf buffer) {
        // 不是发给本服的消息到此为止, 不解码消息体
        if (header.tag() != null && !this.tags.contains(header.tag())) return;
        if (this.serverId.equals(header.source())) return;
        // 对端注册了本端没有的消息时会查不到
        RegisteredRedisMessage<RedisMessage> registered = this.registry.byId(header.typeId());
        if (registered == null) return;
        OneWayMessage message = (OneWayMessage) registered.codec().decode(buffer);
        message.executor().execute(() -> this.handle(message));
    }

    private void receiveRequest(MessageHeader header, ByteBuf buffer) {
        RegisteredRedisMessage<RedisMessage> registered = this.registry.byId(header.typeId());
        if (registered == null) return;
        TwoWayRequestMessage<?> request = (TwoWayRequestMessage<?>) registered.codec().decode(buffer);
        String source = header.source();
        long correlationId = header.correlationId();
        request.executor().execute(() -> this.invoke(request, source).whenComplete((response, error) -> this.reply(request, source, correlationId, response, error)));
    }

    private void receiveResponse(MessageHeader header, ByteBuf buffer) {
        // 查不到等待记录的是迟到的响应, 或者是发给同 serverId 另一个进程的响应
        CompletableFuture<TwoWayResponseMessage> future = this.pending.get(header.correlationId());
        if (future == null) return;
        RegisteredRedisMessage<RedisMessage> registered = this.registry.byId(header.typeId());
        if (registered == null) return;
        this.complete(future, (TwoWayResponseMessage) registered.codec().decode(buffer));
    }

    private void receiveError(MessageHeader header, ByteBuf buffer) {
        CompletableFuture<TwoWayResponseMessage> future = this.pending.get(header.correlationId());
        if (future == null) return;
        String exceptionClass = ByteBufHelper.readUtf8(buffer);
        String exceptionMessage = ByteBufHelper.readUtf8(buffer);
        this.fail(future, new RemoteMessageException(exceptionClass, exceptionMessage));
    }

    private void handle(OneWayMessage message) {
        try {
            message.handle();
        } catch (Throwable error) {
            this.logger.error("Failed to handle message " + message.identifier(), error);
        }
    }

    // 调用请求的处理逻辑. handleRequest() 直接抛出的异常也转成失败的 Future, 后面只需要处理一种失败形态.
    private <R extends TwoWayResponseMessage> CompletableFuture<R> invoke(TwoWayRequestMessage<R> request, String source) {
        request.sourceServer(source);
        try {
            return request.handleRequest();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    // 把请求的处理结果发回请求方. 这里运行在业务代码完成 Future 的线程上, 抛出的异常没有别处可以接住.
    private void reply(TwoWayRequestMessage<?> request, String source, long correlationId, @Nullable TwoWayResponseMessage response, @Nullable Throwable error) {
        try {
            byte[] payload;
            if (error != null) {
                Throwable cause = unwrap(error);
                this.logger.error("Failed to handle request " + request.identifier() + " from " + source, cause);
                payload = this.encodeError(correlationId, cause);
            } else if (response != null) {
                payload = this.encode(response, MessageHeader.RESPONSE, null, null, correlationId);
            } else {
                // 以 null 完成表示不回应
                return;
            }
            this.connection.publish(this.serverChannel(source), payload).whenComplete((receivers, failure) -> {
                if (failure != null) this.logger.error("Failed to publish response of " + request.identifier() + " to " + source, failure);
            });
        } catch (Throwable failure) {
            this.logger.error("Failed to reply request " + request.identifier() + " from " + source, failure);
        }
    }

    // 请求的 Future 换到虚拟线程上完成. 结果常常在 I/O 线程上到达, 调用方拿到响应后的回调可以放心阻塞.
    private <T> void complete(CompletableFuture<T> future, T value) {
        MessageExecutors.VIRTUAL.execute(() -> future.complete(value));
    }

    private void fail(CompletableFuture<?> future, Throwable error) {
        MessageExecutors.VIRTUAL.execute(() -> future.completeExceptionally(error));
    }

    private String serverChannel(String serverId) {
        return this.appId + ":server:" + serverId;
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    private static RemoteMessageException remoteException(Throwable cause) {
        return new RemoteMessageException(cause.getClass().getName(), cause.getMessage() == null ? "" : cause.getMessage());
    }
}