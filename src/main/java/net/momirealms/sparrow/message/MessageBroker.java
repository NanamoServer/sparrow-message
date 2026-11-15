package net.momirealms.sparrow.message;

import net.momirealms.sparrow.message.connection.RedisConnection;
import net.momirealms.sparrow.message.message.OneWayMessage;
import net.momirealms.sparrow.message.message.TwoWayRequestMessage;
import net.momirealms.sparrow.message.message.TwoWayResponseMessage;
import net.momirealms.sparrow.message.registry.RedisMessageRegistry;
import net.momirealms.sparrow.message.registry.TypeIdStrategy;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface MessageBroker {

    @NotNull
    String appId();

    @NotNull
    String serverId();

    @NotNull
    Set<String> tags();

    @NotNull
    RedisMessageRegistry registry();

    /**
     * 订阅广播频道和本服频道, Redis 确认后返回.
     * <strong>调用前要先把所有消息注册完.</strong>
     */
    void subscribe();

    /**
     * 退订两个频道. 还在等待响应的双向请求立即以 {@link IllegalStateException} 失败.
     */
    void unsubscribe();

    /**
     * 向指定目标发送一条单向消息.
     * <p>返回的 Future 在 Redis 确认发布后完成, 不等待对端处理. 接收数量为 0 也算成功.
     * 目标是本服且消息允许本地短路时, 消息直接交给本地处理, Future 立即以 1 完成.
     * 发布失败时 Future 以异常完成. Redis 断线期间的发布由客户端排队, 重连后发出.
     * <p><strong>Future 在 Redis 客户端的 I/O 线程上完成, 挂在它上面的回调不能阻塞</strong>,
     * 需要阻塞时用 {@code thenAcceptAsync} 这类带执行器的方法.
     *
     * @param message 要发送的消息, <strong>必须已经注册</strong>
     * @param target 发送目标
     * @return 收到这条消息的订阅连接数量
     */
    @NotNull
    CompletableFuture<Long> publish(@NotNull OneWayMessage message, @NotNull Target target);

    /**
     * 向指定服务器发送一条单向消息, 等同于 {@code publish(message, Target.server(serverId))}.
     *
     * @param message 要发送的消息
     * @param serverId 目标服务器 ID
     * @return 收到这条消息的订阅连接数量
     */
    @NotNull
    default CompletableFuture<Long> publish(@NotNull OneWayMessage message, @NotNull String serverId) {
        return this.publish(message, Target.server(serverId));
    }

    /**
     * 向所有服务器广播一条单向消息, 本服也会收到.
     *
     * @param message 要发送的消息
     * @return 收到这条消息的订阅连接数量
     */
    @NotNull
    default CompletableFuture<Long> publishAll(@NotNull OneWayMessage message) {
        return this.publish(message, Target.all());
    }

    /**
     * 向本服以外的所有服务器广播一条单向消息.
     *
     * @param message 要发送的消息
     * @return 收到这条消息的订阅连接数量, 其中包含本服的订阅连接, 本服收到后会直接丢弃
     */
    @NotNull
    default CompletableFuture<Long> publishOthers(@NotNull OneWayMessage message) {
        return this.publish(message, Target.others());
    }

    /**
     * 把一条单向消息编码成频道名和字节, 交给调用方自己发布.
     * <p>用于把发布写进 Lua 脚本或事务, 让它和别的 Redis 写入一起原子完成,
     * 脚本里执行 {@code redis.call('PUBLISH', channel, payload)} 即可.
     * 这里不做本地短路, 目标是本服时消息同样经过 Redis.
     *
     * @param message 要发送的消息, <strong>必须已经注册</strong>
     * @param target 发送目标
     * @return 要发布到的频道和完整的消息字节
     */
    @NotNull
    PreparedMessage prepare(@NotNull OneWayMessage message, @NotNull Target target);

    /**
     * 向指定服务器发送一个请求并等待响应, 超时时间用创建 broker 时设置的默认值.
     *
     * @param request 要发送的请求
     * @param serverId 目标服务器 ID
     * @param <R> 响应类型
     * @return 对端给出的响应
     * @see #publishTwoWay(TwoWayRequestMessage, String, Duration)
     */
    @NotNull
    <R extends TwoWayResponseMessage> CompletableFuture<R> publishTwoWay(@NotNull TwoWayRequestMessage<R> request, @NotNull String serverId);

    /**
     * 向指定服务器发送一个请求并等待响应.
     * <p>返回的 Future 以下列异常之一失败:
     * <ul>
     *   <li>TimeoutException: 从调用开始算, 超过 timeout 仍没有响应. 对端不会被取消, 迟到的响应被忽略.</li>
     *   <li>NoSubscriberException: 目标服务器没有订阅, 通常是不在线.</li>
     *   <li>RemoteMessageException: 对端的 {@code handleRequest()} 失败了.</li>
     *   <li>IllegalStateException: 等待期间本 broker 退订了.</li>
     * </ul>
     * 目标是本服且请求允许本地短路时, 请求直接交给本地处理, 上面的超时和失败规则不变.
     *
     * @param request 要发送的请求, <strong>请求和它的响应类型都必须已经注册</strong>
     * @param serverId 目标服务器 ID
     * @param timeout 这次请求的超时时间
     * @param <R> 响应类型
     * @return 对端给出的响应
     */
    @NotNull
    <R extends TwoWayResponseMessage> CompletableFuture<R> publishTwoWay(@NotNull TwoWayRequestMessage<R> request, @NotNull String serverId, @NotNull Duration timeout);

    @NotNull
    static Builder builder() {
        return new Builder();
    }

    final class Builder {
        private String appId;
        private String serverId;
        private Set<String> tags = Set.of();
        private RedisConnection connection;
        private Logger logger;
        private Duration requestTimeout = Duration.ofSeconds(10);
        private TypeIdStrategy typeIdStrategy = TypeIdStrategy.IDENTIFIER_HASH;

        private Builder() {}

        /**
         * 设置这个 broker 的功能标识, 原样作为频道名前缀. 只有 appId 相同的 broker 之间才能通信.
         * <p>Redis 的频道在整个实例内全局可见, 建议写成 "插件名:功能名", 例如 {@code sparrow:teleport}.
         *
         * @param appId 功能标识
         * @return 此构建器
         */
        @NotNull
        public Builder appId(@NotNull String appId) {
            this.appId = appId;
            return this;
        }

        @NotNull
        public Builder serverId(@NotNull String serverId) {
            this.serverId = serverId;
            return this;
        }

        /**
         * 设置本服的 tag, 不设置时没有 tag.
         * 发往 {@link Target#tag(String)} 的消息只有带对应 tag 的服务器会处理.
         *
         * @param tags 本服的 tag
         * @return 此构建器
         */
        @NotNull
        public Builder tags(@NotNull Set<String> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * 设置使用的 Redis 连接, 同一个应用的多个 broker 可以共用一个. 连接由创建它的一方关闭.
         *
         * @param connection Redis 连接
         * @return 此构建器
         */
        @NotNull
        public Builder connection(@NotNull RedisConnection connection) {
            this.connection = connection;
            return this;
        }

        @NotNull
        public Builder logger(@NotNull Logger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * 设置双向请求的默认超时时间, 不设置时为 10 秒.
         * 单次请求可以在 publishTwoWay 上另行指定.
         *
         * @param requestTimeout 默认超时时间
         * @return 此构建器
         */
        @NotNull
        public Builder requestTimeout(@NotNull Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * 设置消息头里类型 ID 的分配方式, 不设置时为 {@link TypeIdStrategy#IDENTIFIER_HASH}.
         * <strong>appId 相同的所有 broker 必须设置成同一种</strong>, 两种方式的消息头互不兼容, 库不检查各端是否一致.
         *
         * @param typeIdStrategy 类型 ID 的分配方式
         * @return 此构建器
         */
        @NotNull
        public Builder typeIdStrategy(@NotNull TypeIdStrategy typeIdStrategy) {
            this.typeIdStrategy = typeIdStrategy;
            return this;
        }

        @NotNull
        public MessageBroker build() {
            Objects.requireNonNull(this.appId, "appId");
            Objects.requireNonNull(this.serverId, "serverId");
            Objects.requireNonNull(this.tags, "tags");
            Objects.requireNonNull(this.connection, "connection");
            Objects.requireNonNull(this.logger, "logger");
            Objects.requireNonNull(this.requestTimeout, "requestTimeout");
            return new MessageBrokerImpl(this.appId, this.serverId, this.tags, this.connection, this.logger, this.requestTimeout, this.typeIdStrategy);
        }
    }
}