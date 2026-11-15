package net.momirealms.sparrow.message;

import net.momirealms.sparrow.message.connection.RedisConnection;
import net.momirealms.sparrow.message.message.OneWayMessage;
import net.momirealms.sparrow.message.registry.RedisMessageRegistry;
import org.jetbrains.annotations.NotNull;

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

    void unsubscribe();

    /**
     * 向指定目标发送一条单向消息.
     * <p>返回的 Future 在 Redis 确认发布后完成, 不等待对端处理. 接收数量为 0 也算成功.
     * 目标是本服且消息允许本地短路时, 消息直接交给本地处理, Future 立即以 1 完成.
     * Redis 未连接或发布失败时 Future 以异常完成.
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

        @NotNull
        public MessageBroker build() {
            return new MessageBrokerImpl(this.appId, this.serverId, this.tags, this.connection, this.logger);
        }
    }
}