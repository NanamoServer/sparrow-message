package net.momirealms.sparrow.message.connection;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface RedisConnection {

    /**
     * 向频道发布一条消息.
     * <p>返回的 Future 在 Redis 确认后完成,
     * <strong>完成线程是 Redis 客户端的 I/O 线程, 后续回调不能阻塞</strong>.
     *
     * @param channel 频道名
     * @param message 消息内容
     * @return 收到这条消息的订阅连接数量, 发布失败时以异常完成
     */
    @NotNull
    CompletableFuture<Long> publish(@NotNull String channel, byte @NotNull [] message);

    /**
     * 订阅一个频道, Redis 确认订阅后返回.
     * <p>每个频道只有一个监听器. 监听器在 Redis 客户端的 I/O 线程上被调用, <strong>不能阻塞</strong>.
     *
     * @param channel 频道名
     * @param listener 接收消息内容的监听器
     */
    void subscribe(@NotNull String channel, @NotNull Consumer<byte[]> listener);

    void unsubscribe(@NotNull String channel);

    boolean isOpen();

    void close();
}