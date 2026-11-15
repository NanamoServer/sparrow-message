package net.momirealms.sparrow.message;

import net.momirealms.sparrow.message.executors.MessageExecutors;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;

public interface RedisMessage {

    @NotNull
    MessageIdentifier identifier();

    /**
     * 编码这条消息时预先分配的缓冲区字节数, 估得准可以省掉缓冲区扩容.
     *
     * @return 预估的编码后字节数
     */
    default int estimateSize() {
        return 16;
    }

    /**
     * 这条消息的处理逻辑在哪个执行器上运行, 默认是 {@link MessageExecutors#SYNC}.
     * <p>默认情况下处理逻辑直接跑在 Redis 客户端的 I/O 线程上, 消息按到达顺序依次处理.
     * <strong>在这条线程上不能阻塞, 也不能同步调用 Redis.</strong>
     * 需要阻塞的消息返回 {@link MessageExecutors#VIRTUAL}, 既要阻塞又要保序的返回同一个 {@link MessageExecutors#newSerialExecutor()}.
     *
     * @return 运行处理逻辑的执行器
     */
    @NotNull
    default Executor executor() {
        return MessageExecutors.SYNC;
    }

    /**
     * 定向发给本服时是否直接在本地处理. 本地处理不经过 Redis 也不编解码, 处理逻辑拿到的就是发送时的那个消息对象.
     * 执行器是 {@link MessageExecutors#SYNC} 时, 处理逻辑就在调用发布方法的那条线程上运行.
     * <p>返回 {@code false} 时消息照常发到本服的频道.
     *
     * @return 是否在本地直接处理
     */
    default boolean shortCircuitLocally() {
        return true;
    }
}