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
     * 这条消息的处理逻辑在哪个执行器上运行, 默认是共享的虚拟线程执行器.
     * <p>需要按接收顺序处理的一组消息返回同一个串行执行器, 见 {@link MessageExecutors#newSerialExecutor()}.
     *
     * @return 运行处理逻辑的执行器
     */
    @NotNull
    default Executor executor() {
        return MessageExecutors.VIRTUAL;
    }

    /**
     * 定向发给本服时是否直接在本地处理. 本地处理不经过 Redis 也不编解码, 处理逻辑拿到的就是发送时的那个消息对象.
     * <p>返回 {@code false} 时消息照常发到本服的频道.
     *
     * @return 是否在本地直接处理
     */
    default boolean shortCircuitLocally() {
        return true;
    }
}