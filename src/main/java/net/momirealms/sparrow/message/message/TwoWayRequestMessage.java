package net.momirealms.sparrow.message.message;

import net.momirealms.sparrow.message.RedisMessage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public abstract class TwoWayRequestMessage<R extends TwoWayResponseMessage> implements RedisMessage {
    private String sourceServer; // 发起这次请求的服务器 ID,

    /**
     * 处理这次请求并给出响应, 在 {@link #executor()} 返回的执行器上运行.
     * <p>返回的 Future 可以稍后在别的线程完成, broker 等它完成后再把响应发回请求方.
     * Future 以 {@code null} 完成表示不回应, 请求方会一直等到超时.
     * Future 以异常完成或本方法直接抛出异常时, 请求方收到 {@code RemoteMessageException}.
     *
     * @return 产出响应的 Future
     */
    @NotNull
    public abstract CompletableFuture<R> handleRequest();

    @NotNull
    public final String sourceServer() {
        return this.sourceServer;
    }

    public final void sourceServer(@NotNull String sourceServer) {
        this.sourceServer = sourceServer;
    }
}