package net.momirealms.sparrow.message.exception;

import org.jetbrains.annotations.NotNull;

/**
 * 双向请求在处理它的服务器上失败了. 这里只带回原异常的类名和消息文本, 完整堆栈记录在处理端的日志里.
 */
public final class RemoteMessageException extends RuntimeException {
    private final String exceptionClass;
    private final String exceptionMessage;

    public RemoteMessageException(@NotNull String exceptionClass, @NotNull String exceptionMessage) {
        super(exceptionMessage.isEmpty() ? exceptionClass : exceptionClass + ": " + exceptionMessage);
        this.exceptionClass = exceptionClass;
        this.exceptionMessage = exceptionMessage;
    }

    @NotNull
    public String exceptionClass() {
        return this.exceptionClass;
    }

    /**
     * 处理端原异常的消息文本.
     *
     * @return 消息文本, 原异常没有消息时为空串
     */
    @NotNull
    public String exceptionMessage() {
        return this.exceptionMessage;
    }
}