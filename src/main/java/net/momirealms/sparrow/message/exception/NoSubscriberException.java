package net.momirealms.sparrow.message.exception;

import org.jetbrains.annotations.NotNull;

/**
 * 双向请求发出时目标服务器没有订阅自己的频道, 通常是目标不在线. 请求不会有响应, 所以不等超时直接失败.
 */
public final class NoSubscriberException extends RuntimeException {
    private final String serverId;

    public NoSubscriberException(@NotNull String serverId) {
        super("No subscriber for server " + serverId);
        this.serverId = serverId;
    }

    @NotNull
    public String serverId() {
        return this.serverId;
    }
}