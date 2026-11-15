package net.momirealms.sparrow.message;

import org.jetbrains.annotations.NotNull;

/**
 * 一条已经定好去向并编码完的单向消息, 把 payload 发布到 channel 就等于发送了它.
 *
 * @param channel 要发布到的 Redis 频道名
 * @param payload 消息头加消息体的完整字节
 */
public record PreparedMessage(@NotNull String channel, byte @NotNull [] payload) {
}