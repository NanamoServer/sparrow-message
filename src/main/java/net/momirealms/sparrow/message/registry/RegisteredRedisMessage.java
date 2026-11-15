package net.momirealms.sparrow.message.registry;

import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.RedisMessage;
import net.momirealms.sparrow.message.codec.MessageCodec;
import org.jetbrains.annotations.NotNull;

/**
 * 一条注册记录.
 *
 * @param id 写进消息头的类型 ID, 怎么得来由 {@link TypeIdStrategy} 决定
 * @param key 注册时使用的消息标识
 * @param codec 这种消息的编解码器
 * @param <M> 消息类型
 */
public record RegisteredRedisMessage<M extends RedisMessage>(int id, @NotNull MessageIdentifier key, @NotNull MessageCodec<M> codec) {
}