package net.momirealms.sparrow.message.registry;

/**
 * 消息头里类型 ID 的分配方式.
 * <strong>appId 相同的所有 broker 必须使用同一种</strong>, 库不检查各端是否一致.
 */
public enum TypeIdStrategy {
    REGISTRATION_ORDER,
    IDENTIFIER_HASH
}