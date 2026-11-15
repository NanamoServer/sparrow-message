package net.momirealms.sparrow.message.message;

import net.momirealms.sparrow.message.RedisMessage;

public abstract class OneWayMessage implements RedisMessage {

    /**
     * 收到这条消息后要做的事, 在 {@link #executor()} 返回的执行器上运行.
     */
    public abstract void handle();
}