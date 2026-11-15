package net.momirealms.sparrow.message.codec;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface MessageEncoder<B, M> {

    void encode(@NotNull M message, @NotNull B buffer);
}