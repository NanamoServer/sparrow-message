package net.momirealms.sparrow.message.codec;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface MessageDecoder<B, M> {

    @NotNull
    M decode(@NotNull B buffer);
}