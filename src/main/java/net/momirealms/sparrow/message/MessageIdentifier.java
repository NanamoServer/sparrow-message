package net.momirealms.sparrow.message;

import org.jetbrains.annotations.NotNull;

public record MessageIdentifier(@NotNull String namespace, @NotNull String value) {

    @NotNull
    public static MessageIdentifier of(@NotNull String namespace, @NotNull String value) {
        return new MessageIdentifier(namespace, value);
    }

    @NotNull
    @Override
    public String toString() {
        return this.namespace + ":" + this.value;
    }
}