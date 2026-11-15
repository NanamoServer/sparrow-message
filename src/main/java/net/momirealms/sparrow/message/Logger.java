package net.momirealms.sparrow.message;

import org.jetbrains.annotations.NotNull;

public interface Logger {

    void error(@NotNull String message, @NotNull Throwable cause);

    void warn(@NotNull String message, @NotNull Throwable cause);

    void info(@NotNull String message);

    void debug(@NotNull String message);
}