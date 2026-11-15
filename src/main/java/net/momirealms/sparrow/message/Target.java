package net.momirealms.sparrow.message;

import org.jetbrains.annotations.NotNull;

public final class Target {
    private static final Target ALL = new Target(Kind.ALL, "");
    private static final Target OTHERS = new Target(Kind.OTHERS, "");

    private final Kind kind;
    private final String value; // 服务器 ID 或 tag, 广播时为空串

    private Target(Kind kind, String value) {
        this.kind = kind;
        this.value = value;
    }

    /**
     * 所有订阅了同一个 appId 的服务器, 包含本服.
     *
     * @return 广播目标
     */
    @NotNull
    public static Target all() {
        return ALL;
    }

    /**
     * 所有订阅了同一个 appId 的服务器, 本服除外.
     *
     * @return 排除本服的广播目标
     */
    @NotNull
    public static Target others() {
        return OTHERS;
    }

    /**
     * 指定的一台服务器. 目标是本服时消息默认在本地直接处理, 见 {@link RedisMessage#shortCircuitLocally()}.
     *
     * @param serverId 目标服务器 ID
     * @return 定向目标
     */
    @NotNull
    public static Target server(@NotNull String serverId) {
        return new Target(Kind.SERVER, serverId);
    }

    /**
     * 所有带有指定 tag 的服务器, 本服带有这个 tag 时也包含本服.
     *
     * @param tag 服务器 tag
     * @return 按 tag 筛选的目标
     */
    @NotNull
    public static Target tag(@NotNull String tag) {
        return new Target(Kind.TAG, tag);
    }

    @NotNull
    Kind kind() {
        return this.kind;
    }

    @NotNull
    String value() {
        return this.value;
    }

    enum Kind {
        ALL, OTHERS, SERVER, TAG
    }
}