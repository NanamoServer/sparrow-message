package net.momirealms.sparrow.message.codec;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public interface MessageCodec<M> extends MessageEncoder<ByteBuf, M>, MessageDecoder<ByteBuf, M> {

    /**
     * 用一对直接读写 {@link ByteBuf} 的方法创建 codec.
     * <p>常见写法是 {@code MessageCodec.of(Message::write, Message::new)}.
     *
     * @param encoder 把消息写进缓冲区的方法
     * @param decoder 从缓冲区读出消息的方法
     * @param <M> 消息类型
     * @return 组合好的 codec
     */
    @NotNull
    static <M> MessageCodec<M> of(
            @NotNull MessageEncoder<? super ByteBuf, M> encoder,
            @NotNull MessageDecoder<? super ByteBuf, M> decoder
    ) {
        return new MessageCodec<>() {
            @Override
            public void encode(@NotNull M message, @NotNull ByteBuf buffer) {
                encoder.encode(message, buffer);
            }

            @NotNull
            @Override
            public M decode(@NotNull ByteBuf buffer) {
                return decoder.decode(buffer);
            }
        };
    }

    /**
     * 用一对读写自定义缓冲区的方法创建 codec, 每次编解码前先用包装函数把 {@link ByteBuf} 包成那种缓冲区.
     * <p>例如 {@code MessageCodec.of(FriendlyByteBuf::new, Message::write, Message::new)}.
     *
     * @param wrapper 把 {@link ByteBuf} 包成自定义缓冲区的函数, <strong>包装后必须读写同一块底层数据</strong>
     * @param encoder 把消息写进自定义缓冲区的方法
     * @param decoder 从自定义缓冲区读出消息的方法
     * @param <B> 自定义缓冲区类型
     * @param <M> 消息类型
     * @return 组合好的 codec
     */
    @NotNull
    static <B, M> MessageCodec<M> of(
            @NotNull Function<? super ByteBuf, ? extends B> wrapper,
            @NotNull MessageEncoder<? super B, M> encoder,
            @NotNull MessageDecoder<? super B, M> decoder
    ) {
        return new MessageCodec<>() {
            @Override
            public void encode(@NotNull M message, @NotNull ByteBuf buffer) {
                encoder.encode(message, wrapper.apply(buffer));
            }

            @NotNull
            @Override
            public M decode(@NotNull ByteBuf buffer) {
                return decoder.decode(wrapper.apply(buffer));
            }
        };
    }
}