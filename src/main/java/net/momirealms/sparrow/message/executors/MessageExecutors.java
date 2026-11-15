package net.momirealms.sparrow.message.executors;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class MessageExecutors {
    public static final Executor VIRTUAL = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("sparrow-message-virtual-", 0).factory());

    private MessageExecutors() {}

    /**
     * 创建一个串行执行器. 提交给它的任务按提交顺序一个接一个地在虚拟线程上运行, 前一个返回后才开始下一个.
     * <p>需要保序的一组消息让 {@code executor()} 返回同一个实例. 不同实例之间互不影响, 可以并行.
     *
     * @return 新的串行执行器
     */
    @NotNull
    public static Executor newSerialExecutor() {
        return new SerialExecutor();
    }

    private static final class SerialExecutor implements Executor {
        private final ArrayDeque<Runnable> pending = new ArrayDeque<>();
        private boolean running;

        @Override
        public void execute(@NotNull Runnable task) {
            synchronized (this.pending) {
                // 前面还有任务没跑完, 排队等它结束后接力
                if (this.running) {
                    this.pending.addLast(task);
                    return;
                }
                this.running = true;
            }
            VIRTUAL.execute(() -> this.run(task));
        }

        private void run(Runnable task) {
            try {
                task.run();
            } finally {
                // 任务抛异常也要接着跑下一个, 异常本身照常交给线程的未捕获异常处理
                Runnable next;
                synchronized (this.pending) {
                    next = this.pending.pollFirst();
                    if (next == null) this.running = false;
                }
                if (next != null) {
                    VIRTUAL.execute(() -> this.run(next));
                }
            }
        }
    }
}