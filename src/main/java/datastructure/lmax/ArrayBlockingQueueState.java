package datastructure.lmax;

import org.openjdk.jmh.annotations.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@State(Scope.Benchmark)
public class ArrayBlockingQueueState {

    private static final int QUEUE_CAPACITY = 2 << 20;

    @Param({"1000000"})
    private int numberOfMessages;
    @Param({"1"})
    private int numberOfProducerThreads;
    @Param({"1"})
    private int numberOfConsumerThreads;

    private BlockingQueue<Integer> blockingQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private ExecutorService producerExecutor;
    private ExecutorService consumerExecutor;
    private List<Callable<Boolean>> producerTasks;
    private List<Callable<Boolean>> consumerTasks;

    @Setup(Level.Invocation)
    public void setUp() {
        producerExecutor = Executors.newFixedThreadPool(numberOfProducerThreads);
        consumerExecutor = Executors.newFixedThreadPool(numberOfConsumerThreads);
        producerTasks = getProducerTasks();
        consumerTasks = getConsumerTasks();
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        producerExecutor.shutdownNow();
        consumerExecutor.shutdownNow();
    }

    void start() throws InterruptedException {
        List<Future<Boolean>> futures = producerExecutor.invokeAll(producerTasks);
        List<Future<Boolean>> futures1 = consumerExecutor.invokeAll(consumerTasks);
        Stream.concat(futures.stream(), futures1.stream())
                .forEach(future -> {
                    try {
                        future.get();
                    } catch (Exception e) {}
                });
    }

    private List<Callable<Boolean>> getProducerTasks() {
        Callable<Boolean> producerThread = () -> {
            AtomicInteger messageCounter = new AtomicInteger(0);
            while (messageCounter.incrementAndGet() <= numberOfMessages) {
                blockingQueue.put(messageCounter.get());
            }
            return true;
        };

        List<Callable<Boolean>> tasks = new LinkedList<>();
        for (int i = 0; i < numberOfConsumerThreads; i++) {
            tasks.add(producerThread);
        }
        return tasks;
    }

    private List<Callable<Boolean>> getConsumerTasks() {
        Callable<Boolean> consumerThread = () -> {
            Integer msg = 0;
            while (msg < numberOfMessages) {
                msg = blockingQueue.take();
            }
            return true;
        };

        List<Callable<Boolean>> tasks = new LinkedList<>();
        for (int i = 0; i < numberOfConsumerThreads; i++) {
            tasks.add(consumerThread);
        }
        return tasks;
    }
}
