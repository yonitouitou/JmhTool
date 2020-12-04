package datastructure.lmax;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.openjdk.jmh.annotations.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
public class LmaxDisruptorState {

    @Param({"1000000"})
    private int numberOfMessages;

    @Param({"1"})
    private int numberOfProducerThreads;
    @Param({"1"})
    private int numberOfConsumerThreads;

    private AtomicInteger counter;
    private static RingBuffer<ValueEvent> ringBuffer;
    private ExecutorService executor;
    private List<Callable<Boolean>> producerTasks;

    @Setup(Level.Invocation)
    public void setUp() {
        counter = new AtomicInteger();
        LmaxDisruptor disruptor = new LmaxDisruptor();
        producerTasks = getProducerTasks();
        executor = Executors.newFixedThreadPool(numberOfProducerThreads);
    }

    private List<Callable<Boolean>> getProducerTasks() {
        Callable<Boolean> producerThread = () -> {
            AtomicInteger messageCounter = new AtomicInteger(0);
            while (messageCounter.incrementAndGet() <= numberOfMessages) {
                long sequenceId = ringBuffer.next();
                ValueEvent valueEvent = ringBuffer.get(sequenceId);
                valueEvent.setValue(counter.incrementAndGet());
                ringBuffer.publish(sequenceId);
            }
            return true;
        };
        List<Callable<Boolean>> tasks = new LinkedList<>();
        for (int i = 0; i < numberOfProducerThreads; i++) {
            tasks.add(producerThread);
        }
        return tasks;
    }


    public void start() throws InterruptedException {
        List<Future<Boolean>> futures = executor.invokeAll(getProducerTasks());

    }

    public class ValueEvent {
        private int value;
        public final static EventFactory EVENT_FACTORY = new IntegerEventFactory();

        private static class IntegerEventFactory implements EventFactory<Integer> {

            public Integer newInstance() {
                return 0;
            }
        }

        public void setValue(int v) {
            this.value = v;
        }

        public int getValue() {
            return value;
        }
    }

    public class SingleEventPrintConsumer {

        public EventHandler<ValueEvent>[] getEventHandler() {
            EventHandler<ValueEvent> eventHandler
                    = (event, sequence, endOfBatch)
                    -> print(event.getValue(), sequence);
            return new EventHandler[] { eventHandler };
        }

        private void print(int id, long sequenceId) {
            System.out.println("Id is " + id
                    + " sequence id that was used is " + sequenceId);
        }
    }

    public class LmaxDisruptor {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;

        public LmaxDisruptor() {
            WaitStrategy waitStrategy = new BusySpinWaitStrategy();
            Disruptor<ValueEvent> lmaxDisruptor = new Disruptor<>(
                    ValueEvent.EVENT_FACTORY,
                    2 << 20,
                    threadFactory,
                    ProducerType.MULTI,
                    waitStrategy);

            for (int i = 0; i < numberOfConsumerThreads; i++) {
                lmaxDisruptor.handleEventsWith(new SingleEventPrintConsumer().getEventHandler());
            }
            ringBuffer = lmaxDisruptor.start();
        }
    }
}
