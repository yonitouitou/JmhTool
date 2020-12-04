package datastructure.lmax;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.concurrent.TimeUnit;

public class RingBufferJmh {

    public static void main(String... args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RingBufferJmh.class.getSimpleName())
                .forks(1)
                .warmupMode(WarmupMode.BULK)
                .warmupIterations(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public static void arrayBlockingQueue(ArrayBlockingQueueState state) throws InterruptedException {
        state.start();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public static void lmaxDisruptor(LmaxDisruptorState state) {
        state.start();
    }
}
