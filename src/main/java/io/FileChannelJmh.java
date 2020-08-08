package io;

import com.sun.nio.file.ExtendedOpenOption;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class FileChannelJmh {

    private static final int FILE_SIZE = 524288;

    public static void main(String... args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FileChannelJmh.class.getSimpleName())
                .forks(1)
                .warmupMode(WarmupMode.BULK)
                .warmupIterations(2)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @BenchmarkMode(Mode.SampleTime)
    public static int readFileWithDirectChannel(ReadState state) throws IOException {
        return state.directChannel.read(state.dest);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @BenchmarkMode(Mode.SampleTime)
    public static int readFileWithRegularChannel(ReadState state) throws IOException {
        return state.channel.read(state.dest);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @BenchmarkMode(Mode.SampleTime)
    public static int readFileWithRandomChannel(ReadState state) throws IOException {
        return state.channelFromRandomAccess.read(state.dest);
    }

    @State(Scope.Benchmark)
    public static class ReadState {
        private FileChannel directChannel;
        private FileChannel channel;
        private FileChannel channelFromRandomAccess;
        private ByteBuffer dest;

        @Setup(Level.Invocation)
        public void setUp() throws IOException {
            directChannel = FileChannel.open(prepareFile());
            channel = FileChannel.open(prepareFile(), ExtendedOpenOption.DIRECT);
            channelFromRandomAccess = new RandomAccessFile(prepareFile().toFile(), "r").getChannel();
            dest = ByteBuffer.allocateDirect(FILE_SIZE);
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            directChannel.close();
            channel.close();
            channelFromRandomAccess.close();
        }
    }

    private static Path prepareFile() throws IOException {
        File datafile = File.createTempFile(FileChannelJmh.class.getSimpleName() + "_" + System.currentTimeMillis() + ".dat", null);
        datafile.deleteOnExit();

        byte[] data = new byte[FILE_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (System.nanoTime() % 2 == 0 ? 1 : 0);
        }
        try (FileOutputStream fos = new FileOutputStream(datafile)) {
            fos.write(data);
        }
        return datafile.toPath();
    }
}

/*
Benchmark                                   Mode  Cnt      Score      Error  Units
FileChannelJmh.readFileWithDirectChannel   thrpt    5  11976.351 ± 4195.801  ops/s
FileChannelJmh.readFileWithRandomChannel   thrpt    5   1850.357 ±  406.292  ops/s
FileChannelJmh.readFileWithRegularChannel  thrpt    5  12851.198 ± 2808.837  ops/s

Benchmark                                   Mode  Cnt      Score     Error  Units
FileChannelJmh.readFileWithDirectChannel   thrpt    5  11655.385 ± 560.667  ops/s
FileChannelJmh.readFileWithRandomChannel   thrpt    5   1701.857 ± 135.315  ops/s
FileChannelJmh.readFileWithRegularChannel  thrpt    5  14585.029 ± 936.137  ops/s

Benchmark                                                                       Mode   Cnt     Score    Error  Units
FileChannelJmh.readFileWithDirectChannel                                      sample  1064    94.945 ±  3.321  us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.00      sample          46.144           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.50      sample          86.976           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.90      sample         115.904           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.95      sample         146.496           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.99      sample         258.253           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.999     sample         420.923           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.9999    sample         421.888           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p1.00      sample         421.888           us/op
FileChannelJmh.readFileWithRandomChannel                                      sample   994   486.935 ± 18.997  us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.00      sample          68.352           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.50      sample         484.864           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.90      sample         704.512           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.95      sample         724.224           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.99      sample         761.446           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.999     sample        2682.880           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.9999    sample        2682.880           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p1.00      sample        2682.880           us/op
FileChannelJmh.readFileWithRegularChannel                                     sample  1056    77.019 ±  2.984  us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.00    sample          40.576           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.50    sample          68.224           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.90    sample         103.539           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.95    sample         128.390           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.99    sample         199.391           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.999   sample         394.131           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.9999  sample         397.312           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p1.00    sample         397.312           us/op






 */
