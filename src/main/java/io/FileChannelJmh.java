package io;

import com.sun.nio.file.ExtendedOpenOption;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class FileChannelJmh {

    private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

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
        return state.directChannel.read(state.directDestination);
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

        @Param({ "4096", "524288" })
        private int fileSize;
        private FileChannel directChannel;
        private FileChannel channel;
        private FileChannel channelFromRandomAccess;
        private ByteBuffer dest;
        private ByteBuffer directDestination;
        private File fileForDirectChannel;
        private File fileForRegularChannel;
        private File fileForRandomChannel;

        @Setup(Level.Invocation)
        public void setUp() throws IOException {
            fileForDirectChannel = prepareFile();
            fileForRegularChannel = prepareFile();
            fileForRandomChannel = prepareFile();
            channel = FileChannel.open(fileForRegularChannel.toPath());
            channelFromRandomAccess = new RandomAccessFile(fileForRandomChannel, "r").getChannel();
            dest = ByteBuffer.allocateDirect(fileSize);
            directDestination = createDirectChannelDestination();
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            try {
                channel.close();
                directChannel.close();
                channelFromRandomAccess.close();
            } finally {
                fileForRegularChannel.delete();
                fileForDirectChannel.delete();
                fileForRandomChannel.delete();
            }
        }

        private ByteBuffer createDirectChannelDestination() throws IOException {
            directChannel = FileChannel.open(fileForDirectChannel.toPath(), ExtendedOpenOption.DIRECT);
            FileStore store = Files.getFileStore(fileForDirectChannel.toPath());
            int alignment = (int) store.getBlockSize();
            return ByteBuffer.allocateDirect(fileSize + alignment).alignedSlice(alignment);
        }

        private File prepareFile() throws IOException {
            File datafile = File.createTempFile(FileChannelJmh.class.getSimpleName() + "_" + RANDOM.nextInt(Integer.MAX_VALUE), "dat");
            byte[] data = new byte[fileSize];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (System.nanoTime() % 2 == 0 ? 1 : 0);
            }
            try (FileOutputStream fos = new FileOutputStream(datafile)) {
                fos.write(data);
            }
            return datafile;
        }
    }
}

/*
Benchmark                                  (fileSize)   Mode  Cnt       Score        Error  Units
FileChannelJmh.readFileWithDirectChannel         4096  thrpt    5  191627.414 ±  41659.578  ops/s
FileChannelJmh.readFileWithDirectChannel       524288  thrpt    5   11091.099 ±   1584.072  ops/s
FileChannelJmh.readFileWithRandomChannel         4096  thrpt    5   87038.371 ±  20830.794  ops/s
FileChannelJmh.readFileWithRandomChannel       524288  thrpt    5    3804.883 ±    384.859  ops/s
FileChannelJmh.readFileWithRegularChannel        4096  thrpt    5  210020.116 ± 142847.969  ops/s
FileChannelJmh.readFileWithRegularChannel      524288  thrpt    5    9718.036 ±   1019.347  ops/s

Benchmark                                  (fileSize)   Mode  Cnt       Score       Error  Units
FileChannelJmh.readFileWithDirectChannel         4096  thrpt    5  191581.138 ± 17027.857  ops/s
FileChannelJmh.readFileWithDirectChannel       524288  thrpt    5    9184.443 ±   440.638  ops/s
FileChannelJmh.readFileWithRandomChannel         4096  thrpt    5  130547.383 ± 45048.965  ops/s
FileChannelJmh.readFileWithRandomChannel       524288  thrpt    5    3924.667 ±   407.995  ops/s
FileChannelJmh.readFileWithRegularChannel        4096  thrpt    5  207453.785 ± 78615.599  ops/s
FileChannelJmh.readFileWithRegularChannel      524288  thrpt    5   10728.523 ±  1083.277  ops/s

Benchmark                                                                     (fileSize)    Mode    Cnt     Score    Error  Units
FileChannelJmh.readFileWithDirectChannel                                            4096  sample  25664     5.983 ±  0.650  us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.00            4096  sample            3.052           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.50            4096  sample            4.784           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.90            4096  sample            7.832           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.95            4096  sample           11.808           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.99            4096  sample           19.136           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.999           4096  sample           49.070           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.9999          4096  sample         1156.140           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p1.00            4096  sample         4366.336           us/op
FileChannelJmh.readFileWithDirectChannel                                          524288  sample    986    89.079 ±  3.091  us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.00          524288  sample           47.808           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.50          524288  sample           83.520           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.90          524288  sample          115.750           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.95          524288  sample          141.901           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.99          524288  sample          196.485           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.999         524288  sample          472.576           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p0.9999        524288  sample          472.576           us/op
FileChannelJmh.readFileWithDirectChannel:readFileWithDirectChannel·p1.00          524288  sample          472.576           us/op
FileChannelJmh.readFileWithRandomChannel                                            4096  sample  24902     7.727 ±  0.408  us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.00            4096  sample            2.616           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.50            4096  sample            4.176           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.90            4096  sample           15.408           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.95            4096  sample           21.536           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.99            4096  sample           43.838           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.999           4096  sample           77.964           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.9999          4096  sample         1187.323           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p1.00            4096  sample         2105.344           us/op
FileChannelJmh.readFileWithRandomChannel                                          524288  sample    935   246.087 ± 14.444  us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.00          524288  sample           36.928           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.50          524288  sample          270.848           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.90          524288  sample          331.776           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.95          524288  sample          344.576           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.99          524288  sample          384.573           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.999         524288  sample         3297.280           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p0.9999        524288  sample         3297.280           us/op
FileChannelJmh.readFileWithRandomChannel:readFileWithRandomChannel·p1.00          524288  sample         3297.280           us/op
FileChannelJmh.readFileWithRegularChannel                                           4096  sample  25287     4.728 ±  0.173  us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.00          4096  sample            2.516           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.50          4096  sample            3.892           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.90          4096  sample            6.256           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.95          4096  sample            9.280           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.99          4096  sample           17.440           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.999         4096  sample           46.830           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.9999        4096  sample          355.751           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p1.00          4096  sample         1122.304           us/op
FileChannelJmh.readFileWithRegularChannel                                         524288  sample    973    88.615 ±  3.549  us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.00        524288  sample           37.440           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.50        524288  sample           84.096           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.90        524288  sample          117.555           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.95        524288  sample          137.190           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.99        524288  sample          179.215           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.999       524288  sample          681.984           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p0.9999      524288  sample          681.984           us/op
FileChannelJmh.readFileWithRegularChannel:readFileWithRegularChannel·p1.00        524288  sample          681.984           us/op
 */
