package streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import constants.Const;
import partitioner.RandomPartitioner;

public class Throughput {

    public static void main(String[] args) throws Exception {
        Properties props = StreamUtils.configureStreamProperties("streams-throughput");

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
        		.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
        		.windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(10)).advanceBy(TimeUnit.SECONDS.toMillis(1)))
        		.count()
        		.toStream()
        		.selectKey((key, value) -> Const.DUMMY_KEY)
        		.flatMapValues(value -> Arrays.asList(value.toString()))
                .to(Const.THROUGHPUT_TOPIC, Produced.streamPartitioner(new RandomPartitioner<>()));
        
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
