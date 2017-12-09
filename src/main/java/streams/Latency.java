package streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;

import constants.Const;
import partitioner.RandomPartitioner;

public class Latency {

	private static Long messageCounter = 0L;

	public static void main(String[] args) throws Exception {

		Properties streamProps = StreamUtils.configureStreamProperties("streams-pipe");

		final StreamsBuilder builder = new StreamsBuilder();

		builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
			.flatMapValues(value -> Arrays.asList(value.toString() + "," + System.currentTimeMillis()))
			.filter(
				(key, value) -> {
					Latency.messageCounter++;
					return (Latency.messageCounter % 100) == 0;
				})
			.to(Const.PIPE_TOPIC, Produced.streamPartitioner(new RandomPartitioner<>()));

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, streamProps);

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
