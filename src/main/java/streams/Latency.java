package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Predicate;

import constants.Const;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Latency {

	private static Long messageCounter = 0L;

	public static void main(String[] args) throws Exception {

		Properties streamProps = configureStreamsProperties();

		final StreamsBuilder builder = new StreamsBuilder();

		builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
			.flatMapValues(value -> Arrays.asList(value.toString() + "," + System.currentTimeMillis()))
			.filter(
				new Predicate<String, String>() {
					@Override
					public boolean test(String key, String value) {
						Latency.messageCounter++;
						return (Latency.messageCounter % 100) == 0;
					}
				})
			.to(Const.PIPE_TOPIC);

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

	private static Properties configureStreamsProperties() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Const.KAFKA_IP + ":9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Const.COMMIT_INTERVAL_MS);

		return props;
	}
}
