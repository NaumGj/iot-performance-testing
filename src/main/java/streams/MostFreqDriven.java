package streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;

import constants.Const;

public class MostFreqDriven {

	public static void main(String[] args) throws Exception {
		Properties props = configureStreamsProperties();

		final StreamsBuilder builder = new StreamsBuilder();

		builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
			.selectKey(
				new KeyValueMapper<String, String, String>() {
					@Override
					public String apply(String key, String value) {
						String[] values = value.split(",");
						return values[7] + "," + values[8];
					}
				})
			.filter(
				new Predicate<String, String>() {
					@Override
					public boolean test(String key, String value) {
						String[] keyParts = key.split(",");
						return Long.valueOf(keyParts[0]) < 264 && Long.valueOf(keyParts[1]) < 264;
					}
				})
			.groupByKey()
			.count()
			.toStream()
			.flatMapValues(value -> Arrays.asList(value.toString()))
			.to(Const.MOST_FREQ_DRIVEN_TOPIC);

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

	private static Properties configureStreamsProperties() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-most-freq");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Const.KAFKA_IP + ":9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Const.COMMIT_INTERVAL_MS);

		return props;
	}
}
