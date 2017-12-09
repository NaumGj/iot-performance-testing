package streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;

import constants.Const;

public class HighDemand {

	public static void main(String[] args) throws Exception {
		Properties props = StreamUtils.configureStreamProperties("streams-high-demand");

		final StreamsBuilder builder = new StreamsBuilder();

		builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
			.selectKey((key, value) -> value.split(",")[7])
			.filter((key, value) -> Long.valueOf(key) < 264)
			.groupByKey()
			.windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(10)).advanceBy(TimeUnit.SECONDS.toMillis(1)))
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
}
