package streams;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;

import constants.Const;
import partitioner.RandomPartitioner;
import top3.MostFreqDrivenTop3;

public class MostFreqDriven {
	
	public static void main(String[] args) throws Exception {
		Properties props = StreamUtils.configureStreamProperties("streams-most-freq");

		final StreamsBuilder builder = new StreamsBuilder();
		
		builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
			.selectKey(
				(key, value) -> {
					String[] values = value.split(",");
					return values[7] + "," + values[8];
				}
			)
			.filter(
				(key, value) -> {
					String[] keyParts = key.split(",");
					return Long.valueOf(keyParts[0]) < 264 && Long.valueOf(keyParts[1]) < 264 && !keyParts[0].equals(keyParts[1]);
				}
			)
			.groupByKey()
			.count()
			.toStream()
			.flatMap(
				(key, value) -> {
				    List<KeyValue<String, String>> result = new LinkedList<>();
				    MostFreqDrivenTop3.handleEntry(key, value);
			    	result.add(KeyValue.pair(Const.DUMMY_KEY, MostFreqDrivenTop3.getString()));
				    return result;
				}
			)
			.to(Const.MOST_FREQ_DRIVEN_TOPIC, Produced.streamPartitioner(new RandomPartitioner<>()));
		
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
