package partitioner;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.streams.processor.StreamPartitioner;

public class RandomPartitioner<K, V> implements StreamPartitioner<K, V>{

	@Override
	public Integer partition(K arg0, V arg1, int numberOfPartitions) {
		return ThreadLocalRandom.current().nextInt(0, numberOfPartitions);
	}

}
