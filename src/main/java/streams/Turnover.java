package streams;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import constants.Const;
import partitioner.RandomPartitioner;

public class Turnover {

    public static void main(String[] args) throws Exception {
        Properties props = StreamUtils.configureStreamProperties("streams-turnover");

        final StreamsBuilder builder = new StreamsBuilder();

//        KGroupedStream<String, String> groupedStream = builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
        builder.<String, String>stream(Const.TAXI_DATA_TOPIC)
        		.flatMap(
        			    (key, value) -> {
        			        List<KeyValue<String, String>> result = new LinkedList<>();
        			        result.add(KeyValue.pair(Const.DUMMY_KEY, value.split(",")[16]));
        			        return result;
        			    }
        		)
        		.groupByKey()
//        		.group((key, value) -> KeyValue.pair(Const.DUMMY_KEY, value.split(",")[16]))
//        KTable<Windowed<String>, String> table = groupedStream
        		.windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(10)).advanceBy(TimeUnit.SECONDS.toMillis(1)))
        		.aggregate(() -> "0.0", /* initializer */
        		        (aggKey, newValue, aggValue) -> {
        		        	 try {
	        			    	  return String.valueOf(Double.valueOf(aggValue) + Double.valueOf(newValue));
       			    	  } catch (Exception e) {
       			    		  System.out.println(e.getMessage());
       			    	  }
       			    	  return "0.0";
        		        } /* aggregator */
        		)
        		.toStream()
        		.selectKey((key, value) -> Const.DUMMY_KEY)
        		.to(Const.TURNOVER_TOPIC, Produced.streamPartitioner(new RandomPartitioner<>()));
        
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
