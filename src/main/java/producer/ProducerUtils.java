package producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import constants.Const;

public class ProducerUtils {
	
	public static Properties configureProducerProperties() {
		Properties props = new Properties();
		
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Const.KAFKA_BROKER_1_IP + ":" + Const.KAFKA_PORT + "," +
				Const.KAFKA_BROKER_2_IP + ":" + Const.KAFKA_PORT);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		return props;
	}

}
