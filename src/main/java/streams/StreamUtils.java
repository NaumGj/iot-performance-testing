package streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import constants.Const;

public class StreamUtils {
	
	public static Properties configureStreamProperties(String applicationId) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Const.KAFKA_BROKER_1_IP + ":" + Const.KAFKA_PORT + "," +
															Const.KAFKA_BROKER_2_IP + ":" + Const.KAFKA_PORT);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Const.COMMIT_INTERVAL_MS);

		return props;
	}

}
