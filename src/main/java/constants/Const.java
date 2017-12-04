package constants;

public class Const {

	// Kafka cluster
	public static final String KAFKA_IP = "52.178.89.30";
	
	// Topics
	public static final String TAXI_DATA_TOPIC = "taxi-data";
	public static final String THROUGHPUT_TOPIC = "throughput";
	public static final String PIPE_TOPIC = "piped-taxi-data";
	public static final String MOST_FREQ_DRIVEN_TOPIC = "most-freq-driven";
	
	// Streams
	public static final String COMMIT_INTERVAL_MS = "500";
	
	// MQTT access tokens
	public static final String PIPE_ACCESS_TOKEN = "ExhjDXXkx3naLxTM5y7g";
	public static final String THROUGHPUT_ACCESS_TOKEN = "q0AUcPPzMyuJZGh2GK7b";
	public static final String MOST_FREQ_DRIVEN_ACCESS_TOKEN = "2CWEktG8sUqmvBxmDs4A";
	
	// MQTT connection parameters
	public static final String MQTT_BROKER = "tcp://demo.thingsboard.io:1883";
	public static final String MQTT_PUBLISH_TOPIC = "v1/devices/me/telemetry";
	public static final int MQTT_MESSAGE_QOS = 0;
	public static final String MQTT_CLIENT_ID(String accessToken) {
		return "{\"username\": \"" + accessToken + "\"}";
	}
	
	// Other
	public static final String DUMMY_KEY = "dummy";
	public static final int MQTT_REPORT_FREQ = 2;	// in seconds
	
	
}
