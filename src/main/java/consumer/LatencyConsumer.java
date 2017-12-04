package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.json.JSONObject;

import constants.Const;
import mqtt.MqttPublishClient;
import mqtt.MqttPublishRunnable;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LatencyConsumer {

	private static final String GROUP_ID = "latency";

	private static Scanner in;

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public static void main(String[] argv) throws Exception{
		in = new Scanner(System.in);

		MqttClient mqttClient = MqttPublishClient.setupMqttConnection(Const.PIPE_ACCESS_TOKEN);
		ConsumerThread consumerRunnable = new ConsumerThread(Const.PIPE_TOPIC, GROUP_ID, mqttClient);
		consumerRunnable.start();
		String line = "";
		while (!line.equals("exit")) {
			line = in.next();
		}
		consumerRunnable.getKafkaConsumer().wakeup();
		System.out.println("Stopping consumer .....");
		consumerRunnable.join();
	}

	private static class ConsumerThread extends Thread {
		private String topicName;
		private String groupId;
		private MqttClient mqttClient;
		private KafkaConsumer<String,String> kafkaConsumer;

		public ConsumerThread(String topicName, String groupId, MqttClient mqttClient){
			this.topicName = topicName;
			this.groupId = groupId;
			this.mqttClient = mqttClient;
		}
		public void run() {
			Properties consumerProps = configureConsumerProperties(groupId);
			//Figure out where to start processing messages from
			kafkaConsumer = new KafkaConsumer<String, String>(consumerProps);
			kafkaConsumer.subscribe(Arrays.asList(topicName));

			String messageContent = new JSONObject().toString();
			MqttPublishRunnable mqttPublishRunnable = new MqttPublishRunnable(this.mqttClient, messageContent);
			scheduler.scheduleAtFixedRate(mqttPublishRunnable, 1, Const.MQTT_REPORT_FREQ, TimeUnit.SECONDS);
			//Start processing messages
			try {
				while (true) {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//					if (records.isEmpty()) {
//						messageContent = new JSONObject()
//								.put("latencyProcessed", 0)
//								.put("latencyConsumed", 0)
//								.toString();
//						mqttPublishRunnable.setMessageContent(messageContent);
//					}
					for (ConsumerRecord<String, String> record : records) {
						String[] values = record.value().split(",");

						Long produced = Long.parseLong(String.valueOf(values[values.length - 2]));
						Long processedByStreams = Long.parseLong(String.valueOf(values[values.length - 1]));
						
						Long latencyProcessedMillis = System.currentTimeMillis() - processedByStreams;
						Long latencyConsumedMillis = System.currentTimeMillis() - produced;
						
						Double latencyProcessed = latencyProcessedMillis / 1000.0;
						Double latencyConsumed = latencyConsumedMillis / 1000.0;
						
						messageContent = new JSONObject()
								.put("latencyProcessed", latencyProcessed < 0 ? 0 : latencyProcessed)
								.put("latencyConsumed", latencyConsumed)
								.toString();
						
						mqttPublishRunnable.setMessageContent(messageContent);
//						System.out.println("MESSAGE IN CONSUMER:");
//						System.out.println(messageContent);
//						System.out.println("KEY: " + record.key());
//						System.out.println("VALUE" + record.value());
//						System.out.println("LATENCY: " + latency);
					}
				}
			} catch(WakeupException ex) {
				System.out.println("Exception caught " + ex.getMessage());
			} finally {
				kafkaConsumer.close();
				System.out.println("After closing KafkaConsumer...");
			}
		}

		public KafkaConsumer<String,String> getKafkaConsumer(){
			return this.kafkaConsumer;
		}

		private static Properties configureConsumerProperties(String groupId) {
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Const.KAFKA_IP + ":9092");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			props.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

			return props;
		}
	}
}

