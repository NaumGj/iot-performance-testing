package consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.json.JSONObject;

import constants.Const;
import mqtt.MqttPublishClient;
import mqtt.MqttPublishRunnable;

public class LatencyConsumer {

	private static final String GROUP_ID = "latency";

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public static void main(String[] argv) throws Exception{
		MqttClient mqttClient = MqttPublishClient.setupMqttConnection(Const.PIPE_ACCESS_TOKEN);
		ConsumerThread consumerRunnable = new ConsumerThread(Const.PIPE_TOPIC, GROUP_ID, mqttClient);
		consumerRunnable.start();
		consumerRunnable.join();
	}

	private static class ConsumerThread extends Thread {

		private MqttClient mqttClient;
		private KafkaConsumer<String,String> kafkaConsumer;

		public ConsumerThread(String topicName, String groupId, MqttClient mqttClient){
			this.mqttClient = mqttClient;
			
			Properties consumerProps = ConsumerUtils.configureConsumerProperties(groupId);

			this.kafkaConsumer = new KafkaConsumer<String, String>(consumerProps);
			this.kafkaConsumer.subscribe(Arrays.asList(topicName));
		}
		
		public void run() {
			String messageContent = new JSONObject().toString();
			MqttPublishRunnable mqttPublishRunnable = new MqttPublishRunnable(this.mqttClient, messageContent);
			scheduler.scheduleAtFixedRate(mqttPublishRunnable, 1, Const.MQTT_REPORT_FREQ, TimeUnit.SECONDS);

			try {
				while (true) {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
					for (ConsumerRecord<String, String> record : records) {
						String[] values = record.value().split(",");

						Long produced = Long.parseLong(String.valueOf(values[values.length - 2]));
						Long processedByStreams = Long.parseLong(String.valueOf(values[values.length - 1]));
						
						Long latencyProcessedMillis = System.currentTimeMillis() - processedByStreams;
						Long latencyConsumedMillis = System.currentTimeMillis() - produced;
						
						Double latencyProcessedSec = latencyProcessedMillis / 1000.0;
						Double latencyConsumedSec = latencyConsumedMillis / 1000.0;
						
						messageContent = new JSONObject()
								.put("latencyProcessed", latencyProcessedSec < 0 ? 0 : latencyProcessedSec)
								.put("latencyConsumed", latencyConsumedSec)
								.toString();
						
						mqttPublishRunnable.setMessageContent(messageContent);
					}
				}
			} catch(WakeupException ex) {
				System.out.println("Exception caught " + ex.getMessage());
			} finally {
				kafkaConsumer.close();
				System.out.println("After closing KafkaConsumer...");
			}
		}
	}
}

