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
import constants.Locations;
import mqtt.MqttPublishClient;
import mqtt.MqttPublishRunnable;

public class MostFreqDrivenConsumer {

	private static final String GROUP_ID = "freq";

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public static void main(String[] argv) throws Exception{
		MqttClient mqttClient = MqttPublishClient.setupMqttConnection(Const.MOST_FREQ_DRIVEN_ACCESS_TOKEN);
		ConsumerThread consumerRunnable = new ConsumerThread(Const.MOST_FREQ_DRIVEN_TOPIC, GROUP_ID, mqttClient);
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
						String[] values = record.value().split(";");
						String[] first = values[0].split(",");
						String[] second = values[1].split(",");
						String[] third = values[2].split(",");

						try {
							messageContent = new JSONObject()
									.put("mostFreqDriven1", Locations.locations.get(first[0]) + " - " + Locations.locations.get(first[1]))
									.put("mostFreqDriven1Freq", first[2])
									.put("mostFreqDriven2", Locations.locations.get(second[0]) + " - " + Locations.locations.get(second[1]))
									.put("mostFreqDriven2Freq", second[2])
									.put("mostFreqDriven3", Locations.locations.get(third[0]) + " - " + Locations.locations.get(third[1]))
									.put("mostFreqDriven3Freq", third[2])
									.toString();
							mqttPublishRunnable.setMessageContent(messageContent);
						} catch (ArrayIndexOutOfBoundsException e) {
							System.out.println(e.getMessage());
						}
					}
				}
			} catch(WakeupException ex) {
				System.out.println("Exception caught " + ex.getMessage());
			} catch (Exception e) {
				System.out.println("Exception: ");
				e.printStackTrace();
			} finally {
				kafkaConsumer.close();
				System.out.println("After closing KafkaConsumer...");
			}
		}		
	}
}
