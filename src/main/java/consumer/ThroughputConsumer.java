package consumer;

import java.util.Arrays;
import java.util.LinkedList;
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

public class ThroughputConsumer {
	
	private static final String GROUP_ID = "throughput";
	
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	public static void main(String[] argv) throws Exception{
		MqttClient mqttClient = MqttPublishClient.setupMqttConnection(Const.THROUGHPUT_ACCESS_TOKEN);
		ConsumerThread consumerRunnable = new ConsumerThread(Const.THROUGHPUT_TOPIC, GROUP_ID, mqttClient);
		consumerRunnable.start();
		consumerRunnable.join();
	}

	private static class ConsumerThread extends Thread {
		private MqttClient mqttClient;
		private KafkaConsumer<String,String> kafkaConsumer;

		public ConsumerThread(String topicName, String groupId, MqttClient mqttClient){
			this.mqttClient = mqttClient;
			
			Properties consumerProps = ConsumerUtils.configureConsumerProperties(groupId);

			kafkaConsumer = new KafkaConsumer<String, String>(consumerProps);
			kafkaConsumer.subscribe(Arrays.asList(topicName));
		}
		
		public void run() {
			LinkedList<Long> throughputs = new LinkedList<Long>();
			String messageContent = new JSONObject().toString();
			MqttPublishRunnable mqttPublishRunnable = new MqttPublishRunnable(this.mqttClient, messageContent);
			scheduler.scheduleAtFixedRate(mqttPublishRunnable, 1, Const.MQTT_REPORT_FREQ, TimeUnit.SECONDS);

			try {
				while (true) {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
					for (ConsumerRecord<String, String> record : records) {
						while (throughputs.size() > 10) {
							throughputs.removeLast();
						}
						throughputs.addFirst(Long.valueOf(record.value()));
						
						Long throughput = getCorrectThroughput(throughputs);
						
						messageContent = new JSONObject()
								.put("throughput", throughput).toString();
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
		
		public Long getCorrectThroughput(LinkedList<Long> list) {
			Long throughput = 0L;
			for (Long entry : list) {
				if (entry > throughput) {
					throughput = entry;
				}
			}
			
			return throughput;
		}
	}
}

