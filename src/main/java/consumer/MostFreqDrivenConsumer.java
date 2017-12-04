package consumer;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

public class MostFreqDrivenConsumer {
	
	private static final String GROUP_ID = "freq";
	private static final Integer TOP_LIST_SIZE = 3;
	
	private static Scanner in;
	
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	public static void main(String[] argv) throws Exception{
		in = new Scanner(System.in);

		MqttClient mqttClient = MqttPublishClient.setupMqttConnection(Const.MOST_FREQ_DRIVEN_ACCESS_TOKEN);
		ConsumerThread consumerRunnable = new ConsumerThread(Const.MOST_FREQ_DRIVEN_TOPIC, GROUP_ID, mqttClient);
		consumerRunnable.start();
		// TODO: see if this can be deleted
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
		
		private LinkedList<String> mostFreqDriven;
		private LinkedList<Long> mostFreqDrivenCnt;

		public ConsumerThread(String topicName, String groupId, MqttClient mqttClient){
			this.topicName = topicName;
			this.groupId = groupId;
			this.mqttClient = mqttClient;
			
			this.mostFreqDriven = new LinkedList<String>();
			fillLinkedList(this.mostFreqDriven, "N/A");
			this.mostFreqDrivenCnt = new LinkedList<Long>();
			fillLinkedList(this.mostFreqDrivenCnt, 0L);
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
					for (ConsumerRecord<String, String> record : records) {
						Long count = Long.valueOf(record.value());
						checkCount(record.key(), count);
						
						messageContent = new JSONObject()
								.put("mostFreqDriven1", this.mostFreqDriven.get(0))
								.put("mostFreqDriven1Freq", this.mostFreqDrivenCnt.get(0))
								.put("mostFreqDriven2", this.mostFreqDriven.get(1))
								.put("mostFreqDriven2Freq", this.mostFreqDrivenCnt.get(1))
								.put("mostFreqDriven3", this.mostFreqDriven.get(2))
								.put("mostFreqDriven3Freq", this.mostFreqDrivenCnt.get(2))
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
		
		private void checkCount(String route, Long count) {
			if (this.mostFreqDriven.contains(route)) {
				for (int i = 0; i < this.mostFreqDriven.indexOf(route); i++) {
					
				}
			} else {
				
			}
			int i = 0;
			for (Long mostFreqDrivenValue : this.mostFreqDrivenCnt) {
				if (count > mostFreqDrivenValue) {
					this.mostFreqDrivenCnt.add(i, count);
					this.mostFreqDrivenCnt.removeLast();

					this.mostFreqDriven.add(i, route);
					this.mostFreqDriven.removeLast();
					break;
				}
				i++;
			}
		}
		
		private void isInMostFreq() {
			for (Long mostFreqDrivenValue : this.mostFreqDrivenCnt) {
				
			}
		}
		
		private <E> void fillLinkedList(LinkedList<E> linkedList, E value) {
			for (int i = 0; i < TOP_LIST_SIZE; i++) {
				linkedList.add(value);
			}
		}
	}
}
