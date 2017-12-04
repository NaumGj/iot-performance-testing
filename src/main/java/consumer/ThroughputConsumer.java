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

public class ThroughputConsumer {
	
	private static final String GROUP_ID = "throughput";
	
	private static Scanner in;
	
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	public static void main(String[] argv) throws Exception{
		in = new Scanner(System.in);

		MqttClient mqttClient = MqttPublishClient.setupMqttConnection(Const.THROUGHPUT_ACCESS_TOKEN);
		ConsumerThread consumerRunnable = new ConsumerThread(Const.THROUGHPUT_TOPIC, GROUP_ID, mqttClient);
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
			
			LinkedList<Long> throughputs = new LinkedList<Long>();
			String messageContent = new JSONObject().toString();
			MqttPublishRunnable mqttPublishRunnable = new MqttPublishRunnable(this.mqttClient, messageContent);
			scheduler.scheduleAtFixedRate(mqttPublishRunnable, 1, Const.MQTT_REPORT_FREQ, TimeUnit.SECONDS);
			//Start processing messages
			try {
				while (true) {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//					if (records.isEmpty()) {
//						messageContent = new JSONObject().put("throughput", 0).toString();
//						mqttPublishRunnable.setMessageContent(messageContent);
//					}
					for (ConsumerRecord<String, String> record : records) {
//						System.out.println("KEY: " + record.key());
//						System.out.println("VALUE: " + record.value());
						while (throughputs.size() > 10) {
							throughputs.removeLast();
						}
						throughputs.addFirst(Long.valueOf(record.value()));
						
						Long throughput = getCorrectThroughput(throughputs);
						
						messageContent = new JSONObject()
								.put("throughput", throughput).toString();
						mqttPublishRunnable.setMessageContent(messageContent);
//						System.out.println(throughput);
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

