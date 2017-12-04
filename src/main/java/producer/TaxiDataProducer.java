package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import constants.Const;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TaxiDataProducer {
	
	private static final Integer SLEEP_TIME = 10;
	
	public static void main(String[] argv) throws Exception {
		Properties producerConfig = configureProducerProperties();

		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		InputStream is = classloader.getResourceAsStream("nyc-taxi-data.csv");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		
		Producer<String, String> producer = new KafkaProducer<String, String>(producerConfig);
		
		produceEvents(producer, reader);
	}
	
	private static Properties configureProducerProperties() {
		Properties props = new Properties();
		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Const.KAFKA_IP + ":9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
		
		return props;
	}
	
	private static void produceEvents(Producer<String, String> producer, BufferedReader reader) {
		Long counter = 0L;
//		System.out.println("PRODUCE EVENTS");
		try {
//			System.out.println("READING");
//			System.out.println(reader.readLine());
		    String line;
		    //TODO: once file is read, read it again
		    //TODO: add random noise to data
		    while ((line = reader.readLine()) != null) {
		        String value = line + "," + String.valueOf(System.currentTimeMillis());
				ProducerRecord<String, String> rec = new ProducerRecord<String, String>(Const.TAXI_DATA_TOPIC, Const.DUMMY_KEY, value);
				producer.send(rec);
				
				if (counter % 100 == 0)  {
					System.out.println(counter);
				}
				
				counter++;
				TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
		    }

		} catch (IOException e) {
		    e.printStackTrace();
		} catch (InterruptedException e) {
			System.err.println("Sleep exception");
			e.printStackTrace();
		} finally {
		    try {
		        reader.close();
		    } catch (IOException e) {
		        e.printStackTrace();
		    }
		}
		
//		System.out.println("END");
		producer.close();
	}
}
