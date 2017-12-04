package mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import constants.Const;

public class MqttPublishClient {
	
	public static MqttClient setupMqttConnection(String accessToken) {
		MemoryPersistence persistence = new MemoryPersistence();
		try {
			MqttClient mqttClient = new MqttClient(Const.MQTT_BROKER, Const.MQTT_CLIENT_ID(accessToken), persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			System.out.println("Connecting to broker: " + Const.MQTT_BROKER);
			connOpts.setUserName(accessToken);
			mqttClient.connect(connOpts);
			System.out.println("Connected");
			
			return mqttClient;
//			System.out.println("Publishing message: " + content);
//			MqttMessage message = new MqttMessage(content.getBytes());
//			message.setQos(qos);
//			sampleClient.publish(topic, message);
//			System.out.println("Message published");
//			sampleClient.disconnect();
//			System.out.println("Disconnected");
//			System.exit(0);
		} catch(MqttException me) {
			System.out.println("Reason: " + me.getReasonCode());
			System.out.println("Message:  " + me.getMessage());
			System.out.println("Localized message: " + me.getLocalizedMessage());
			System.out.println("Cause: " + me.getCause());
			System.out.println("Exception: " + me);
			me.printStackTrace();
		}
		
		return null;
	}
}
