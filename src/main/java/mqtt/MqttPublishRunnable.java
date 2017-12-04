package mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import constants.Const;

public class MqttPublishRunnable implements Runnable {

	private MqttClient mqttClient;
	private String messageContent;

	public MqttPublishRunnable(MqttClient mqttClient, String messageContent) {
		this.mqttClient = mqttClient;
		this.messageContent = messageContent;
	}

	@Override
	public void run() {
		System.out.println("Publishing message: " + messageContent);
		MqttMessage message = new MqttMessage(messageContent.getBytes());
		message.setQos(Const.MQTT_MESSAGE_QOS);
		try {
			this.mqttClient.publish(Const.MQTT_PUBLISH_TOPIC, message);
		} catch (MqttException me) {
			System.out.println("Reason: " + me.getReasonCode());
			System.out.println("Message: " + me.getMessage());
			System.out.println("Localized message: " + me.getLocalizedMessage());
			System.out.println("Cause: " + me.getCause());
			System.out.println("Exception: " + me);
			me.printStackTrace();
		}
		System.out.println("Message published");
	}
	
	public String getMessageContent() {
		return messageContent;
	}

	public void setMessageContent(String messageContent) {
		this.messageContent = messageContent;
	}

}