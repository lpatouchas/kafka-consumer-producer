package gr.patouchas.kafka.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer {
	private final kafka.javaapi.producer.Producer<String, String> producer;
	private final Properties props = new Properties();

	private final String topic;

	public Producer(final String topic) {
		this.props.put("metadata.broker.list", "170.118.146.163:9092,170.118.146.163:9093,170.118.146.163:9094");
		this.props.put("serializer.class", "kafka.serializer.StringEncoder");
		this.props.put("request.required.acks", "1");
		this.producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(this.props));
		this.topic = topic;
	}

	public void run(final int times, final String key, final String msg) {
		final List<KeyedMessage<String, String>> listOfMessages = new ArrayList<>();
		for (int i = 1; i <= times; i++) {
			final String messageStr = new String(msg + "_" + i);
			listOfMessages.add(new KeyedMessage<String, String>(this.topic, key, messageStr));
		}
		this.producer.send(listOfMessages);
		this.producer.close();
	}
}
