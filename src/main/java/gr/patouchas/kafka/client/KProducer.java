package gr.patouchas.kafka.client;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KProducer {

	private final KafkaProducer<String, String> kProducer;

	private final Properties props = new Properties();

	private final String topic;

	public KProducer(final String topic) {
		this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "170.118.146.163:9092,170.118.146.163:9093,170.118.146.163:9094");
		this.props.put(ProducerConfig.ACKS_CONFIG, "1");
		this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		this.topic = topic;
		this.kProducer = new KafkaProducer<>(this.props);
	}

	public void run(final int times, final String key, final String msg) {

		for (int i = 1; i <= times; i++) {
			final String messageStr = new String(msg + "_" + i);

			final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(this.topic, key, messageStr);
			this.kProducer.send(producerRecord, (metadata, e) -> {
				if (e != null) {
					e.printStackTrace();
				}
				System.out.println("The offset of the record we just sent is: " + metadata.offset());
			});
			// this.kProducer.close();
		}
	}
}
