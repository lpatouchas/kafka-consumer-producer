package eu.iri.ace.kafka.deprecated;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class DepConsumer implements Runnable {
	private final String topic;
	final Properties props = new Properties();

	public DepConsumer(final String topic, final String group) {

		this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "170.118.146.163:9092,170.118.146.163:9093,170.118.146.163:9094"); // Configure
																																	// ZooKeeper
																																	// location
		this.props.put(ConsumerConfig.GROUP_ID_CONFIG, group); // Configure consumer group
		// this.props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "1000");
		this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		// this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
		this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		this.props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin");
		this.topic = topic;
	}

	@Override
	public void run() {
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(this.props);
		consumer.subscribe(this.topic);
		final boolean isRunning = true;
		while (isRunning) {
			final Map<String, ConsumerRecords<String, String>> records = consumer.poll(100);
			this.process(records);
		}
		// consumer.close();
	}

	private Map<TopicPartition, Long> process(final Map<String, ConsumerRecords<String, String>> records) {
		final Map<TopicPartition, Long> processedOffsets = new HashMap<TopicPartition, Long>();
		if (null != records) {
			for (final Entry<String, ConsumerRecords<String, String>> recordMetadata : records.entrySet()) {
				final List<ConsumerRecord<String, String>> recordsPerTopic = recordMetadata.getValue().records();
				for (int i = 0; i < recordsPerTopic.size(); i++) {
					final ConsumerRecord<String, String> record = recordsPerTopic.get(i);
					// process record
					try {
						processedOffsets.put(record.topicAndPartition(), record.offset());
					} catch (final Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		return processedOffsets;
	}
}
