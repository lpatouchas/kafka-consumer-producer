package gr.patouchas.kafka.deprecated;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class SimpleExample {
	private List<String> m_replicaBrokers = new ArrayList<String>();

	public SimpleExample() {
		this.m_replicaBrokers = new ArrayList<String>();
	}

	public void run(long a_maxReads, final String a_topic, final int a_partition, final List<String> a_seedBrokers, final int a_port)
					throws Exception {
		// find the meta data about the topic and partition we are interested in
		//
		final PartitionMetadata metadata = this.findLeader(a_seedBrokers, a_port, a_topic, a_partition);
		if (metadata == null) {
			System.out.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out.println("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		final String clientName = "Client_" + a_topic + "_" + a_partition;

		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
		long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

		int numErrors = 0;
		while (a_maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
			}
			/**
			 * Reading data
			 */
			final FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(a_topic, a_partition, readOffset, 100000).build();
			final FetchResponse fetchResponse = consumer.fetch(req);

			/**
			 * SimpleConsumer does not handle lead broker failures, you have to handle it once the fetch returns an
			 * error, we log the reason, close the consumer then try to figure out who the new leader is
			 */
			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				final short code = fetchResponse.errorCode(a_topic, a_partition);
				System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				if (numErrors > 5) {
					break;
				}
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for the last element to reset
					readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = this.findNewLeader(leadBroker, a_topic, a_partition, a_port);
				continue;
			}
			// End Error handling

			/**
			 * Reading data cont.
			 */
			numErrors = 0;

			long numRead = 0;
			for (final MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
				final long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				final ByteBuffer payload = messageAndOffset.message().payload();

				final byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
				numRead++;
				a_maxReads--;
			}

			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException ie) {
				}
			}
		}
		if (consumer != null) {
			consumer.close();
		}
	}

	/**
	 * Defines where to start reading data from Helpers Available: kafka.api.OffsetRequest.EarliestTime() => finds the
	 * beginning of the data in the logs and starts streaming from there kafka.api.OffsetRequest.LatestTime() => will
	 * only stream new messages
	 * 
	 * @param consumer
	 * @param topic
	 * @param partition
	 * @param whichTime
	 * @param clientName
	 * @return
	 */
	public static long getLastOffset(final SimpleConsumer consumer, final String topic, final int partition, final long whichTime,
					final String clientName) {
		final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		final Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		final kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
						clientName);
		final OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		final long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	/**
	 * Uses the findLeader() logic we defined to find the new leader, except here we only try to connect to one of the
	 * replicas for the topic/partition. This way if we can’t reach any of the Brokers with the data we are interested
	 * in we give up and exit hard.
	 * 
	 * @param a_oldLeader
	 * @param a_topic
	 * @param a_partition
	 * @param a_port
	 * @return
	 * @throws Exception
	 */
	private String findNewLeader(final String a_oldLeader, final String a_topic, final int a_partition, final int a_port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			final PartitionMetadata metadata = this.findLeader(this.m_replicaBrokers, a_port, a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give ZooKeeper a second to recover
				// second time, assume the broker did recover before failover, or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException ie) {
				}
			}
		}
		System.out.println("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	/**
	 * Query a live broker to find out leader information and replica information for a given topic and partition
	 * 
	 * @param a_seedBrokers
	 * @param a_port
	 * @param a_topic
	 * @param a_partition
	 * @return
	 */
	private PartitionMetadata findLeader(final List<String> a_seedBrokers, final int a_port, final String a_topic, final int a_partition) {
		PartitionMetadata returnMetaData = null;
		for (final String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup"); // broker_host,
																								// broker_port, timeout,
																								// buffer_size,
																								// client_id
				final List<String> topics = new ArrayList<String>();
				topics.add(a_topic);
				final TopicMetadataRequest req = new TopicMetadataRequest(topics);
				final kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				// call to topicsMetadata() asks the Broker you are connected to for all the details about the topic we
				// are interested in
				final List<TopicMetadata> metaData = resp.topicsMetadata();
				// loop on partitionsMetadata iterates through all the partitions until we find the one we want.
				for (final TopicMetadata item : metaData) {
					for (final PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break;
						}
					}
				}
			} catch (final Exception e) {
				System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", " + a_partition + "] Reason: "
								+ e);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		// add replica broker info to m_replicaBrokers
		if (returnMetaData != null) {
			this.m_replicaBrokers.clear();
			for (final kafka.cluster.Broker replica : returnMetaData.replicas()) {
				this.m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
}
