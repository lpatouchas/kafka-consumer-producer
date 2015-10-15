package gr.patouchas.kafka.client;

public class Main {

	private static final String TOPIC = "partitioned-topic";// "cross-machine-topic";// "test-rep-topic";

	private static final String GROUP = "partitioned-group";// "multi-machine-group";//

	public static void main(final String[] args) throws InterruptedException {

		final Consumer consumer1 = new Consumer(TOPIC, GROUP, "Con1");
		final Thread consumer1Thread = new Thread(consumer1);

		// Second consumer thread to validate that consumers
		// in the same group does not consume twice the message
		//
		final Consumer consumer2 = new Consumer(TOPIC, GROUP, "Con2");
		final Thread consumer2Thread = new Thread(consumer2);

		final Consumer consumer3 = new Consumer(TOPIC, GROUP, "Con3");
		final Thread consumer3Thread = new Thread(consumer3);

		final KProducer kproducer = new KProducer(TOPIC);

		// Different producer implementation

		// final Producer producer = new Producer(TOPIC);
		// producer.run(1, "key", "new message");

		consumer1Thread.start();
		// consumer2Thread.start();
		// consumer3Thread.start();

		// kproducer.run(50, "zKey", "custom_message");

	}
}