package gr.patouchas.kafka.client;

public class Main {

	private static final String TOPIC = "test-rep-topic";

	private static final String GROUP = "test-consumer-group";

	public static void main(final String[] args) throws InterruptedException {

		final Consumer consumer1 = new Consumer(TOPIC, GROUP);
		final Thread consumer1Thread = new Thread(consumer1);
		consumer1Thread.start();

		// Second consumer thread to validate that consumers
		// in the same group does not consume twice the message

		// final Consumer consumer2 = new Consumer(TOPIC, GROUP);
		// final Thread consumer2Thread = new Thread(consumer2);
		// consumer2Thread.start();

		// Different producer implementation

		// final Producer producer = new Producer(TOPIC);
		// producer.run(1, "key", "new message");

		final KProducer kproducer = new KProducer(TOPIC);
		kproducer.run(10, "akey", "a other test message");

	}
}