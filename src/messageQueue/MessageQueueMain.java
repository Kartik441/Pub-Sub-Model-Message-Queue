package messageQueue;

import messageQueue.model.Message;
import messageQueue.model.Queue;
import messageQueue.model.Topic;
import messageQueue.subscribers.SleepingSubscriber;

// Objectives
// A subscriber should be able to subscribe multiple topics
// When a message is published all the subscribers must be able to function 
// in parallel
// A topic can be subscribed by multiple subscribers

public class MessageQueueMain {

	public static void main(String[] args) throws InterruptedException {
		Long startTime = System.currentTimeMillis();
		final Queue queue = new Queue();
		final Topic topic1 = queue.createTopic("t1");
		final Topic topic2 = queue.createTopic("t2");
		final SleepingSubscriber sub1 = new SleepingSubscriber("sub1", 10000);
		final SleepingSubscriber sub2 = new SleepingSubscriber("sub2", 10000);
		queue.subscribe(sub1, topic1);
		queue.subscribe(sub2, topic1);

		final SleepingSubscriber sub3 = new SleepingSubscriber("sub3", 5000);
		queue.subscribe(sub3, topic2);

		queue.publish(topic1, new Message("m1"));
		queue.publish(topic1, new Message("m2"));

		queue.publish(topic2, new Message("m3"));
		System.out.println("Published 3 messages in :: " + (System.currentTimeMillis() - startTime));

		Thread.sleep(20000);
		queue.publish(topic2, new Message("m4"));
		queue.publish(topic1, new Message("m5"));

		System.out.println("Resetting offest for subscriber 1");
		queue.resetOffset(topic1, sub1, 0);

//		System.out.println("Publish a new message in topic1");
//		
//		Scanner sc = new Scanner(System.in);
//		String myMssg = sc.next();
//		
//		queue.publish(topic1, new Message(myMssg));
//		System.out.println("Published my message ");
//		sc.close();
		
//		queue.subscribe(sub2, topic2);
//		
//		queue.publish(topic2, new Message("Message for sub2 and sub3"));

	}

}
