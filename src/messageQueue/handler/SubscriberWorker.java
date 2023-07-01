package messageQueue.handler;

import messageQueue.model.Message;
import messageQueue.model.Topic;
import messageQueue.model.TopicSubscriber;

public class SubscriberWorker implements Runnable {
	private final Topic topic;
	private final TopicSubscriber topicSubscriber;

	public SubscriberWorker(final Topic topic,final TopicSubscriber topicSubscriber) {
		this.topic = topic;
		this.topicSubscriber = topicSubscriber;
	}

	@Override
	public void run() {
		synchronized (topicSubscriber) {
			while(true) {
				int curOffset = topicSubscriber.getOffset().get();
				System.out.println("Current thread "+Thread.currentThread().getName());
				while (curOffset >= topic.getMessages().size()) {
					System.out.println("Waiting....");
					try {
						topicSubscriber.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				Message message = topic.getMessages().get(curOffset);
				try {
					topicSubscriber.getSubscriber().consume(message);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// We cannot just increment here since subscriber offset can be reset while it
				// is consuming. So, after
				// consuming we need to increase only if it was previous one.
				topicSubscriber.getOffset().compareAndSet(curOffset, curOffset + 1);
				System.out.println("Raised offest");
			} 
		}
	}

	synchronized public void wakeUpIfNeeded() {
		synchronized (topicSubscriber) {
			topicSubscriber.notify();
		}
	}
}
