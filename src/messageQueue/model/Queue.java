package messageQueue.model;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import messageQueue.handler.TopicManager;
import messageQueue.publicInterface.Subscriber;

public class Queue {
	private final Map<String, TopicManager> topicProcessors;

	public Queue() {
		this.topicProcessors = new HashMap<>();
	}

	public Topic createTopic(final String topicName) {
		final Topic topic = new Topic(topicName, UUID.randomUUID().toString());
		TopicManager topicManager = new TopicManager(topic);
		topicProcessors.put(topic.getTopicId(), topicManager);
		System.out.println("Created topic: " + topic.getTopicName());
		return topic;
	}

	public void subscribe(final Subscriber subscriber, final Topic topic) {
		topic.addSubscriber(new TopicSubscriber(subscriber));
		System.out.println(subscriber.getId() + " subscribed to topic: " + topic.getTopicName());
	}

	public void publish( final Topic topic,  final Message message) {
		topic.addMessage(message);
		System.out.println(message.getMessage() + " published to topic: " + topic.getTopicName());
		new Thread(() -> topicProcessors.get(topic.getTopicId()).publish()).start();
	}

	public void resetOffset(final Topic topic, final Subscriber subscriber,
			final Integer newOffset) {
		for (TopicSubscriber topicSubscriber : topic.getSubscribers()) {
			if (topicSubscriber.getSubscriber().equals(subscriber)) {
				topicSubscriber.getOffset().set(newOffset);
				System.out.println(topicSubscriber.getSubscriber().getId() + " offset reset to: " + newOffset);
				new Thread(() -> topicProcessors.get(topic.getTopicId()).startSubsriberWorker(topicSubscriber)).start();
				break;
			}
		}
	}

}
