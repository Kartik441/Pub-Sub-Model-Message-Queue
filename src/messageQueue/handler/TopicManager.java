package messageQueue.handler;

import java.util.HashMap;
import java.util.Map;

import messageQueue.model.Topic;
import messageQueue.model.TopicSubscriber;

public class TopicManager {
	private final Topic topic;
    private final Map<String, SubscriberWorker> subscriberWorkers;

    public TopicManager(final Topic topic) {
        this.topic = topic;
        subscriberWorkers = new HashMap<>();
    }

    public void publish() {
        for (TopicSubscriber topicSubscriber:topic.getSubscribers()) {
            startSubsriberWorker(topicSubscriber);
        }
    }

    public void startSubsriberWorker(final TopicSubscriber topicSubscriber) {
        final String subscriberId = topicSubscriber.getSubscriber().getId();
        if (!subscriberWorkers.containsKey(subscriberId)) {
            final SubscriberWorker subscriberWorker = new SubscriberWorker(topic, topicSubscriber);
            subscriberWorkers.put(subscriberId, subscriberWorker);
            new Thread(subscriberWorker).start();
        }
        final SubscriberWorker subscriberWorker = subscriberWorkers.get(subscriberId);
        subscriberWorker.wakeUpIfNeeded();
    }

}
