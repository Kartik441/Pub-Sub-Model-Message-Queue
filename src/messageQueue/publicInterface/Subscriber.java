package messageQueue.publicInterface;

import messageQueue.model.Message;

public interface Subscriber {
	String getId();
    void consume(Message message) throws InterruptedException;
}
