package messageQueue.model;


public class Message {
	private String message;
	
	public Message(String message) {
		this.message = message;
	}
	
	public String getMessage() {
		return this.message;
	}
	
	public void setMessage(String message) {
		this.message = message;
	}
}
