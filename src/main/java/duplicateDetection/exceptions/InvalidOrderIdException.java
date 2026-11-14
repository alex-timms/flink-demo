package duplicateDetection.exceptions;

public class InvalidOrderIdException extends Exception {

    public InvalidOrderIdException() {
        super("Invalid order ID found");
    }

    public InvalidOrderIdException(String orderId) {
        super("Invalid order ID found: " + orderId);
    }
}
