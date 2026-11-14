package duplicateDetection.exceptions;

public class DuplicateOrderIdException extends Exception {

    public DuplicateOrderIdException() {
        super("Duplicate order ID found");
    }
}
