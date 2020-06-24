package sesame.components.exception;

/**
 * Custom exception
 */
public class InvalidIDException extends Exception {
    private static final long serialVersionUID = 4410379836219437416L;

    public InvalidIDException(String message) {
        super(message);
    }
}
