package sesame.components.exception;

/**
 * Custom exception
 */
public class UnhandledCaseException extends Exception {
    private static final long serialVersionUID = 8308956830238052858L;

    public UnhandledCaseException(String message) {
        super(message);
    }
}
