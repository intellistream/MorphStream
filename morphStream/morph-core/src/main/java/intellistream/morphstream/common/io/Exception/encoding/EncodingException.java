package intellistream.morphstream.common.io.Exception.encoding;

public class EncodingException extends RuntimeException {
    public EncodingException() {
        // do nothing
    }

    public EncodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public EncodingException(String message) {
        super(message);
    }

    public EncodingException(Throwable cause) {
        super(cause);
    }
}
