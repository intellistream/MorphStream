package common.io.Exception.encoding;

public class DecodingException extends RuntimeException{
    private static final long serialVersionUID = -8632392900655017028L;

    public DecodingException() {
        // do nothing
    }

    public DecodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public DecodingException(String message) {
        super(message);
    }

    public DecodingException(Throwable cause) {
        super(cause);
    }
}
