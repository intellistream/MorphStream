package state_engine.storage.datatype;
/**
 * Exception that is thrown for DataBox errors such as type mismatches
 */
public class DataBoxException extends RuntimeException {
    private static final long serialVersionUID = 4036154007954899267L;
    public DataBoxException() {
        super();
    }
    public DataBoxException(String message) {
        super(message);
    }
}
