package storage.table;
public class SchemaException extends Exception {
    private static final long serialVersionUID = -4581159206803128556L;
    private String message;
    public SchemaException(String message) {
        this.message = message;
    }
    public SchemaException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }
    @Override
    public String getMessage() {
        return this.message;
    }
}
