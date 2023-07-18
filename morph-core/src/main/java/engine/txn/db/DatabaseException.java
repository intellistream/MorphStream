package engine.txn.db;

public class DatabaseException extends Exception {
    private static final long serialVersionUID = -5696965345839589596L;
    private final String message;

    public DatabaseException(String message) {
        this.message = message;
    }

    public DatabaseException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
