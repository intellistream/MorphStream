package intellistream.morphstream.engine.txn.storage.store.disk;

/**
 * Exception thrown for errors while paging.
 * <p>
 * YOU SHOULD NOT NEED TO CHANGE ANY OF THE CODE IN THIS PACKAGE.
 */
public class PageException extends RuntimeException {
    private static final long serialVersionUID = -2765706135666534861L;

    public PageException() {
        super();
    }

    public PageException(String message) {
        super(message);
    }
}

