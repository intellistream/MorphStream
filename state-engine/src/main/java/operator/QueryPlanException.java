package operator;
public class QueryPlanException extends Exception {
    private static final long serialVersionUID = -1980270696814984096L;
    private final String message;
    public QueryPlanException(String message) {
        this.message = message;
    }
    public QueryPlanException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }
    @Override
    public String getMessage() {
        return this.message;
    }
}

