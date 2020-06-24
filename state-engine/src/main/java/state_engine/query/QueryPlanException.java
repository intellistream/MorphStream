package state_engine.query;
public class QueryPlanException extends Exception {
    private static final long serialVersionUID = -1980270696814984096L;
    private String message;
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

