package common.model.log;
public class NotificationDetails {
    private String to;
    private Severity severity;
    private String message;
    public NotificationDetails(String to, Severity severity, String message) {
        this.to = to;
        this.severity = severity;
        this.message = message;
    }
    public String getTo() {
        return to;
    }
    public Severity getSeverity() {
        return severity;
    }
    public String getMessage() {
        return message;
    }
}