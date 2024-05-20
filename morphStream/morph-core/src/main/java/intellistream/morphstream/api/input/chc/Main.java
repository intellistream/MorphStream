package intellistream.morphstream.api.input.chc;


public class Main {
    public static void main(String[] args) {
        // implement an example for CHC
        DBManager manager = DBManager.getInstance();
        Request request = new Request(0, 0, 0, 0, 0, Request.Operation.READ, new Pattern(Pattern.StateType.CROSS_FLOW, Pattern.AccessType.READ));
        manager.submit(request);
    }
}
