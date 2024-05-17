package intellistream.morphstream.api.input.chc.example;

import java.intellistream.chc.common.dao.Pattern;
import java.intellistream.chc.common.dao.Request;
import java.intellistream.chc.database.manage.DBManager;

public class Main {
    public static void main(String[] args) {
        // implement an example for CHC
        DBManager manager = DBManager.getInstance();
        Request request = new Request(0, 0, 0, 0, 0, Request.Operation.READ, new Pattern(Pattern.StateType.CROSS_FLOW, Pattern.AccessType.READ));
        manager.submit(request);
    }
}
