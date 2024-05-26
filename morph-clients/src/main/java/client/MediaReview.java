package client;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.api.state.FunctionDescription;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;

import java.util.HashMap;

public class MediaReview extends Client {

    @Override
    public boolean transactionUDF(Function function) {
        String txnName = function.getFunctionName();
        switch (txnName) {
            case "login": {
                StateObject userState = function.getStateObject("password");
                String password = userState.getStringValue("password");
                String inputPassword = (String) function.getPara("password");
                if (password.equals(inputPassword)) {
                    function.udfResult = true;
                } else {
                    function.udfResult = false;
                }
                break;
            }
            case "rate": {
                function.udfResult = function.getPara("rate");
                break;
            }
            case "review": {
                function.udfResult = function.getPara("review");
                break;
            }
        }
        return true;
    }

    @Override
    public Result postUDF(long bid, String txnFlag, HashMap<String, Function> FunctionMap) {
        return new Result(bid);
    }

    @Override
    public void defineFunction() {
        FunctionDAGDescription userLogin = new FunctionDAGDescription("userLogin");
        FunctionDescription login = new FunctionDescription("login", MetaTypes.AccessType.READ);
        login.addStateObjectDescription("password", MetaTypes.AccessType.READ, "user_pwd", "password", 0);
        login.addParaName("password");
        userLogin.addFunctionDescription("login", login);

        FunctionDAGDescription ratingMovie = new FunctionDAGDescription("ratingMovie");
        FunctionDescription rate = new FunctionDescription("rate", MetaTypes.AccessType.WRITE);
        rate.addStateObjectDescription("rate", MetaTypes.AccessType.WRITE, "movie_rating", "rate", 0);
        rate.addParaName("rate");
        ratingMovie.addFunctionDescription("rate", rate);

        FunctionDAGDescription reviewMovie = new FunctionDAGDescription("reviewMovie");
        FunctionDescription review = new FunctionDescription("review", MetaTypes.AccessType.WRITE);
        review.addStateObjectDescription("review", MetaTypes.AccessType.WRITE, "movie_review", "review", 0);
        review.addParaName("review");
        reviewMovie.addFunctionDescription("review", review);

        this.txnDescriptions.put("userLogin", userLogin);
        this.txnDescriptions.put("ratingMovie", ratingMovie);
        this.txnDescriptions.put("reviewMovie", reviewMovie);
    }
}
