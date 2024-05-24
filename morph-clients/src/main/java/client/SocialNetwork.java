package client;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.api.state.FunctionDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;

import java.util.HashMap;

public class SocialNetwork extends Client {

    @Override
    public boolean transactionUDF(Function function) {
        String txnName = function.getFunctionName();
        switch (txnName) {
            case "userLogin": {
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
            case "userProfile": {
                function.udfResult = function.getStateObject("profile").getStringValue("profile");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "getTimeLine": {
                function.udfResult = function.getStateObject("tweet").getStringValue("tweet");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet": {
                function.udfResult = function.getPara("tweet");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
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
        FunctionDAGDescription UserLogin = new FunctionDAGDescription("userLogin");
        FunctionDescription login = new FunctionDescription("login", MetaTypes.AccessType.READ);
        login.addStateObjectDescription("password", MetaTypes.AccessType.READ, "user_pwd", "password", 0);
        login.addParaName("password");
        UserLogin.addFunctionDescription("login", login);

        FunctionDAGDescription userProfile = new FunctionDAGDescription("userProfile");
        FunctionDescription profile = new FunctionDescription("profile", MetaTypes.AccessType.READ);
        profile.addStateObjectDescription("profile", MetaTypes.AccessType.READ, "user_profile", "profile", 0);
        userProfile.addFunctionDescription("profile", profile);

        FunctionDAGDescription GetTimeLine = new FunctionDAGDescription("getTimeLine");
        FunctionDescription getTimeLine = new FunctionDescription("getTimeLine", MetaTypes.AccessType.READ);
        getTimeLine.addStateObjectDescription("tweet", MetaTypes.AccessType.READ, "tweet", "tweet", 0);
        GetTimeLine.addFunctionDescription("getTimeLine", getTimeLine);

        FunctionDAGDescription PostTweet = new FunctionDAGDescription("postTweet");
        FunctionDescription postTweet = new FunctionDescription("postTweet", MetaTypes.AccessType.WRITE);
        postTweet.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 0);
        postTweet.addParaName("tweet");
        PostTweet.addFunctionDescription("postTweet", postTweet);

        this.txnDescriptions.put("userLogin", UserLogin);
        this.txnDescriptions.put("userProfile", userProfile);
        this.txnDescriptions.put("getTimeLine", GetTimeLine);
        this.txnDescriptions.put("postTweet", PostTweet);
    }
}
