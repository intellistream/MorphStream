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
            case "getProfile": {
                function.udfResult = function.getStateObject("profile").getStringValue("profile");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "getTimeLine1":
            case "getTimeLine2": {
                function.udfResult = function.getStateObject("tweet").getStringValue("tweet");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet1": {
                function.udfResult = function.getPara("tweet1");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet2": {
                function.udfResult = function.getPara("tweet2");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet3": {
                function.udfResult = function.getPara("tweet3");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet4": {
                function.udfResult = function.getPara("tweet4");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet5": {
                function.udfResult = function.getPara("tweet5");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet6": {
                function.udfResult = function.getPara("tweet6");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet7": {
                function.udfResult = function.getPara("tweet7");
                if (function.udfResult == null) {
                    throw new NullPointerException("Profile not found");
                }
                break;
            }
            case "postTweet8": {
                function.udfResult = function.getPara("tweet8");
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
        FunctionDescription profile = new FunctionDescription("getProfile", MetaTypes.AccessType.READ);
        profile.addStateObjectDescription("profile", MetaTypes.AccessType.READ, "user_profile", "profile", 0);
        userProfile.addFunctionDescription("getProfile", profile);

        FunctionDAGDescription GetTimeLine = new FunctionDAGDescription("getTimeLine");
        FunctionDescription getTimeLine1 = new FunctionDescription("getTimeLine1", MetaTypes.AccessType.READ);
        getTimeLine1.addStateObjectDescription("tweet", MetaTypes.AccessType.READ, "tweet", "tweet", 0);
        GetTimeLine.addFunctionDescription("getTimeLine1", getTimeLine1);
        FunctionDescription getTimeLine2 = new FunctionDescription("getTimeLine2", MetaTypes.AccessType.READ);
        getTimeLine2.addStateObjectDescription("tweet", MetaTypes.AccessType.READ, "tweet", "tweet", 1);
        GetTimeLine.addFunctionDescription("getTimeLine2", getTimeLine2);

        FunctionDAGDescription PostTweet = new FunctionDAGDescription("postTweet");
        FunctionDescription postTweet1 = new FunctionDescription("postTweet1", MetaTypes.AccessType.WRITE);
        postTweet1.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 0);
        postTweet1.addParaName("tweet1");
        PostTweet.addFunctionDescription("postTweet1", postTweet1);
        FunctionDescription postTweet2 = new FunctionDescription("postTweet2", MetaTypes.AccessType.WRITE);
        postTweet2.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 1);
        postTweet2.addParaName("tweet2");
        PostTweet.addFunctionDescription("postTweet2", postTweet2);
//        FunctionDescription postTweet3 = new FunctionDescription("postTweet3", MetaTypes.AccessType.WRITE);
//        postTweet3.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 2);
//        postTweet3.addParaName("tweet3");
//        PostTweet.addFunctionDescription("postTweet3", postTweet3);
//        FunctionDescription postTweet4 = new FunctionDescription("postTweet4", MetaTypes.AccessType.WRITE);
//        postTweet4.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 3);
//        postTweet4.addParaName("tweet4");
//        PostTweet.addFunctionDescription("postTweet4", postTweet4);
//        FunctionDescription postTweet5 = new FunctionDescription("postTweet5", MetaTypes.AccessType.WRITE);
//        postTweet5.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 4);
//        postTweet5.addParaName("tweet5");
//        PostTweet.addFunctionDescription("postTweet5", postTweet5);
//        FunctionDescription postTweet6 = new FunctionDescription("postTweet6", MetaTypes.AccessType.WRITE);
//        postTweet6.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 5);
//        postTweet6.addParaName("tweet6");
//        PostTweet.addFunctionDescription("postTweet6", postTweet6);
//        FunctionDescription postTweet7 = new FunctionDescription("postTweet7", MetaTypes.AccessType.WRITE);
//        postTweet7.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 6);
//        postTweet7.addParaName("tweet7");
//        PostTweet.addFunctionDescription("postTweet7", postTweet7);
//        FunctionDescription postTweet8 = new FunctionDescription("postTweet8", MetaTypes.AccessType.WRITE);
//        postTweet8.addStateObjectDescription("tweet", MetaTypes.AccessType.WRITE, "tweet", "tweet", 7);
//        postTweet8.addParaName("tweet8");
//        PostTweet.addFunctionDescription("postTweet8", postTweet8);

        this.txnDescriptions.put("userLogin", UserLogin);
        this.txnDescriptions.put("userProfile", userProfile);
        this.txnDescriptions.put("getTimeLine", GetTimeLine);
        this.txnDescriptions.put("postTweet", PostTweet);
    }
}
