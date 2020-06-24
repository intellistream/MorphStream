package application.constants;

public interface LeaderboardConstants {
	String PREFIX = "lb";
	int max_hz = 450000;

	interface Field extends BaseConstants.BaseField {
		String WORD = "word";
		String COUNT = "count";
		String LargeData = "LD";
	}

	interface Conf extends BaseConstants.BaseConf {
		String VOTER_THREADS = "lb.voter.threads";
		String MAINTAINER_THREADS = "lb.maintainer.threads";
		String DELETER_THREADS = "lb.deleter.threads";
		String WINDOW_THREADS = "lb.window.threads";
		String WINDOW_TRIGGER_THREADS = "lb.window_tigger.threads";
		String Leaderboard_THREADS = "lb.Leaderboard.threads";
	}

	interface Component extends BaseConstants.BaseComponent {
		String VOTER = "voter";
		String MAINTAINER = "maintainer";
		String WINDOW = "window";
		String WINDOW_TRIGGER = "trigger";
		String DELETER = "deleter";
		String Leaderboard = "Leaderboard";
	}


}
