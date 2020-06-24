package application.constants;

public interface VoterSStoreExampleConstants {
	String PREFIX = "lb";
	String TABLENAME_CONTESTANTS = "contestants";
	String TABLENAME_AREA_CODE_STATE = "area_code_state";
	String TABLENAME_VOTES = "votes";
	int VOTE_THRESHOLD = 1;//org: 1000
	int MAX_VOTES = 1;
	int NUM_CONTESTANTS = 25;
	// Initialize some common constants and variables
	String CONTESTANT_NAMES_CSV = "Jann Arden,Micah Barnes,Justin Bieber,Jim Bryson,Michael Buble," +
			"Leonard Cohen,Celine Dion,Nelly Furtado,Adam Gontier,Emily Haines," +
			"Avril Lavigne,Ashley Leggat,Eileen McGann,Sarah McLachlan,Joni Mitchell," +
			"Mae Moore,Alanis Morissette,Emilie Mover,Anne Murray,Sam Roberts," +
			"Serena Ryder,Tamara Sandor,Nicholas Scribner,Shania Twain,Neil Young";
	// potential return codes
	long VOTE_SUCCESSFUL = 0;
	long ERR_INVALID_CONTESTANT = 1;
	long ERR_VOTER_OVER_VOTE_LIMIT = 2;
	long ERR_NO_VOTE_FOUND = 3;
	long DELETE_CONTESTANT = 4;
	long WINDOW_SUCCESSFUL = 5;
	long ERR_NOT_ENOUGH_CONTESTANTS = 6;
	long DELETE_SUCCESSFUL = 7;

	interface Field extends BaseConstants.BaseField {
		String voteId = "voteId";
		String phoneNumber = "phoneNumber";
		String state = "state";
		String contestantNumber = "contestantNumber";
		String timestamp = "timestamp";
		String ts = "ts";
	}

	interface Stream extends BaseConstants.BaseStream {
		String trendingLeaderboard = "trendingLeaderboardStream";

	}
}
