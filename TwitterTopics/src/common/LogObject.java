package common;

public class LogObject {
	
	String hashTag;
	double score;
	int freq;
	 
	public LogObject(String h, double s, int f) {
		// TODO Auto-generated constructor stub
		hashTag = h;
		score = s;
		freq = f;
	}

	public int getFreq() {
		return freq;
	}

	public void setFreq(int freq) {
		this.freq = freq;
	}

	public String getHashTag() {
		return hashTag;
	}

	public void setHashTag(String hashTag) {
		this.hashTag = hashTag;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	

}
