package common;

import java.util.LinkedList;
import java.util.List;

public class BucketEntity {
	
	private String entity;
	private List<Double> tweets_senti;
	private int frequency;
	private int delta;
	
	public BucketEntity(String entity, int freq, int bno){
		this.entity = entity;
		tweets_senti = new LinkedList<>();
		frequency = freq;
		delta = bno;
	}


	public List<Double> getTweets_senti() {
		return tweets_senti;
	}


	public void setTweets_senti(double senti_score) {
		this.tweets_senti.add(senti_score);
	}


	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}


	public int getFrequency() {
		return frequency;
	}


	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}


	public int getDelta() {
		return delta;
	}


	public void setDelta(int maxError) {
		this.delta = maxError;
	}
	
	
}
