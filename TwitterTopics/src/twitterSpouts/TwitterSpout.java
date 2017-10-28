package twitterSpouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	String consumerKey = "x7U9qTHvjg22hR19weN1DOU26";
	String consumerSecret = "3Zpnq7WOkqhlibl1FH9a05XsjuyGvjnWzjmh0nO1LLES3aOvf1";
	String accessToken = "1153339314-KSpHSEbMm9DLAn0PBmuxQxolhtkrai0io8OJfPb";
	String accessTokenSecret = "nDjZyRJSBELCCfrePtra7j4BBWVbKNlHLRAimI5EOyLyh";

	public TwitterSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Status tweet = queue.poll();
		
	      if (tweet == null) {
	         Utils.sleep(50);
	      } else {
	         collector.emit(new Values(tweet));
	      }

	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;

		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {
			}

			@Override
			public void onException(Exception e) {
			}
		};

		twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();
		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		twitterStream.sample();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet"));

	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config config = new Config();
		config.setMaxTaskParallelism(1);
		return config;
	}

}
