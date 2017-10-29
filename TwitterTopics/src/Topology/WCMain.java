package Topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import twitterBolts.CountBolt;
import twitterBolts.HashtagCount;
import twitterBolts.HashtagReader;
import twitterBolts.OutputBolt;
import twitterBolts.WordBolt;
import twitterSpouts.SentenceSpout;
import twitterSpouts.TwitterSpout;

public class WCMain {
	private static final String TWITTER_SPOUT_ID = "twitter-spout";
	private static final String HASHTAG_BOLT_ID = "hashtag-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String OUTPUT_BOLT_ID = "output-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	
	public static void main(String args[]) {
		TwitterSpout ss = new TwitterSpout();
		HashtagReader wb = new HashtagReader();
		HashtagCount cb = new HashtagCount();
		OutputBolt ob = new OutputBolt();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(TWITTER_SPOUT_ID, ss);
		//The shuffleGrouping() method tells Storm to shuffle tuples emitted by the SentenceSpout class 
		//and distribute them evenly among instances of the SplitSentenceBolt object.
		builder.setBolt(HASHTAG_BOLT_ID, wb).shuffleGrouping(TWITTER_SPOUT_ID);
		//Here, we use the fieldsGrouping() method of the BoltDeclarer class to ensure that 
		//all tuples containing the same "word" value get routed to the same WordCountBolt instance.
		builder.setBolt(COUNT_BOLT_ID, cb).fieldsGrouping(HASHTAG_BOLT_ID, new Fields("word"));
		
		builder.setBolt(OUTPUT_BOLT_ID, ob).globalGrouping(COUNT_BOLT_ID);
		
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.
				createTopology());
		Utils.sleep(10);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

	

}
