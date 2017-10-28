package twitterBolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashtagReader extends BaseRichBolt{
	
	private OutputCollector collector;
	//private Map<String, Integer> counterTags;

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Status tweet = (Status) tuple.getValueByField("tweet");
	      for(HashtagEntity hashtage : tweet.getHashtagEntities()) {
	         System.out.println("Hashtag: " + hashtage.getText());
	         this.collector.emit(new Values(hashtage.getText()));
	      }
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		collector = arg2;
		//counterTags = new HashMap<>();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("hashtag"));
		
	}

}
