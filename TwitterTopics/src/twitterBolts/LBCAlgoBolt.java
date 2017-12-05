package twitterBolts;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import common.BucketEntity;


public class LBCAlgoBolt extends BaseRichBolt {
	
	private int bucketID = 0;
	private int threshold = 0;
	private int bucketSize = 0;
	private OutputCollector collector;
	private Map<String, BucketEntity> bucket = null; 
	

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		//System.out.println("----------------------- LBC ALGO------------------");
		String key = tuple.getStringByField("key");
		double tweet_senti = tuple.getDoubleByField("senti_score");
		if(!bucket.containsKey(key)) {
			BucketEntity entity = new BucketEntity(key, 1, (bucketID-1));
			entity.setTweets_senti(tweet_senti);
			bucket.put(key, entity);
		} else {
			BucketEntity prev_entity = bucket.get(key);
			prev_entity.setFrequency(prev_entity.getFrequency()+1);
			prev_entity.setTweets_senti(tweet_senti);
			bucket.put(key, prev_entity);
		}
		threshold++;
		System.out.println("bucket id:"+bucketID+"\t threshold:"+threshold);
		
		if(threshold%bucketSize == 0) { 
			
			for(String hshtg : bucket.keySet()) {
				BucketEntity temp = bucket.get(hshtg);	
				//System.out.println("Checking before emitting:"+hshtg+"\t frequency:"+temp.getFrequency());
				if(temp.getFrequency()+temp.getDelta() > bucketID) { 
					System.out.println("emitting ------------------- :"+temp.getEntity());
					double senti_tot = 0;
					for(double t: temp.getTweets_senti()) {
						senti_tot+= t;
					}
					double final_senti = senti_tot/(double)temp.getFrequency();
					collector.emit(new Values(temp.getEntity(), (temp.getFrequency()), final_senti));
				} else { 
					bucket.remove(hshtg);
				}
			}
			
			bucketID++;
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		bucket = new ConcurrentHashMap<>();
		bucketID = 1;
		bucketSize = 500;
		threshold = 0;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("key", "true_count", "senti_score"));
		
	}
	
	

}
