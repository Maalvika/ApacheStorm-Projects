package twitterBolts.parallel;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import common.BucketEntity;

public class LBCAlgoBolt_small extends BaseRichBolt {

	private int bucketID = 0;
	private int threshold = 0;
	private int bucketSize = 0;
	private OutputCollector collector;
	private Map<String, BucketEntity> bucket = null;
	private Map<Integer, Set<String>> top_value = null;
	private Timer timer;

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		// System.out.println("----------------------- LBC
		// ALGO------------------");
		String key = tuple.getStringByField("key");
		double tweet_senti = tuple.getDoubleByField("senti_score");
		if (!bucket.containsKey(key)) {
			BucketEntity entity = new BucketEntity(key, 1, (bucketID - 1));
			entity.setTweets_senti(tweet_senti);
			bucket.put(key, entity);
		} else {
			BucketEntity prev_entity = bucket.get(key);
			prev_entity.setFrequency(prev_entity.getFrequency() + 1);
			prev_entity.setTweets_senti(tweet_senti);
			bucket.put(key, prev_entity);
		}
		if (!top_value.containsKey(bucket.get(key).getFrequency())) {
			HashSet<String> temp = new HashSet<>();
			temp.add(bucket.get(key).getEntity());
			top_value.put(bucket.get(key).getFrequency(), temp);
		} else {
			Set<String> temp = top_value.get(bucket.get(key).getFrequency());
			if (top_value.containsKey(bucket.get(key).getFrequency() - 1)) {
				Set<String> prev_temp = top_value.get(bucket.get(key).getFrequency() - 1);
				prev_temp.remove(bucket.get(key).getEntity());
				top_value.put(bucket.get(key).getFrequency() - 1, prev_temp);
			}
			temp.add(bucket.get(key).getEntity());
			top_value.put(bucket.get(key).getFrequency(), temp);
		}
		threshold++;
		System.out.println("bucket id:" + bucketID + "\t threshold:" + threshold);

		if (threshold % bucketSize == 0) {

			for (String hshtg : bucket.keySet()) {
				BucketEntity temp = bucket.get(hshtg);
				// System.out.println("Checking before emitting:"+hshtg+"\t
				// frequency:"+temp.getFrequency());
				if (temp.getFrequency() + temp.getDelta() <= bucketID) {
					Set<String> temp_tag = top_value.get(temp.getFrequency());
					temp_tag.remove(temp.getEntity());
					top_value.put(temp.getFrequency(), temp_tag);
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
		top_value = new TreeMap<>(Collections.reverseOrder());
		timer = new Timer();
		timer.scheduleAtFixedRate(new java.util.TimerTask() {
			@Override
			public void run() {
				// your code here
				emit_top();
			}
		}, 10000, 8000);
		bucketID = 1;
		bucketSize = 500;
		threshold = 0;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("key", "freq", "senti_score"));

	}

	private void emit_top() {

		if (!top_value.isEmpty()) {
			int cnt = 1;
			for (Integer f : top_value.keySet()) {
				for (String hash : top_value.get(f)) {
					BucketEntity emit = bucket.get(hash);
					double senti_tot = 0;
					for (double t : emit.getTweets_senti()) {
						senti_tot += t;
					}
					collector.emit(new Values(emit.getEntity(), emit.getFrequency(),
							senti_tot / (double) emit.getFrequency()));

					if (cnt >= 100) {
						break;
					}
					cnt++;
				}
				if (cnt >= 100) {
					break;
				}
			}
		}
	}

}
