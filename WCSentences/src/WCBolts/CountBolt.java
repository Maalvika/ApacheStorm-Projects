package WCBolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CountBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;

	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String w = arg0.getStringByField("word");
		if(counts.containsKey(w)) {
			long temp = counts.get(w);
			temp++;
			counts.put(w, temp);
		} else {
			counts.put(w, 1L);
		}
		collector.emit(new Values(w, counts.get(w)));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		collector  = arg2;
		counts = new HashMap<>();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("word","count"));
		
	}

}
