package WCSpouts;

import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.*;



//BaseRichSpout is implementation of ISpout interface
public class SentenceSpout extends BaseRichSpout{
	
	private SpoutOutputCollector collector;
	//Each sentence is emitted as a single field of tuple
	private static String[] sentences = {
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"i don't think i like fleas"
	};
	
	private static int counter = 0;
	
	/**
	 * The nextTuple() method represents the core of any spout implementation. 
	 * Storm calls this method to request that the spout emit tuples to the output collector.
	 */
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		collector.emit(new Values(sentences[counter]));
		counter++;
		//System.out.println("------------- counter ---------"+counter);
		if(counter>=sentences.length) {
			counter = 0;
		}
		Utils.sleep(100);	
		
	}

	/**
	 * The open() method is defined in the ISpout interface and is called whenever a spout component is initialized.
	 */
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		
	}

	/**
	 * The declareOutputFields() method is defined in the IComponent interface that 
	 * all Storm components (spouts and bolts) must implement and is used to tell 
	 * Storm what streams a component will emit and the fields each stream's tuples will contain. 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("sentence"));
		
	}

}
