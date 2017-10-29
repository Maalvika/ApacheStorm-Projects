package Topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import WCBolts.CountBolt;
import WCBolts.OutputBolt;
import WCBolts.WordBolt;
import WCSpouts.SentenceSpout;

public class WCMain {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String WORD_BOLT_ID = "word-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String OUTPUT_BOLT_ID = "output-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	
	public static void main(String args[]) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		SentenceSpout ss = new SentenceSpout();
		WordBolt wb = new WordBolt();
		CountBolt cb = new CountBolt();
		OutputBolt ob = new OutputBolt();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, ss);
		//The shuffleGrouping() method tells Storm to shuffle tuples emitted by the SentenceSpout class 
		//and distribute them evenly among instances of the SplitSentenceBolt object.
		builder.setBolt(WORD_BOLT_ID, wb).shuffleGrouping(SENTENCE_SPOUT_ID);
		//Here, we use the fieldsGrouping() method of the BoltDeclarer class to ensure that 
		//all tuples containing the same "word" value get routed to the same WordCountBolt instance.
		builder.setBolt(COUNT_BOLT_ID, cb).fieldsGrouping(WORD_BOLT_ID, new Fields("word"));
		
		builder.setBolt(OUTPUT_BOLT_ID, ob).globalGrouping(COUNT_BOLT_ID);
		
		
		Config config = new Config();
		config.setDebug(false);
		config.put(config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		if(args == null || args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.
				createTopology());
			try {
				
				System.out.println("----------------ENTERRING INTO SLEEP MODE---------------");
				Utils.sleep(10000);
			} catch(Exception e){
				System.out.println("------------- exitting sleep mode-----------");
				e.printStackTrace();
			}
		
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		} else if (args[0].equals("remote")) {
			config.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, config, builder.createTopology());
		}
	}

	

}
