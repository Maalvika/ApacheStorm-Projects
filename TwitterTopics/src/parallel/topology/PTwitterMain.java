package parallel.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import twitterBolts.Hashtag_Emit_Bolt;
import twitterBolts.LBCAlgoBolt;
import twitterBolts.LogBolt;
import twitterBolts.NER_Emit_Bolt;
import twitterBolts.parallel.AggrLogBolt;
import twitterBolts.parallel.LBCAlgoBolt_small;
import twitterSpouts.TwitterSpout;

public class PTwitterMain {
	private static final String TWITTER_SPOUT_ID = "twitter-spout";
	
	private static final String NER_BOLT = "NER_bolt";
	private static final String HASHTAG_BOLT = "hashtag_bolt";
	
	private static final String LSB_SMALL_NER_ID = "LBSmall-bolt_ner";
	private static final String LSB_LARGE_NER_ID = "LBLarge-bolt_ner";
	
	private static final String LSB_SMALL_BUCKET = "LBSmall-bolt_hash";
	private static final String LSB_LARGE_BUCKET = "LBLarge-bolt_hash";
	
	private static final String LOG_BOLT_NER_ID = "output-bolt_ner";
	private static final String LOG_BOLT_HASH_ID = "output-bolt_hash";
	
	private static final String TOPOLOGY_NAME = "twitter-topology-parallel";

	public static void main(String args[])
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TwitterSpout ss = new TwitterSpout();
		
		NER_Emit_Bolt nerb = new NER_Emit_Bolt();
		
		LBCAlgoBolt_small lbc_small_ner = new LBCAlgoBolt_small();
		AggrLogBolt lbc_large_ner = new AggrLogBolt("/s/chopin/a/grad/mbachani/TwitterOutput/Parallel/NER.txt");
		
		//LogBolt ob_ner = new LogBolt("/s/chopin/a/grad/mbachani/TwitterOutput/Parallel/NER.txt");
		
		Hashtag_Emit_Bolt hb = new Hashtag_Emit_Bolt();
		
		LBCAlgoBolt_small lbc_small_hash = new LBCAlgoBolt_small();
		AggrLogBolt lbc_large_hash = new AggrLogBolt("/s/chopin/a/grad/mbachani/TwitterOutput/Parallel/Hashtag.txt");
		
		//LogBolt ob_hash = new LogBolt("/s/chopin/a/grad/mbachani/TwitterOutput/Parallel/Hashtag.txt");
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(TWITTER_SPOUT_ID, ss);
		
		 /**The shuffleGrouping() method tells Storm to shuffle tuples emitted by
		 the SentenceSpout class and distribute them evenly among 
		 instances of the SplitSentenceBolt object.*/
		
		builder.setBolt(NER_BOLT, nerb).shuffleGrouping(TWITTER_SPOUT_ID);
		builder.setBolt(HASHTAG_BOLT, hb).shuffleGrouping(TWITTER_SPOUT_ID);
		 
		/**Here, we use the fieldsGrouping() method of the BoltDeclarer class to
		 ensure that all tuples containing the same "word" value get routed to the same
		 WordCountBolt instance. */
		
		builder.setBolt(LSB_SMALL_NER_ID, lbc_small_ner, 4).fieldsGrouping(NER_BOLT, new Fields("key"));
		builder.setBolt(LSB_LARGE_NER_ID, lbc_large_ner).globalGrouping(LSB_SMALL_NER_ID);
		
		builder.setBolt(LSB_SMALL_BUCKET, lbc_small_hash, 4).fieldsGrouping(HASHTAG_BOLT, new Fields("key"));
		builder.setBolt(LSB_LARGE_BUCKET, lbc_large_hash).globalGrouping(LSB_SMALL_BUCKET);
		

		//builder.setBolt(LOG_BOLT_NER_ID, ob_ner).globalGrouping(LSB_LARGE_NER_ID);
		//builder.setBolt(LOG_BOLT_HASH_ID, ob_hash).globalGrouping(LSB_LARGE_BUCKET);

		Config config = new Config();
		config.setDebug(false);
		config.put(config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.put(Config.TOPOLOGY_WORKERS, 6);
		int fourGB = 8*1024;
		config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, fourGB);
		if (args == null || args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

			System.out.println("----------------ENTERRING INTO SLEEP MODE---------------");
			//Utils.sleep(10000);
			//ss.twitterStream.shutdown();
			//cluster.killTopology(TOPOLOGY_NAME);
			//cluster.shutdown();
		} else if (args[0].equals("remote")) {
			//config.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, config, builder.createTopology());
		}
	}

}
