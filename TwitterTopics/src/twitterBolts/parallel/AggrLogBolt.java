package twitterBolts.parallel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TreeMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import common.LogObject;

public class AggrLogBolt extends BaseRichBolt {

	// private OutputCollector collector;
	private Map<Integer, Set<String>> top_value = null;
	private Map<String, LogObject> log_values = null;
	private Timer timer;
	private PrintWriter writer;
	private String path;
	private DateFormat dateFormat;

	public AggrLogBolt(String path) {
		// TODO Auto-generated constructor stub
		this.path = path;
	}

	@Override
	public void execute(Tuple tuple) {
		String key = tuple.getStringByField("key");
		int count = tuple.getIntegerByField("freq");
		double senti_score = tuple.getDoubleByField("senti_score");

		System.out.println("######################\t key:" + key + "\tcount:" + count + "######################");
		// System.out.println("List of tweets");

		LogObject obj = new LogObject(key, senti_score, count);
		if (!top_value.containsKey(count)) {
			Set<String> list_objs = new HashSet<>();
			list_objs.add(key);
			top_value.put(count, list_objs);
			log_values.put(key, obj);
		} else {
			if (log_values.containsKey(obj.getHashTag()) && top_value.containsKey(log_values.get(key).getFreq())) {
				int prev_freq = log_values.get(key).getFreq();
				Set<String> temp_list_objs = top_value.get(prev_freq);
				temp_list_objs.remove(key);
				top_value.put(prev_freq, temp_list_objs);
			}

			Set<String> list_objs = top_value.get(count);
			list_objs.add(key);
			top_value.put(count, list_objs);
			log_values.put(key, obj);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		// this.collector = collector;
		top_value = new TreeMap<>(Collections.reverseOrder());
		log_values = new HashMap<>();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		try {
			writer = new PrintWriter(new File(path));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		timer = new Timer();
		timer.scheduleAtFixedRate(new java.util.TimerTask() {
			@Override
			public void run() {
				// your code here
				print_log();
			}
		}, 10000, 10000);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		// declarer.declare(new Fields("key", "true_count", "senti_score"));

	}

	private void print_log() {
		int cnt = 1;
		// write to file
		writer.write("<TIME:" + dateFormat.format(new Date().getTime()) + ">");
		writer.flush();
		if (!top_value.isEmpty()) {
			for (Integer rank : top_value.keySet()) {
				for (String s : top_value.get(rank)) {
					LogObject l = log_values.get(s);
					if (rank == l.getFreq()) {
						writer.write("<#" + l.getHashTag() + ", count:" + l.getFreq() + ", senti score:"
								+ String.format("%.3f", l.getScore()) + ">");
						writer.flush();
						if (cnt >= 100) {
							break;
						}
						cnt++;
					}
				}
				if (cnt >= 100) {
					break;
				}
			}
		} else {
			writer.write("<NO LOG GENERATED>");
			writer.flush();
		}
		writer.write(System.getProperty("line.separator"));
		top_value.clear();
		log_values.clear();
	}

}
