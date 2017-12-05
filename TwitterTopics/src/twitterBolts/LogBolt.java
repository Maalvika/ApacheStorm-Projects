package twitterBolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import common.LogObject;

public class LogBolt extends BaseRichBolt {

	private PrintWriter writer;
	private double start_time;
	private String file_path;

	Map<Double, TreeMap<Integer, List<LogObject>>> timed_log;
	DateFormat dateFormat;

	public LogBolt(String path) {
		// TODO Auto-generated constructor stub
		file_path = path;
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub

		String key = tuple.getStringByField("key");
		int count = tuple.getIntegerByField("true_count");
		double senti_score = tuple.getDoubleByField("senti_score");

		System.out.println("######################\t key:" + key + "\tcount:" + count + "######################");
		// System.out.println("List of tweets");

		LogObject obj = new LogObject(key, senti_score, count);

		double now = new Date().getTime() / 1000;
		System.out.println("Start time - Now:" + (start_time - now));

		if (now - start_time < 10) {
			if (timed_log.containsKey(start_time)) {
				TreeMap<Integer, List<LogObject>> lobj = timed_log.get(start_time);
				List<LogObject> log = lobj.get(count);
				if (log != null && !log.isEmpty()) {
					log.add(obj);
					lobj.put(count, log);
					timed_log.put(start_time, lobj);
				} else {
					log = new LinkedList<>();
					log.add(obj);
					lobj.put(count, log);
					timed_log.put(start_time, lobj);
				}
			} else {
				TreeMap<Integer, List<LogObject>> lobj = new TreeMap<>(Collections.reverseOrder());
				List<LogObject> log = new LinkedList<>();
				log.add(obj);
				lobj.put(count, log);
				timed_log.put(start_time, lobj);
			}
		} else {

			double prev_time = start_time;
			start_time = new Date().getTime() / 1000;
			TreeMap<Integer, List<LogObject>> output_obj = timed_log.get(prev_time);
			timed_log.remove(prev_time);
			int cnt = 1;
			// write to file
			writer.write("<TIME:" + dateFormat.format(start_time*1000) + ">");
			writer.flush();
			if (output_obj != null && !output_obj.isEmpty()) {
				for (Integer rank : output_obj.keySet()) {
					for (LogObject l : output_obj.get(rank)) {

						writer.write("<#" + l.getHashTag() + ", count:"+rank+", senti score:" + String.format( "%.3f", l.getScore())+ ">");
						writer.flush();
						if (cnt >= 100) {
							break;
						}
						cnt++;
					}
					if(cnt >= 100) {
						break;
					}
				}
			} else {
				writer.write("<NO LOG GENERATED>");
				writer.flush();
			}
			writer.write(System.getProperty("line.separator"));
			
			

		}

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		try {
			writer = new PrintWriter(new File(file_path));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		timed_log = new HashMap<>();
		start_time = new Date().getTime() / 1000;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
