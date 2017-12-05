package twitterBolts;


import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class Hashtag_Emit_Bolt extends BaseRichBolt {

	OutputCollector collector;

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Object tweet = tuple.getValueByField("tweet");
		collector.ack(tuple);
		Status tweet_stats = (Status) tweet;

		for (HashtagEntity hstg : tweet_stats.getHashtagEntities()) {
			System.out.println("HASHTAG--------------: " + hstg.getText());
			collector.emit(new Values(hstg.getText().toUpperCase(), analyze_sentiment(tweet_stats.getText())));

		}

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("key", "senti_score"));

	}
	
	private double analyze_sentiment(String text) {

		int senti_value = 0;
		text = text.replace("#", ".");
		Properties properties = new Properties();
		properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(properties);
		Annotation document = new Annotation(text);
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
			switch (sentiment) {
			case "Neutral":
				senti_value += 0;
				break;
			case "Negative":
				senti_value += -1;
				break;
			case "Positive":
				senti_value += 1;
				break;
			case "Very Negative":
				senti_value += -2;
				break;
			case "Very Positive":
				senti_value += 2;
				break;

			}

		}
		double final_senti = (double)senti_value/(double)sentences.size();
		System.out.println("in NLPPPPPPPPPP-------------- score:" + final_senti);
		return final_senti;

	}

}
