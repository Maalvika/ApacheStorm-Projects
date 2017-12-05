package twitterBolts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Optional;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class NER_Emit_Bolt extends BaseRichBolt {

	OutputCollector collector;

	AbstractSequenceClassifier<CoreLabel> classifier = null;
	String serializedClassifier = "edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz";
	final Pattern ORGANIZATION = Pattern.compile("<ORGANIZATION>(.+?)</ORGANIZATION>");
	final Pattern PERCENT = Pattern.compile("<PERCENT>(.+?)</PERCENT>");
	final Pattern PERSON = Pattern.compile("<PERSON>(.+?)</PERSON>");
	final Pattern LOCATION = Pattern.compile("<LOCATION>(.+?)</LOCATION>");
	final Pattern DATE = Pattern.compile("<DATE>(.+?)</DATE>");
	final Pattern MONEY = Pattern.compile("<MONEY>(.+?)</MONEY>");
	final Pattern TIME = Pattern.compile("<TIME>(.+?)</TIME>");

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub

		Object tweet = tuple.getValueByField("tweet");
		collector.ack(tuple);
		Status tweet_stats = (Status) tweet;
		String tagged_ner = classifier.classifyToString(tweet_stats.getText(), "inlineXML", false);
		System.out.println("NER--------------: " + tagged_ner);
		List<String> list_ner = getTagValues(tagged_ner);
		if (list_ner != null && !list_ner.isEmpty()) {
			for (String ner : list_ner) {
				collector.emit(new Values(ner.trim().toUpperCase(), analyze_sentiment(tweet_stats.getText())));
			}
		}

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		try {
			classifier = CRFClassifier.getClassifier(serializedClassifier);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("key", "senti_score"));

	}

	private List<String> getTagValues(String tempNer) {
		final List<String> tagValues = new ArrayList<String>();
		final Matcher matcher1 = DATE.matcher(tempNer);
		while (matcher1.find()) {
			tagValues.add(matcher1.group(1));
		}
		final Matcher matcher2 = TIME.matcher(tempNer);
		while (matcher2.find()) {
			tagValues.add(matcher2.group(1));
		}
		final Matcher matcher3 = MONEY.matcher(tempNer);
		while (matcher3.find()) {
			tagValues.add(matcher3.group(1));
		}
		final Matcher matcher4 = PERSON.matcher(tempNer);
		while (matcher4.find()) {
			tagValues.add(matcher4.group(1));
		}
		final Matcher matcher5 = ORGANIZATION.matcher(tempNer);
		while (matcher5.find()) {
			tagValues.add(matcher5.group(1));
		}
		final Matcher matcher6 = PERCENT.matcher(tempNer);
		while (matcher6.find()) {
			tagValues.add(matcher6.group(1));
		}
		final Matcher matcher7 = LOCATION.matcher(tempNer);
		while (matcher7.find()) {
			tagValues.add(matcher7.group(1));
		}
		return tagValues;
	}
	
	private double analyze_sentiment(String text) {

		int senti_value = 0;
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
		
		if(senti_value>2){
			senti_value = 2;
		}
		if(senti_value<-2){
			senti_value = -2;
		}
		double final_senti = (double)senti_value/(double)sentences.size();
		System.out.println("in NLPPPPPPPPPP-------------- score:" + final_senti);
		return final_senti;

	}

}
