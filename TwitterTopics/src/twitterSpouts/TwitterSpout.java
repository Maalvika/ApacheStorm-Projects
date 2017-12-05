package twitterSpouts;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	public TwitterStream twitterStream;
	private List<LanguageProfile> languageProfiles;
	private LanguageDetector languageDetector;
	private TextObjectFactory textObjectFactory;

	private String consumerKey = "x7U9qTHvjg22hR19weN1DOU26";
	private String consumerSecret = "3Zpnq7WOkqhlibl1FH9a05XsjuyGvjnWzjmh0nO1LLES3aOvf1";
	private String accessToken = "1153339314-KSpHSEbMm9DLAn0PBmuxQxolhtkrai0io8OJfPb";
	private String accessTokenSecret = "nDjZyRJSBELCCfrePtra7j4BBWVbKNlHLRAimI5EOyLyh";

	public TwitterSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Status tweet = queue.poll();

		if (tweet == null) {
			Utils.sleep(50);
		} else {

			collector.emit(new Values(tweet));

		}

	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		queue = new LinkedBlockingQueue<>();
		try {
			detect_lang_init();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				if (detect_language(status.getText()).trim().equalsIgnoreCase("en")) {
					queue.offer(status);
				}
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {
			}

			@Override
			public void onException(Exception e) {
			}
		};

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setJSONStoreEnabled(true);

		cb.setOAuthConsumerKey(consumerKey);
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(accessToken);
		cb.setOAuthAccessTokenSecret(accessTokenSecret);

		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		twitterStream.addListener(listener);
		twitterStream.sample();
		System.out.println("Connect Successful");

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config config = new Config();
		config.setMaxTaskParallelism(1);
		return config;
	}

	private String detect_language(String tweet) {
		TextObject textObject = textObjectFactory.forText(tweet);
		Optional<LdLocale> lang = languageDetector.detect(textObject);
		if (lang.isPresent()) {
			//System.out.println(lang.get().toString());
			return lang.get().toString();
		} 
		return "NULL";
	}
	
	private void detect_lang_init() throws IOException{
		languageProfiles = new LanguageProfileReader().readAllBuiltIn();
		languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
				.withProfiles(languageProfiles).build();
		textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
	}

}
