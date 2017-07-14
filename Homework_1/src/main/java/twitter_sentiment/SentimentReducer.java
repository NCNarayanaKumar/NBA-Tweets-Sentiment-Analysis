package twitter_sentiment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilities.TweetWritable;

/**
 * The SentimentReducer processes scored tweet data and finds the highest and the lowest scoring tweets.
 * 
 * 
 * The goal of the SentimentReducer is to find the maximum and minimum scoring tweets for each topic.
 * 
 * In this first version of Sentiment processing, we are only processing one topic - tweets about the Clippers on the
 * evening of May 1, 2017
 * 
 * 
 * @author elizabeth corey
 *
 */
public class SentimentReducer extends Reducer<Text, TweetWritable, Text, TweetWritable> {

	public static int maxScore = Integer.MIN_VALUE;
	public static int minScore = Integer.MAX_VALUE;

	public static TweetWritable tweet, maxTweet, minTweet;

	public static boolean testing = true;

	/**
	 * Each time reduce runs it processes the scored tweets for one key
	 * 
	 * Keys are Tweet topics for a specific time-frame (an evening in May, for instance)
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<TweetWritable> tweetList, Context context)
			throws IOException, InterruptedException {

		// Iterate through the TweetWritables in the tweetList

		Iterator<TweetWritable> iter = tweetList.iterator();
		while (iter.hasNext()) {

			// find the maximum and minimum scores and their associated tweets

			tweet = iter.next();
			if (tweet.getScore() < minScore) {
				minTweet = new TweetWritable(tweet);
				minScore = (int) tweet.getScore();
			}

			if (tweet.getScore() > maxScore) {
				maxTweet = new TweetWritable(tweet);
				maxScore = (int) tweet.getScore();

			}
		}

		/*-
		 * Use context.write to write out 
		 * 
		 * 		1) the topic and the maxTweet 
		 * 		2) the topic and the minTweet
		 * 
		 */

		String topic = key.toString();

		if (testing)
			System.err.println("Printing out max:  " + maxTweet.toString());
		key.set(topic + "(maximum score): ");
		context.write(key, maxTweet);

		if (testing)
			System.err.println("Printing out min:  " + minTweet.toString());
		key.set(topic + "(minimum score): ");
		context.write(key, minTweet);
	}

}
