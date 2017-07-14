package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TweetWritable implements WritableComparable<TweetWritable> {

	public Text tweet;
	public IntWritable nHits;
	public IntWritable size;
	public LongWritable tweetId;
	public DoubleWritable score;

	public TweetWritable() {
		tweet = new Text();
		nHits = new IntWritable();
		size = new IntWritable();
		tweetId = new LongWritable();
		score = new DoubleWritable();
	}

	public TweetWritable(TweetWritable tw) {
		tweet = new Text(tw.tweet);
		nHits = new IntWritable(tw.nHits.get());
		size = new IntWritable(tw.size.get());
		tweetId = new LongWritable(tw.tweetId.get());
		score = new DoubleWritable(tw.score.get());
	}

	@Override
	public int compareTo(TweetWritable o) {
		return (tweetId.compareTo(o.tweetId));
	}

	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (o == null)
			return false;
		if (getClass() != o.getClass())
			return false;

		TweetWritable w = (TweetWritable) o;
		if (w.tweetId.equals(this.tweetId))
			return true;

		return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		tweet.readFields(in);
		nHits.readFields(in);
		size.readFields(in);
		tweetId.readFields(in);
		score.readFields(in);

	}

	@Override
	public String toString() {
		return "[" + tweetId + "]  \n\t Emotion:  " + nHits + ", Tweet length (words): " + size + ", score:  " + score
				+ "\n\t TWEET:  " + tweet;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		tweet.write(out);
		nHits.write(out);
		size.write(out);
		tweetId.write(out);
		score.write(out);

	}

	public void setTweet(String tweet) {
		this.tweet = new Text(tweet);
	}

	public String getTweet() {
		return tweet.toString();
	}

	public void setNHits(int nSentiments) {
		this.nHits.set(nSentiments);
	}

	public int getNHits() {
		return nHits.get();
	}

	public void setSize(int size) {
		this.size.set(size);
	}

	public int getSize() {
		return size.get();
	}

	public void setTweetId(long tweetId) {
		this.tweetId.set(tweetId);
	}

	public long getTweetId() {
		return tweetId.get();
	}

	public void setScore(double score) {
		this.score.set(score);
	}

	public double getScore() {
		return score.get();
	}

}
