package com.lucidworks.spark.example.streaming;

import java.util.Arrays;

import com.lucidworks.spark.SolrSupport;
import com.lucidworks.spark.SparkApp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

/**
 * Simple example of indexing tweets into Solr using Spark streaming; be sure to update the
 * twitter4j.properties file on the classpath with your Twitter API credentials.
 */
public class TwitterToSolrStreamProcessor extends SparkApp.StreamProcessor {

  public static Logger log = Logger.getLogger(TwitterToSolrStreamProcessor.class);

  public String getName() { return "twitter-to-solr"; }

  /**
   * Sends a stream of tweets to Solr.
   */
  @Override
  public void setup(JavaStreamingContext jssc, CommandLine cli) throws Exception {
    String filtersArg = cli.getOptionValue("tweetFilters");
    String[] filters = (filtersArg != null) ? filtersArg.split(",") : new String[0];

    // start receiving a stream of tweets ...
    JavaReceiverInputDStream<Status> tweets =
      TwitterUtils.createStream(jssc, null, filters);
    
    final HashingTF htf  = new HashingTF(10000);
	final NaiveBayesModel model = NaiveBayesModel.load(jssc.sparkContext().sc(), "nb-model");
	
    String fusionUrl = cli.getOptionValue("fusion");
    if (fusionUrl != null) {
      // just send JSON directly to Fusion
      SolrSupport.sendDStreamOfDocsToFusion(fusionUrl, cli.getOptionValue("fusionCredentials"), tweets, batchSize);
    } else {
      // map incoming tweets into PipelineDocument objects for indexing in Solr
      JavaDStream<SolrInputDocument> docs = tweets.map(
          new Function<Status,SolrInputDocument>() {

            /**
             * Convert a twitter4j Status object into a SolrJ SolrInputDocument
             */
            public SolrInputDocument call(Status status) {

              if (log.isDebugEnabled())
                log.debug("Received tweet: " + status.getId() + ": " + status.getText().replaceAll("\\s+", " "));

              // simple mapping from primitives to dynamic Solr fields using reflection
              SolrInputDocument doc =
                  SolrSupport.autoMapToSolrInputDoc("tweet-"+status.getId(), status, null);
              doc.setField("provider_s", "twitter");
              doc.setField("author_s", status.getUser().getScreenName());
              doc.setField("type_s", status.isRetweet() ? "echo" : "post");
              doc.setField("created_time_dt", status.getCreatedAt());
              doc.setField("text_s", status.getText());
              doc.setField("country_code_s", status.getPlace()==null?"00":status.getPlace().getCountryCode());
              doc.setField("id_l", status.getId());
              doc.setField("user_s", status.getUser().getScreenName());
              doc.setField("is_possibly_sensitive_b", status.isPossiblySensitive());
              doc.setField("lat_f", status.getGeoLocation()==null?0.0:status.getGeoLocation().getLatitude());
              doc.setField("long_f", status.getGeoLocation()==null?0.0:status.getGeoLocation().getLongitude());
              
              String filtered  = status.getText().replaceAll("[^\\x00-\\x7F]", "").replaceAll("@\\w+", "").replaceAll("http.+ ", "")
      				.replaceAll("\\d+", "").replaceAll("\\s+", " ");
              if(filtered.trim().length() > 14){
	              Double iii = model.predict(htf.transform(Arrays.asList(filtered.split(" "))));
	              doc.setField("sentiment_d", iii);
	              doc.setField("sentiment_s", iii.equals(0.0)?"Negative":"Positive");
              }
              else
              {
            	  doc.setField("sentiment_d", -1.0D);
	              doc.setField("sentiment_s", "NA");
              }
              return doc;
            }
          }
      );

      // when ready, send the docs into a SolrCloud cluster
      SolrSupport.indexDStreamOfDocs(zkHost, collection, batchSize, docs);
    }
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
              .withArgName("LIST")
              .hasArg()
              .isRequired(false)
              .withDescription("List of Twitter keywords to filter on, separated by commas")
              .create("tweetFilters"),
        OptionBuilder
            .withArgName("URL(s)")
            .hasArg()
            .isRequired(false)
            .withDescription("Fusion endpoint")
            .create("fusion"),
        OptionBuilder
            .withArgName("user:password:realm")
            .hasArg()
            .isRequired(false)
            .withDescription("Fusion credentials user:password:realm")
            .create("fusionCredentials")
    };
  }
}
