package com.lucidworks.spark.example.streaming;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import yahoofinance.Stock;

import com.coder.YahooStreamReceiver;
import com.lucidworks.spark.SolrSupport;
import com.lucidworks.spark.SparkApp;

/**
 * Simple example of indexing tweets into Solr using Spark streaming; be sure to update the
 * twitter4j.properties file on the classpath with your Twitter API credentials.
 */
public class YahooToSolrStreamProcessor extends SparkApp.StreamProcessor {

  public static Logger log = Logger.getLogger(YahooToSolrStreamProcessor.class);

  public String getName() { return "yahoo-to-solr"; }

  /**
   * Sends a stream of tweets to Solr.
   */
  @Override
  public void setup(JavaStreamingContext jssc, CommandLine cli) throws Exception {
    
	  JavaReceiverInputDStream<Stock> stocks = jssc
				.receiverStream(new YahooStreamReceiver());
	  
	String fusionUrl = cli.getOptionValue("fusion");
    if (fusionUrl != null) {
      // just send JSON directly to Fusion
      SolrSupport.sendDStreamOfDocsToFusion(fusionUrl, cli.getOptionValue("fusionCredentials"), stocks, batchSize);
    } else {
      // map incoming tweets into PipelineDocument objects for indexing in Solr
      JavaDStream<SolrInputDocument> docs = stocks.map(
          new Function<Stock,SolrInputDocument>() {

            /**
             * Convert a twitter4j Status object into a SolrJ SolrInputDocument
             */
            public SolrInputDocument call(Stock stock) {

              if (log.isDebugEnabled())
                log.debug("Received stock: " + stock);

              // simple mapping from primitives to dynamic Solr fields using reflection
              SolrInputDocument doc =
                  SolrSupport.autoMapToSolrInputDoc("tweet-"+stock.getSymbol()+System.currentTimeMillis(), stock, null);
              
              doc.setField("ticker_s", stock.getSymbol());
              doc.setField("name_s", stock.getName());
              doc.setField("price_d",stock.getQuote().getPrice().doubleValue() );
              doc.setField("change_d", stock.getQuote().getChangeInPercent().doubleValue());
              doc.setField("peg_d", stock.getStats().getPeg().doubleValue());
              //doc.setField("dividend_d", stock.getDividend().getAnnualYieldPercent().doubleValue());
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
