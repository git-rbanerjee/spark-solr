package com.coder;

import java.util.Map;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import yahoofinance.Stock;
import yahoofinance.YahooFinance;

public class YahooStreamReceiver extends Receiver<Stock> { 
	
	
	private static final long serialVersionUID = 701605443217308658L;

	public YahooStreamReceiver() {
		super(StorageLevel.MEMORY_AND_DISK_2());
	}

	@Override
	public void onStart() {
		new Thread() {
			@Override
			public void run() {
				receive();
			}
		}.start();	
	}

	@Override
	public void onStop() {
		//Nothing to do
		//
	}

	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		try {
			String[] tickers = new String[]{"INTC", "BABA", "TSLA", "AIR.PA", "YHOO"};
			while(isStarted() && !isStopped())
			{
				Map<String, Stock> stocks = YahooFinance.get(tickers); // single request
				for(String tick : stocks.keySet())
					store(stocks.get(tick));
				Thread.sleep(10000);
			}
			
			
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
		restart("Next");
	}

}
