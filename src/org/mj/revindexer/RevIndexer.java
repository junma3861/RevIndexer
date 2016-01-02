/**
 * 
 * @author Jun
 */

package org.mj.revindexer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.UnsupportedEncodingException;

import com.mongodb.MongoClient;
import com.mongodb.Block;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.FindIterable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RevIndexer {
	
	private static final Logger logger = LoggerFactory.getLogger(RevIndexer.class);
	
	private static final String INDEX_DB_NAME = "WebCrawlerIndexDB";
	private static final String REV_INDEX_DB = "RevIndexDB";
	
	private MongoClient mongoClient;
	private MongoDatabase indexDB, revIndexDB;
	
	protected final Object mutex = new Object();
	

	/**
	 * class constructor
	 */
	public RevIndexer() {
		
		
	}
	
	
	/**
	 * Initialize the databases
	 */
	public void initialize() {
		
		try {
			
			mongoClient = new MongoClient();
			indexDB = mongoClient.getDatabase(INDEX_DB_NAME);
			revIndexDB = mongoClient.getDatabase(REV_INDEX_DB);
			
			logger.info("Successfully initialized {} and {}.", INDEX_DB_NAME, REV_INDEX_DB);
			//txnIndexDB = indexDBEnv.beginTransaction(null, null);
			//txnOutgoingUrlDB = outgoingDBEnv.beginTransaction(null, null);
			
		} catch (Exception dbe) {
			shutDown();
			logger.error("Error while openining index or outgoingUrlDB database.");
			dbe.printStackTrace();			
			
		}
		
	}
	
	
	/**
	 * 
	 */
	protected void shutDown() {
		
		logger.info("Shutting down databases.");
		
		try {
			mongoClient.close();
			
		} catch (Exception e) {
			logger.error("Error while shutting down MongoDB Client.");
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Start
	 */
	public void start() {
		
		try {
			
			// For each entry
			FindIterable<Document> iterable = indexDB.getCollection("DocId_WordCount").find();
			
			iterable.forEach(new Block<Document>() {
				
				@Override
				public void apply(final Document document) {
					
					
					revIndexDB.getCollection("Word_DocId").insertOne(new Document().append("word", value));
				}
			});
			
			
			
		} catch (Exception e) {
			
		}
		
	}

}
