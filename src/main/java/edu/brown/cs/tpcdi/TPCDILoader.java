package edu.brown.cs.tpcdi;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * Created by Lexi on 3/16/16.
 */
public class TPCDILoader {
	private static HashMap<String, AtomicLong> uniqueIDs;
    private static HashMap<Long, Long> brokerIDs;
    private static HashMap<String, Long> secIDs;
    private static HashMap<Long, Long> compIDs;
    private static HashMap<String, Long> compNames;
    private static HashMap<Long, Long> custIDs;
    private static HashMap<Long, Long> acctIDs;
    private static AtomicInteger objectType;
    
    private static HashMap<String, AtomicLong> copiedRowCount;
    
    private static CopyManager cm;
    private static HashMap<String, CopyIn> tableCopiers;
    /*
    private static CopyIn statusTypeCopier;
    private static CopyIn tradeTypeCopier;
    private static CopyIn dimDateCopier;
    private static CopyIn dimTimeCopier;
    private static CopyIn dimBrokerCopier;
    private static CopyIn dimSecurityCopier;
    private static CopyIn dimCompanyCopier;
    private static CopyIn dimCustomerCopier;
    private static CopyIn dimAccountCopier;
    */
    
	public TPCDILoader(Connection dbConn) throws SQLException {//String[] args) {
        //super(args);
        //if (d) LOG.debug("CONSTRUCTOR: " + TPCDILoader.class.getName());
        uniqueIDs = new HashMap<String, AtomicLong>();
        brokerIDs = new HashMap<Long, Long>();
        secIDs = new HashMap<String, Long>();
        compIDs = new HashMap<Long, Long>();
        compNames = new HashMap<String, Long>();
        custIDs = new HashMap<Long, Long>();
        acctIDs = new HashMap<Long, Long>();
        objectType = new AtomicInteger();

		//drop and recreate the tables
		createTables(dbConn);

        cm = new CopyManager((BaseConnection) dbConn);
        copiedRowCount = new HashMap<>(TPCDIConstants.LOADFILES.length);
        tableCopiers = new HashMap<>(TPCDIConstants.LOADFILES.length);


		System.out.println("here");
        for (String[] s : TPCDIConstants.LOADFILES) {
        	// init row counts for all tables to be loaded
        	copiedRowCount.put(s[0], new AtomicLong());
        	// init copiers for all tables to be loaded (no escaping of table names...)
        	tableCopiers.put(s[0], cm.copyIn("COPY " + s[0] + " FROM STDIN WITH CSV"));
        }

		System.out.println("here");

        /*
        statusTypeCopier = cm.copyIn("COPY " + TPCDIConstants.STATUSTYPE_TABLE + " FROM STDIN WITH CSV");
        tradeTypeCopier = cm.copyIn("COPY " + TPCDIConstants.TRADETYPE_TABLE + " FROM STDIN WITH CSV");
        dimDateCopier = cm.copyIn("COPY " + TPCDIConstants.DIMDATE_TABLE + " FROM STDIN WITH CSV");
        dimTimeCopier = cm.copyIn("COPY " + TPCDIConstants.DIMTIME_TABLE + " FROM STDIN WITH CSV");
        dimBrokerCopier = cm.copyIn("COPY " + TPCDIConstants.DIMBROKER_TABLE + " FROM STDIN WITH CSV");
        dimSecurityCopier = cm.copyIn("COPY " + TPCDIConstants.DIMSECURITY_TABLE + " FROM STDIN WITH CSV");
        dimCompanyCopier = cm.copyIn("COPY " + TPCDIConstants.DIMCOMPANY_TABLE + " FROM STDIN WITH CSV");
        dimCustomerCopier = cm.copyIn("COPY " + TPCDIConstants.DIMCUSTOMER_TABLE + " FROM STDIN WITH CSV");
        dimAccountCopier = cm.copyIn("COPY " + TPCDIConstants.DIMACCOUNT_TABLE + " FROM STDIN WITH CSV");
        */
        

    }
	
	public Object[] parseRow(String tablename, String tuple, int partId) {
		Object[] row = new Object[tableCopiers.get(tablename).getFieldCount()];
		
    	String[] parseTuple;
    	long sk_id;
    	long orig_id;
    	String symbol;
    	String type;
    	switch(tablename){
    	case TPCDIConstants.DIMTRADE_TABLE:
    		break;
    	case TPCDIConstants.STATUSTYPE_TABLE:
    		parseTuple = tuple.split("\\|");
     		row[0] = parseTuple[0];
    		row[1]= parseTuple[1];
				//row[2] = partId;
    		break;
    	case TPCDIConstants.TRADETYPE_TABLE:
    		parseTuple = tuple.split("\\|");
    		row[0] = parseTuple[0];
				row[1] = parseTuple[1];
				row[2] = new Integer(parseTuple[2]);
				row[3] = new Integer(parseTuple[3]);
				//row[4] = partId;
 			break;
    	case TPCDIConstants.TRADETXT_TABLE:
    	case TPCDIConstants.DIMDATE_TABLE:
    		parseTuple = tuple.split("\\|");
    		row[0] = new Long(parseTuple[0]);
    		row[1] = parseTuple[1];
    		row[2] = new Integer(parseTuple[3]);
    		row[3] = new Integer(parseTuple[7]);
    		row[4] = new Integer(parseTuple[9]);
    		row[5] = new Integer(parseTuple[11]);
    		row[6] = new Integer(parseTuple[13]);
    		row[7] = new Integer(parseTuple[15]);
				//row[8] = partId;
    		break;    		
    	case TPCDIConstants.DIMTIME_TABLE:
    		parseTuple = tuple.split("\\|");
    		row[0] = new Long(parseTuple[0]);
    		row[1] = new Integer(parseTuple[2]);
    		row[2] = new Integer(parseTuple[4]);
    		row[3] = new Integer(parseTuple[6]);
				//row[4] = partId;
    		break;    		
    	case TPCDIConstants.DIMBROKER_TABLE://csv HR.csv
    		parseTuple = tuple.split(",");
    		if(!parseTuple[5].equals("314"))
    			return null;
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		orig_id = new Long(parseTuple[0]);
    		row[0] = sk_id;
    		row[1] = orig_id;
    		brokerIDs.put(orig_id, sk_id);    		
    		break;
    	case TPCDIConstants.DIMSECURITY_TABLE:
    		type = tuple.substring(15, 18);
    		symbol = tuple.substring(18, 33);
    		String coNameOrCIK = tuple.substring(160);
    		if(!type.equals("SEC") || secIDs.containsKey(symbol))
        		return null;
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		row[0] = sk_id;
    		row[1] = symbol;
				//row[3] = TPCDIUtil.hashCode(symbol);
    		if(StringUtils.isNumeric(coNameOrCIK)){
    			if(!compIDs.containsKey(new Long(coNameOrCIK))) {
    				System.out.println(coNameOrCIK + " not found in DimCompany!");
    				return null;
    			}
    			row[2] = compIDs.get(new Long(coNameOrCIK));
    		}
    		else {
    			if(!compNames.containsKey(coNameOrCIK)) {
    				System.out.println(coNameOrCIK + " not found in DimCompany!");
    				return null;
    			}
    			row[2] = compNames.get(coNameOrCIK);
    		}
    		secIDs.put(symbol, sk_id);
    		break;
    	case TPCDIConstants.DIMCOMPANY_TABLE:
    		type = tuple.substring(15, 18);
    		
    		if(!type.equals("CMP"))
        		return null;
    		orig_id = new Long(tuple.substring(78, 88));
    		String name = tuple.substring(18, 78);
    		if(compIDs.containsKey(orig_id))
    			return null;
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		row[0] = sk_id;
    		row[1] = orig_id;
    		compIDs.put(orig_id, sk_id);
    		compNames.put(name, sk_id);
    		break;
    		
    	case TPCDIConstants.DIMCUSTOMER_TABLE://xml CustomerMgmt.xml
    		parseTuple = tuple.split("\\|");
    		type = parseTuple[0];
    		if(!type.equals("NEW"))
    			return null;

    		orig_id = new Long(parseTuple[2]);
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		//System.out.println(parseTuple[2] + ": " + sk_id + " , " + orig_id);
    		row[0] = sk_id;
    		row[1] = orig_id;
    		custIDs.put(orig_id, sk_id);
    		break;
    		
    	case TPCDIConstants.DIMACCOUNT_TABLE:
    		parseTuple = tuple.split("\\|");
    		type = parseTuple[0];
    		long orig_customer_id;
			  long orig_broker_id;
    		
    		if(type.equals("NEW")) {
    			orig_customer_id = new Long(parseTuple[2]);
    			orig_broker_id = new Long(parseTuple[34]);
    			orig_id = new Long(parseTuple[32]);
    		} else if(type.equals("ADDACCT")) {
    			orig_customer_id = new Long(parseTuple[2]);
    			orig_broker_id = new Long(parseTuple[5]);
    			orig_id = new Long(parseTuple[3]);
    		} else {
    			//System.out.println("ACCOUNT NOT NEW OR ADDACCT");
    			return null;
    		}
    		if(!custIDs.containsKey(orig_customer_id)){
    			System.out.println("WARNING: CUSTOMER ID " + orig_customer_id +" NOT FOUND");
    			return null;
    		}
    		if(!brokerIDs.containsKey(orig_broker_id)){
    			System.out.println("WARNING: BROKER ID " + orig_broker_id +" NOT FOUND");
    			return null;
    		}
    		long new_customer_id = custIDs.get(orig_customer_id);
    		long new_broker_id = brokerIDs.get(orig_broker_id);
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		row[0] = sk_id;
    		row[1] = orig_id;
    		row[2] = new_broker_id;
    		row[3] = new_customer_id;
				//row[4] = TPCDIUtil.hashCode(String.valueOf(orig_id));
    		acctIDs.put(orig_id, sk_id);
    		break;
    		
    	}
    	//int part_id = TPCDIUtil.getPartitionID(table, row);
    	//row[row.length-1] = part_id;
    	
    	return row;
    };

	
    public void load() {
    	String loadfiles[][] = TPCDIConstants.LOADFILES;
        for(int i = 0; i < loadfiles.length; i++){
        	try {
        		AtomicLong startID = new AtomicLong(objectType.getAndIncrement() * 100000000L);
        		uniqueIDs.put(loadfiles[i][0], startID);
        		if(loadfiles[i][1].equals(TPCDIConstants.FINWIRE))
        		{
        			File folder = new File(TPCDIConstants.FILE_DIR);
        			File[] listOfFiles = folder.listFiles();
        			Arrays.sort(listOfFiles);
        			for(File curFile : listOfFiles) {
        				String filename = curFile.getName();
        				if(!filename.contains(TPCDIConstants.FINWIRE) || filename.contains("audit"))
        					continue;


								loadSimple(loadfiles[i][0], curFile.getName());

        			}
        			
        		} else {

        					/*
							// CMATHIES: Copy Date, Time, StatusType, and TradeType to all partitions.
							if (loadfiles[i][0].equals(TPCDIConstants.DIMDATE_TABLE) || loadfiles[i][0].equals(TPCDIConstants.DIMTIME_TABLE)
									|| loadfiles[i][0].equals(TPCDIConstants.STATUSTYPE_TABLE) || loadfiles[i][0].equals(TPCDIConstants.TRADETYPE_TABLE)) {

								for (int j = 0; j < TPCDIConstants.NUM_PARTITIONS; j++) {
									loadSimple(loadfiles[i][0], loadfiles[i][1]);
								}
							} else {
								loadSimple(loadfiles[i][0], loadfiles[i][1]);
							}
							*/
        			loadSimple(loadfiles[i][0], loadfiles[i][1]);
        		}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
    }

	private void loadSimple(final String tablename, String filename) throws Exception {
		String file = TPCDIConstants.FILE_DIR + filename;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	        String nextline = br.readLine();
	        
	        // while redundant b/c of the next for loop??
	        while(nextline != null) {
	        	// why does the original benchmark does it like this?
	        	String[] tuples = new String[TPCDIConstants.MAX_ARRAY_SIZE];
	        	for(int curIndex = 0; curIndex < TPCDIConstants.MAX_ARRAY_SIZE; curIndex++) {
		    		if(nextline == null)
		    		{
		    			if(curIndex == 0)
				    		return;
		    			tuples = Arrays.copyOfRange(tuples,0,curIndex);
		    			break;
		    		}
	        		tuples[curIndex] = nextline;
		        	nextline = br.readLine();
		    	}
			    		        	
		    	final String[] allTuples = tuples;
		    	
		    	List<Object[]> currBatch = new ArrayList<>();
	            for (int i = 0; i < allTuples.length; i++) {
	            	Object[] row = parseRow(allTuples[i], allTuples[i], 0);
	            	
	            	if(row != null)
	            		currBatch.add(row);
	            	
	            	if (currBatch.size() >= TPCDIConstants.BATCH_SIZE) {
	            		addBatchToTable(tablename, currBatch);
	            		currBatch.clear();
	            	}
	            } // FOR
	            
	            // add remaining batch
	            addBatchToTable(tablename, currBatch);
	    		currBatch.clear();
	            
	        }//while
	        br.close();
        }
	}
	
	public void createTables(Connection dbConn) throws SQLException {
		Statement st = dbConn.createStatement();

		// drop all tables to be loaded into
		String[] tables = new String[TPCDIConstants.LOADFILES.length];
		for (int i = 0; i < TPCDIConstants.LOADFILES.length; i++) {
			tables[i] = TPCDIConstants.LOADFILES[i][0];
		}
		st.execute(
				"DROP TABLE IF EXISTS " + String.join(", ", tables) + " CASCADE;");
		
		// recreate all tables w/ constraints
		
		// trade type
		st.execute(
				"CREATE TABLE TradeType ("
				+ "TT_ID            varchar(3)   NOT NULL,"
				+ "TT_NAME          varchar(12)  NOT NULL,"
				+ "TT_IS_SELL       smallint  NOT NULL,"
				+ "TT_IS_MRKT       smallint  NOT NULL,"
				//+ "part_id          int       NOT NULL,"
				+ "CONSTRAINT PK_TradeType PRIMARY KEY (TT_ID));");
		
		// status type
		st.execute(
				"CREATE TABLE StatusType ("
				+ "ST_ID            varchar(4)   NOT NULL,"
				+ "ST_NAME          varchar(10)  NOT NULL,"
				//+ "part_id          int       NOT NULL,"
				+ "CONSTRAINT PK_StatusType PRIMARY KEY (ST_ID));");
		
		// dim security
		st.execute(
				"CREATE TABLE DimSecurity ("
				+ "SK_SecurityID    bigint    NOT NULL,"
				+ "Symbol           char(15)  NOT NULL,"
				+ "SK_CompanyID     bigint    NOT NULL,"
				//+ "part_id          int       NOT NULL,"
				+ "CONSTRAINT PK_DimSecurity PRIMARY KEY (SK_SecurityID));");
		st.execute(
				"CREATE INDEX IX_Security on DimSecurity(Symbol);");
		
		// dim customer
		st.execute(
				"CREATE TABLE DimCustomer ("
				+ "SK_CustomerID    bigint    NOT NULL,"
				+ "CustomerID       bigint    NOT NULL,"
				//+ "part_id          int       NOT NULL,"
				+ "CONSTRAINT PK_DimCustomer PRIMARY KEY (SK_CustomerID));");
		
		// dim company
		st.execute(
				"CREATE TABLE DimCompany ("
				+ "SK_CompanyID    bigint    NOT NULL,"
				+ "CompanyID       bigint    NOT NULL,"
				//+ "part_id          int       NOT NULL,"
				+ "CONSTRAINT PK_DimCompany PRIMARY KEY (SK_CompanyID));");
		
		// dim account
		st.execute(
				"CREATE TABLE DimAccount ("
				+ "SK_AccountID    bigint    NOT NULL,"
				+ "AccountID       bigint    NOT NULL,"
				+ "SK_BrokerID     bigint    NOT NULL,"
				+ "SK_CustomerID   bigint    NOT NULL,"
				//+ "part_id          int       NOT NULL,"
				+ "CONSTRAINT PK_DimAccount PRIMARY KEY (AccountID));");
		
		// dim broker
		st.execute(
				"CREATE TABLE DimBroker ("
				+ "SK_BrokerID    bigint    NOT NULL,"
				+ "BrokerID       bigint    NOT NULL,"
				//+ "part_id          int       NOT NULL,"
				+ "CONSTRAINT PK_DimBroker PRIMARY KEY (SK_BrokerID));");
		
		// dim date
		st.execute(
				"CREATE TABLE DimDate ("
				+ "SK_DateID              bigint    NOT NULL,"
				+ "DateValue		   varchar(10) NOT NULL,"
				+ "CalendarYearID         smallint  NOT NULL,"
				+ "CalendarMonthID        integer   NOT NULL,"
				+ "CalendarWeekID         integer   NOT NULL,"
				+ "DayOfWeekNum           smallint  NOT NULL,"
				+ "FiscalYearID           smallint  NOT NULL,"
				+ "FiscalQuarterID        smallint  NOT NULL,"
				//+ "part_id                int       NOT NULL,"
				+ "CONSTRAINT PK_DimDate PRIMARY KEY (SK_DateID));");
		st.execute(
				"CREATE INDEX IX_DimDate on DimDate(DateValue);");
		
		// dim time
		st.execute(
				"CREATE TABLE DimTime ("
				+ "SK_TimeID              bigint    NOT NULL,"
				+ "HourID                 smallint  NOT NULL,"
				+ "MinuteID               smallint  NOT NULL,"
				+ "SecondID               smallint  NOT NULL,"
				//+ "part_id                int       NOT NULL,"
				+ "CONSTRAINT PK_TimeID PRIMARY KEY (SK_TimeID));");
		st.execute(
				"CREATE INDEX IX_DimTime on DimTime(HourID,MinuteID,SecondID);");
		
		// clean up
		st.close();
	}

	private void addBatchToTable(String tablename, List<Object[]> currBatch) throws SQLException {
		//build string
		StringBuilder sb = new StringBuilder();
		for (Object[] row : currBatch) {
			String[] rowString = new String[row.length];
			for (int i = 0; i < row.length; i++) {
				rowString[i] = row[i].toString();
			}
			
			sb.append(String.join(", ", rowString));
			sb.append("\n");
		}
		String inputCSV = sb.toString();
		byte[] csvBytes = inputCSV.getBytes();
		
		CopyIn copier = tableCopiers.get(tablename);
		copier.writeToCopy(csvBytes, 0, csvBytes.length);
		copier.endCopy();
		
		copiedRowCount.get(tablename).addAndGet(currBatch.size());
	}
}
