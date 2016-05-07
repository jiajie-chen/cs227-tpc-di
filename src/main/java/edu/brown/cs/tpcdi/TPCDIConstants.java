package edu.brown.cs.tpcdi;
/**
 * Created by Lexi on 3/16/16.
 */
public class TPCDIConstants {
	public static final String FILE_DIR = "withdebug/Batch2/";
	
	
    public static final int STREAMINGESTOR_PORT = 18000;
    public static final String STREAMINGESTOR_HOST = "localhost";
    
    public static final int  BATCH_SIZE = 1000;
	public static final int  MAX_ARRAY_SIZE = 10000;
    
    public static final String DIMTRADE_TABLE = "DIMTRADE";
    public static final String TRADETXT_TABLE = "TRADETXT";
    public static final String TRADETYPE_TABLE = "TRADETYPE";
    public static final String STATUSTYPE_TABLE = "STATUSTYPE";
    public static final String DIMSECURITY_TABLE = "DIMSECURITY";
    public static final String DIMCUSTOMER_TABLE = "DIMCUSTOMER";
    public static final String DIMCOMPANY_TABLE = "DIMCOMPANY";
    public static final String DIMACCOUNT_TABLE = "DIMACCOUNT";
    public static final String DIMBROKER_TABLE = "DIMBROKER";
    public static final String DIMESSAGES_TABLE = "DIMESSAGES";
    public static final String DIMDATE_TABLE = "DIMDATE";
    public static final String DIMTIME_TABLE = "DIMTIME";
    
  //public static final String DIMTRADE_FILE = "DimTrade";
    public static final String TRADETXT_FILE = "Trade.txt";
    public static final String TRADETYPE_FILE = "TradeType.txt";
    public static final String STATUSTYPE_FILE = "StatusType.txt";
    public static final String DIMSECURITY_FILE = "FINWIRE";
    public static final String DIMCUSTOMER_FILE = "CustomerMgmt_debug.txt";
    public static final String DIMCOMPANY_FILE = "FINWIRE";
    public static final String DIMACCOUNT_FILE = "CustomerMgmt_debug.txt";
    public static final String DIMBROKER_FILE = "HR.csv";
    //public static final String DIMESSAGES_FILE = "DiMessages";
    public static final String DIMDATE_FILE = "Date.txt";
    public static final String DIMTIME_FILE = "Time.txt";
    
    
    public static final String FINWIRE = "FINWIRE";
    
    public static final String LOADFILES[][] = {
    	//{DIMTRADE_TABLE, DIMTRADE_FILE},
    	//{TRADETXT_TABLE, TRADETXT_FILE},
    	{TRADETYPE_TABLE, TRADETYPE_FILE},
    	{STATUSTYPE_TABLE, STATUSTYPE_FILE},
    	{DIMCOMPANY_TABLE, DIMCOMPANY_FILE},
    	{DIMSECURITY_TABLE, DIMSECURITY_FILE},
    	{DIMDATE_TABLE, DIMDATE_FILE},
    	{DIMTIME_TABLE, DIMTIME_FILE},
    	{DIMBROKER_TABLE, DIMBROKER_FILE},
    	{DIMCUSTOMER_TABLE, DIMCUSTOMER_FILE},
    	{DIMACCOUNT_TABLE, DIMACCOUNT_FILE},
    	//{DIMESSAGES_TABLE, DIMESSAGES_FILE},
    };

    /*
    public static final int STATUSTYPE_COLS = 3;
    public static final int TRADETYPE_COLS = 5;
    public static final int DIMDATE_COLS = 9;
    public static final int DIMTIME_COLS = 5;
    public static final int DIMBROKER_COLS = 2;
    public static final int DIMSECURITY_COLS = 4;
    public static final int DIMCOMPANY_COLS = 2;
	public static final int DIMCUSTOMER_COLS = 2;
	public static final int DIMACCOUNT_COLS = 5;
	*/
}
