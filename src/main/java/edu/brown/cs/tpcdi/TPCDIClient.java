package edu.brown.cs.tpcdi;
/**
 * Created by Lexi on 3/16/16.
 */
import javafx.util.Pair;

import java.io.BufferedInputStream;
import java.net.Socket;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class TPCDIClient {
    private Connection c = null;
    public static final String getStatusSQL = "SELECT ST_NAME FROM StatusType WHERE ST_ID = ?";
    public static final String getTradeTypeSQL = "SELECT TT_NAME FROM TradeType WHERE TT_ID = ?";
    public static final String getDateIDSQL = "SELECT SK_DateID FROM DimDate WHERE CAST(DateValue AS TEXT) LIKE ?;";
    public static final String getTimeIDSQL = "SELECT SK_TimeID FROM DimTime WHERE HourID = ? AND MinuteID = ? AND SecondID = ?";
    public static final String getSecurityIDSQL = "SELECT SK_SecurityID, SK_CompanyID FROM DimSecurity WHERE CAST(Symbol AS TEXT) LIKE ?";
    public static final String getAccountInfoSQL = "SELECT SK_AccountID, SK_CustomerID,SK_BrokerID FROM DimAccount WHERE AccountID = ?";
    public static final String insertDimTrade = "INSERT INTO DimTrade "
                    + "(TradeID,SK_BrokerID,SK_CreateDateID,SK_CreateTimeID,SK_CloseDateID,SK_CloseTimeID,Status,Type,"
                    + "CashFlag,SK_SecurityID,SK_CompanyID,Quantity,BidPrice,SK_CustomerID,SK_AccountID,"
                    + "ExecutedBy,TradePrice,Fee,Commission,Tax)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

    private HashMap<String, AtomicInteger> successCountMap;
    private HashMap<String, AtomicInteger> failureCountMap;

    private String replaceWithZeroIfEmpty(String str) {
      if (str.isEmpty()) {
        return "0";
      } else {
        return str;
      }
    }

    public TPCDIClient(Connection dbConn) {
        successCountMap = new HashMap<String, AtomicInteger>();
        failureCountMap = new HashMap<String, AtomicInteger>();

        for (String[] s : TPCDIConstants.LOADFILES) {
            // init row counts for all tables to be loaded
            successCountMap.put(s[0], new AtomicInteger());
            failureCountMap.put(s[0], new AtomicInteger());
        }

        try {
            c = dbConn;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
    }

    private Long getDateID(String T_DTS) {
        String date = T_DTS.split(" ")[0];
        Long res = TPCDIUtil.queryLongWithSingleParam(getDateIDSQL, c, "%"+date+"%");
        if (res == -1) {
            failureCountMap.get(TPCDIConstants.DIMDATE_TABLE).incrementAndGet();
        } else {
            successCountMap.get(TPCDIConstants.DIMDATE_TABLE).incrementAndGet();
        }
        return res;
    }

    private Long getTimeID(String T_DTS) {
        String[] timestamp = T_DTS.split(" ")[1].split(":");
        int hour = new Integer(timestamp[0]);
        int min = new Integer(timestamp[1]);
        int sec = new Integer(timestamp[2]);
        Long res = null;
        try {
            PreparedStatement ps = c.prepareStatement(getTimeIDSQL);
            ps.setInt(1, hour);
            ps.setInt(2, min);
            ps.setInt(3, sec);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                successCountMap.get(TPCDIConstants.DIMTIME_TABLE).incrementAndGet();
                res = rs.getLong(1);
            } else {
                failureCountMap.get(TPCDIConstants.DIMTIME_TABLE).incrementAndGet();
                res = (long)-1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    private String getStatus(String statusID) {
        String res = TPCDIUtil.queryStringWithSingleParam(getStatusSQL, c, statusID);
        if (res == "") {
            failureCountMap.get(TPCDIConstants.STATUSTYPE_TABLE).incrementAndGet();
        } else {
            successCountMap.get(TPCDIConstants.STATUSTYPE_TABLE).incrementAndGet();
        }
        return res;
    }

    private String getTradeType(String tradeTypeID) {
        String res = TPCDIUtil.queryStringWithSingleParam(getTradeTypeSQL, c, tradeTypeID);
        if (res == "") {
            failureCountMap.get(TPCDIConstants.TRADETYPE_TABLE).incrementAndGet();
        } else {
            successCountMap.get(TPCDIConstants.TRADETYPE_TABLE).incrementAndGet();
        }
        return res;
    }

    private Pair<Long, Long> getSecurityID(String symbol) {
        try {
            PreparedStatement ps = c.prepareStatement(getSecurityIDSQL);
            ps.setString(1, "%"+symbol+"%");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                successCountMap.get(TPCDIConstants.DIMSECURITY_TABLE).incrementAndGet();
                return new Pair(rs.getLong(1), rs.getLong(2));
            } else {
                failureCountMap.get(TPCDIConstants.DIMSECURITY_TABLE).incrementAndGet();
                return new Pair((long)-1, (long)-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private ArrayList<Long> getAccountInfo(int accountID) {
        try {
            PreparedStatement ps = c.prepareStatement(getAccountInfoSQL);
            ps.setInt(1, accountID);
            ResultSet rs = ps.executeQuery();
            ArrayList<Long> res = new ArrayList<Long>();
            if (rs.next()) {
                res.add(0, rs.getLong(1));
                res.add(1, rs.getLong(2));
                res.add(2, rs.getLong(3));

                successCountMap.get(TPCDIConstants.DIMACCOUNT_TABLE).incrementAndGet();
            } else {
                res.add(0, (long)-1);
                res.add(1, (long)-1);
                res.add(2, (long)-1);

                failureCountMap.get(TPCDIConstants.DIMACCOUNT_TABLE).incrementAndGet();
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void createTable() {
        try {
            Statement stmt = c.createStatement();
            stmt.execute("DROP TABLE IF EXISTS DimTrade;");
            String sql = "CREATE TABLE DimTrade (" +
                    "    TradeID          bigint      NOT NULL,\n" +
                    "    SK_BrokerID      bigint      ,\n" +
                    "    SK_CreateDateID  bigint      NOT NULL,\n" +
                    "    SK_CreateTimeID  bigint      NOT NULL,\n" +
                    "    SK_CloseDateID   bigint      ,\n" +
                    "    SK_CloseTimeID   bigint      ,\n" +
                    "    Status           char(10)    NOT NULL,\n" +
                    "    Type             char(12)    NOT NULL,\n" +
                    "    CashFlag         smallint    NOT NULL,\n" +
                    "    SK_SecurityID    bigint      NOT NULL,\n" +
                    "    SK_CompanyID     bigint      NOT NULL,\n" +
                    "    Quantity         int         NOT NULL,\n" +
                    "    BidPrice         float     NOT NULL,\n" +
                    "    SK_CustomerID    bigint      NOT NULL,\n" +
                    "    SK_AccountID     bigint      NOT NULL,\n" +
                    "    ExecutedBy       varchar(64) NOT NULL,\n" +
                    "    TradePrice       float     ,\n" +
                    "    Fee              float     ,\n" +
                    "    Commission       float     ,\n" +
                    "    Tax              float     " +
                    ");";
            stmt.execute(sql);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Successfully Created the Destination Table: DimTrade");
    }

    public void run() {
        Socket clientSocket = null;
        BufferedInputStream in = null;
        int length = 0;
        try {
            clientSocket = new Socket(TPCDIConstants.STREAMINGESTOR_HOST,
                    TPCDIConstants.STREAMINGESTOR_PORT);
            clientSocket.setSoTimeout(5000);
            in = new BufferedInputStream(clientSocket.getInputStream());

            long startTime = System.nanoTime();
            int count = 0;
            while (true) {
                length = in.read();
                if (length == -1) break;
                byte[] messageByte = new byte[length];
                in.read(messageByte);
                String tuple = new String(messageByte);
                if (tuple.trim().equals("")) break;


                String[] st = tuple.split("\\|", -1);
                if (st[0].trim().equals("")) break;

                count++;
                if (count % 1000 == 0) {
                    System.out.println("We have loaded " + count + " tuples");
                    long tmpTime = System.nanoTime();
                    long sec = (tmpTime - startTime) / 1000000000;
                    System.out.println(sec + " seconds since we start");
                    System.out.println("Throughput: " + (double)count / sec + " tuples/sec");
                }

                long T_ID = new Long(st[0]);
                String T_DTS = st[1];
                String T_ST_ID = st[2];
                String T_TT_ID = st[3];
                short T_IS_CASH = new Short(st[4]);
                String T_S_SYMB = st[5];
                // Previous StoreProcedure 2
                Long dateID = getDateID(T_DTS);
                Long timeID = getTimeID(T_DTS);
                String tradeType = getTradeType(T_TT_ID);
                String status = getStatus(T_ST_ID);

                int T_QTY = new Integer(st[6]);
                double T_BID_PRICE = new Double(st[7]);
                int T_CA_ID = new Integer(st[8]);
                String T_EXEC_NAME = st[9];
                double T_TRADE_PRICE = new Double(replaceWithZeroIfEmpty(st[10]));
                double T_CHRG = new Double(replaceWithZeroIfEmpty(st[11]));
                double T_COMM = new Double(replaceWithZeroIfEmpty(st[12]));
                double T_TAX = new Double(replaceWithZeroIfEmpty(st[13]));

                long SK_CreateDateID = -1;
                long SK_CreateTimeID = -1;
                long SK_CloseDateID = -1;
                long SK_CloseTimeID = -1;

                if((T_ST_ID.equals("SBMT") && (T_TT_ID.equals("TMB") || T_TT_ID.equals("TMS"))) || T_ST_ID.equals("PNDG")) {
                    SK_CreateDateID = dateID;
                    SK_CreateTimeID = timeID;
                } else if (T_ST_ID.equals("CMPT") || T_ST_ID.equals("CNCL")) {
                    SK_CloseDateID = dateID;
                    SK_CloseTimeID = timeID;
                }

                // Previous StoreProcedure 3
                Pair<Long, Long> tmp = getSecurityID(T_S_SYMB);
                long SK_SecurityID = tmp.getKey();
                long SK_CompanyID = tmp.getValue();

                // Previous StoreProcedure 4
                ArrayList<Long> tmp2 = getAccountInfo(T_CA_ID);
                long SK_AccountID = tmp2.get(0);
                long SK_CustomerID = tmp2.get(1);
                long SK_BrokerID = tmp2.get(2);

                // Final Insertion
                PreparedStatement ps = c.prepareStatement(insertDimTrade);
                ps.setLong(1, T_ID);
                ps.setLong(2, SK_BrokerID);
                ps.setLong(3, SK_CreateDateID);
                ps.setLong(4, SK_CreateTimeID);
                ps.setLong(5, SK_CloseDateID);
                ps.setLong(6, SK_CloseTimeID);
                ps.setString(7, status);
                ps.setString(8, tradeType);
                ps.setShort(9, T_IS_CASH);
                ps.setLong(10, SK_SecurityID);
                ps.setLong(11, SK_CompanyID);
                ps.setInt(12, T_QTY);
                ps.setDouble(13, T_BID_PRICE);
                ps.setLong(14, SK_CustomerID);
                ps.setLong(15, SK_AccountID);
                ps.setString(16, T_EXEC_NAME);
                ps.setDouble(17, T_TRADE_PRICE);
                ps.setDouble(18, T_CHRG);
                ps.setDouble(19, T_COMM);
                ps.setDouble(20, T_TAX);
                ps.execute();
            }

            Long endTime = System.nanoTime();
            long sec = (endTime - startTime) / 1000000000;
            System.out.println(sec + " seconds in total.");
            System.out.println(count+ " tuples.");
            System.out.println("Throughput: " + (double)count / sec + " tuples/sec");
            System.out.println("|TABLENAME|NUM OF SUCC LOOKUP|NUM OF FAIL LOOKUP|SUCCESS RATIO");
            for (String[] s : TPCDIConstants.LOADFILES) {
                // init row counts for all tables to be loaded
                AtomicInteger a = successCountMap.get(s[0]);
                AtomicInteger b = failureCountMap.get(s[0]);
                System.out.printf("%11s %14d %20d %9f%%\n", s[0], a.intValue(), b.intValue(), a.doubleValue()/(a.doubleValue()+b.doubleValue())*100);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}
