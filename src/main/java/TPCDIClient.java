/**
 * Created by Lexi on 3/16/16.
 */
import java.io.BufferedInputStream;
import java.net.Socket;
import java.sql.*;


public class TPCDIClient {
    private Connection c = null;
    public static final String databaseName = "Lexi";
    public static final String databaseUsername = "Lexi";
    public static final String databasePassword = "";
    public static final String getStatusSQL = "SELECT ST_NAME FROM StatusType WHERE ST_ID = ?";
    public static final String getTradeTypeSQL = "SELECT TT_NAME FROM TradeType WHERE TT_ID = ?";
    public static final String getDateIDSQL = "SELECT SK_DateID FROM DimDate WHERE DateValue = ?";
    public static final String getTimeIDSQL = "SELECT SK_TimeID FROM DimTime WHERE HourID = ? AND MinuteID = ? AND SecondID = ?";

    private String replaceWithZeroIfEmpty(String str) {
      if (str.isEmpty()) {
        return "0";
      } else {
        return str;
      }
    }

    public TPCDIClient() {
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/" + databaseName,
                            databaseUsername, databasePassword);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
    }

    private Long getDateID(String T_DTS) {
        String date = T_DTS.split(" ")[0];
        return TPCDIUtil.queryLongWithSingleParam(getDateIDSQL, c, getDateIDSQL)
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
            res = rs.getLong(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    private String getStatus(String statusID) {
        return TPCDIUtil.queryStringWithSingleParam(getStatusSQL, c, statusID);
    }

    private String getTradeType(String tradeTypeID) {
        return TPCDIUtil.queryStringWithSingleParam(getTradeTypeSQL, c, tradeTypeID);
    }

    private void run() {

        Socket clientSocket = null;
        BufferedInputStream in = null;
        int length = 0;
        try {
            clientSocket = new Socket(TPCDIConstants.STREAMINGESTOR_HOST,
                    TPCDIConstants.STREAMINGESTOR_PORT);
            clientSocket.setSoTimeout(5000);
            in = new BufferedInputStream(clientSocket.getInputStream());
            while (true) {
                length = in.read();
                if (length == -1) break;
                byte[] messageByte = new byte[length];
                in.read(messageByte);
                String tuple = new String(messageByte);

                String[] st = tuple.split("\\|", -1);

                long T_ID = new Long(st[0]);
                String T_DTS = st[1];
                String T_ST_ID = st[2];
                String T_TT_ID = st[3];
                String T_S_SYMB = st[5];
                Long dateID = getDateID(T_DTS);
                Long timeID = getTimeID(T_DTS);
                String tradeType = getTradeType(T_TT_ID);
                String status = getStatus(T_ST_ID);

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

// TODO: Insert into sp1out
//                try {
//                    Statement stmt = c.createStatement();
//                    String insertSQL = "INSERT INTO SP1out (T_ID, T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH, T_S_SYMB" +
//                            ", T_QTY, T_BID_PRICE, T_CA_ID, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, T_COMM, T_TAX, batch_id, part_id) VALUES (" +
//                            T_ID + ", \'" + st[1] + "\', \'" + st[2] + "\', \'" + st[3] + "\', " + new Short(st[4]) + ", \'" +
//                            st[5] + "\', " + new Integer(st[6]) + ", " + new Double(st[7]) + ", " + new Integer(st[8]) + ", " +
//                            st[9] + ", " + new Double(replaceWithZeroIfEmpty(st[10])) + ", " + new Double(replaceWithZeroIfEmpty(st[11])) + ", " +
//                            new Double((replaceWithZeroIfEmpty(st[12]))) + ", " + new Double(replaceWithZeroIfEmpty(st[13])) + ", " + batchid + ", " +
//                            partid + ");";
//                    stmt.execute(insertSQL);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }

                // TODO: Insert into sp2out

            }

        } catch (Exception e) {
            e.printStackTrace();
        }



    }


    public static void main(String[] args) {
        TPCDIClient client = new TPCDIClient();

        try {
            Statement stmt = c.createStatement();
            stmt.execute("DROP TABLE SP1out;");
            String sql = "CREATE TABLE SP1out " +
                    "(T_ID BIGINT NOT NULL, " +
                    " T_DTS VARCHAR(30) NOT NULL, " +
                    " T_ST_ID VARCHAR(4) NOT NULL, " +
                    " T_TT_ID VARCHAR(3) NOT NULL, " +
                    " T_IS_CASH SMALLINT NOT NULL, " +
                    " T_S_SYMB VARCHAR(15) NOT NULL, " +
                    " T_QTY INT NOT NULL, " +
                    " T_BID_PRICE FLOAT NOT NULL, " +
                    " T_CA_ID INT NOT NULL, " +
                    " T_EXEC_NAME VARCHAR(49) NOT NULL, " +
                    " T_TRADE_PRICE FLOAT, " +
                    " T_CHRG FLOAT, " +
                    " T_COMM FLOAT, " +
                    " T_TAX FLOAT, " +
                    " batch_id BIGINT NOT NULL, " +
                    " part_id int NOT NULL" +
                    ")";
            stmt.execute(sql);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Successfully create table");







    }
}
