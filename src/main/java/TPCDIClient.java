/**
 * Created by Lexi on 3/16/16.
 */
import java.io.BufferedInputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class TPCDIClient {

    //if no value found replace with zero
    public static String replaceWithZeroIfEmpty(String str) {
      if (str.isEmpty()) {
        return "0";
      } else {
        return str;
      }
    }

    public static void main(String[] args) {

        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/Lexi",
                            "Lexi", "");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");


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
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Successfully create table");

        Socket clientSocket = null;
        int batchid = 0;
        int partid = 0;
        BufferedInputStream in = null;
        try {
            clientSocket = new Socket(TPCDIConstants.STREAMINGESTOR_HOST, TPCDIConstants.STREAMINGESTOR_PORT);
            clientSocket.setSoTimeout(5000);
            in = new BufferedInputStream(clientSocket.getInputStream());
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (true) {
            int length = 0;
            try {
                length = in.read();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (length == -1) break;
            byte[] messageByte = new byte[length];
            try {
                in.read(messageByte);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String tuple = new String(messageByte);
            if (tuple == "") break;

            String[] st = tuple.split("\\|", -1);
            long T_ID = new Long(st[0]);
            try {
                Statement stmt = c.createStatement();
                String insertSQL = "INSERT INTO SP1out (T_ID, T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH, T_S_SYMB" +
                        ", T_QTY, T_BID_PRICE, T_CA_ID, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, T_COMM, T_TAX, batch_id, part_id) VALUES (" +
                        T_ID + ", \'" + st[1] + "\', \'" + st[2] + "\', \'" + st[3] + "\', " + new Short(st[4]) + ", \'" +
                        st[5] + "\', " + new Integer(st[6]) + ", " + new Double(st[7]) + ", " + new Integer(st[8]) + ", " +
                        st[9] + ", " + new Double(replaceWithZeroIfEmpty(st[10])) + ", " + new Double(replaceWithZeroIfEmpty(st[11])) + ", " +
                        new Double((replaceWithZeroIfEmpty(st[12]))) + ", " + new Double(replaceWithZeroIfEmpty(st[13])) + ", " + batchid + ", " +
                        partid + ");";
                stmt.execute(insertSQL);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }
}
