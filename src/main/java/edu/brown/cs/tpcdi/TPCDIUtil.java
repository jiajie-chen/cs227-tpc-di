package edu.brown.cs.tpcdi;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by Lexi on 3/16/16.
 */
public class TPCDIUtil {
    public static int hashCode(String key, int numPartitions) {
        return (Math.abs(key.hashCode()) % numPartitions);
    }

    public static int hashCode(String key) {
        return key.hashCode();
    }

    public static String queryStringWithSingleParam(String sql, Connection c, String param) {
        String res = null;
        try {
            PreparedStatement ps = c.prepareStatement(sql);
            ps.setString(1, param);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                res = rs.getString(1);
            } else {
                res = "";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public static Long queryLongWithSingleParam(String sql, Connection c, String param) {
        Long res = null;
        try {
            PreparedStatement ps = c.prepareStatement(sql);
            ps.setString(1, param);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                res = rs.getLong(1);
            } else {
                res = (long)-1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }
}
