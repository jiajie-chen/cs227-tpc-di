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
}
