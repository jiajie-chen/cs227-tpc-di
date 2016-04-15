/**
 * Created by Lexi on 3/16/16.
 */
import java.io.BufferedInputStream;
import java.net.Socket;

public class TPCDIClient {

    public static void main(String[] args) {

        Socket clientSocket = null;
        try {
            clientSocket = new Socket(TPCDIConstants.STREAMINGESTOR_HOST, TPCDIConstants.STREAMINGESTOR_PORT);
            clientSocket.setSoTimeout(5000);

            BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

            while (true) {
                int length = in.read();
                if (length == -1) break;
                byte[] messageByte = new byte[length];
                in.read(messageByte);
                String tuple = new String(messageByte);
                String[] curTuple = new String[1];
                curTuple[0] = tuple;
                System.out.println(tuple);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
