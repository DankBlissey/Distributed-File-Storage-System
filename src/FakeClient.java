import java.io.*;
import java.net.*;

public class FakeClient {
    public static void main(String [] args) {
        Socket socket = null;
        try {
            InetAddress address = InetAddress.getLocalHost();
            socket = new Socket(address, Integer.parseInt(args[0]));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            for(int i = 0; i < 10; i++) {
                out.println("TCP message "+i); //out.flush();
                Thread.sleep(1000);
            }
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (socket != null)
                try { socket.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }
}
