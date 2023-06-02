import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

public class Dstore {
    static ServerSocket port = null;
    static ServerSocket cport = null;
    static Integer timeout = null;
    static String fileFolderTxt = null;
    static Socket client = null;
    public static void main (String [] args) {
        try {
            port = new ServerSocket(Integer.parseInt(args[0]));
            cport = new ServerSocket(Integer.parseInt(args[1]));
            timeout = Integer.parseInt(args[2]);
            fileFolderTxt = args[3];
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }

        try {
            assert port != null : "Connection is null:";
            client = port.accept();
            System.out.println("Client connected:");
        } catch (Exception e) {
            System.err.println("Socket Accept failed: ");
        }
        ReceiveRequest(client);
    }

    public static void StoreFile(Socket client, String fileFolderTxt, String fileName) {
        try {
            System.out.println("Client Connected: " + client.getInetAddress().getHostAddress());
            InputStream inputStream = client.getInputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            Path destinationPath = Path.of(fileFolderTxt + fileName);
            FileOutputStream fileOutputStream = new FileOutputStream(destinationPath.toString());

            while ((bytesRead = inputStream.readNBytes(buffer, 0, buffer.length)) > 0) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }

            fileOutputStream.close();
            client.close();

            System.out.println("File recieved and saved to: " + fileFolderTxt);
        } catch (Exception e) {
            System.err.println("error: " + e);
        }
    }

    public static void ReceiveRequest(Socket client) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line;
            while((line = in.readLine()) != null) {
                String[] lines = line.split(" ");
                if(Objects.equals(lines[0], "STORE") && lines.length == 3) {
                    System.out.println("Storage request recieved:");
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println("ACK");
                    Timer timer = new Timer();
                    timer.schedule(new TimerTask() {
                        @Override
                        public void run() {
                            // Timeout occurred, handle it
                            System.out.println("Timeout occurred. Closing connection.");
                            try {
                                in.close();
                                out.close();
                                client.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }, timeout);
                    timer.cancel();
                    StoreFile(client, fileFolderTxt, lines[1]);
                }
            }
        } catch (Exception e) {
            System.err.println("Confirmation of storage failed: ");
        }
    }


}
