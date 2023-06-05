import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Dstore class for storing files sent by client
 */
public class Dstore {
    static ServerSocket port = null;
    static Integer cport = null;
    static Integer timeout = null;
    static String fileFolderTxt = null;
    static Socket client = null;

    /**
     * Main method for running Dstore
     * @param args arguments given in command line, port is the main port for the client-Dstore connections. Cport is for the Dstore
     *             and the controller to connect by. Timeout is in milliseconds how long the Dstore will wait for a reply before cancelling
     *             the request. FileFolderTxt is the pathname of the storage location.
     */
    public static void main (String [] args) {
        try {
            port = new ServerSocket(Integer.parseInt(args[0]));
            cport = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            fileFolderTxt = args[3];

            deleteFilesInFolder(fileFolderTxt);

            //InetAddress address = InetAddress.getLocalHost();
            //Socket cSocket = new Socket(address, cport);

            while(true) {
                try {
                    assert port != null : "Connection is null:";
                    new Thread(new DstoreThread(port.accept())).start();
                    System.out.println("Client connected:");
                } catch (Exception e) {
                    System.err.println("Socket Accept failed: ");
                }
            }
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }
    }

    /**
     * Receives file data through a socket and saves it to the storage location
     * @param client the socket connection to the client
     * @param fileFolderTxt string of the storage location path
     * @param fileName name to save the file as
     */
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
            //client.close();

            System.out.println("File recieved and saved to: " + fileFolderTxt);
        } catch (Exception e) {
            System.err.println("error: " + e);
        }
    }

    /**
     * Waits for a STORE request through the socket, replies to confirm the requst has been recieved, and then runs
     * "storeFile" function. Times out if the file data is not sent within the specified timeout time.
     * @param client socket that receives the message
     */
    public static void ReceiveRequest(Socket client) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line;
            line = in.readLine();
            String[] lines = line.split(" ");
            switch(lines[0]) {
                case "STORE":
                    System.out.println("Storage request recieved:");
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println("ACK");
                    client.setSoTimeout(timeout);
                    try {
                        StoreFile(client, fileFolderTxt, lines[1]);
                    } catch (Exception e) {
                        System.err.println("Timeout occurred. Closing connection");
                        client.close();
                    }
                    break;
                case "LOAD_DATA":
                    System.out.println("Load request received:");
                    loadFile(client, lines[1]);
                    break;
            }
        } catch (Exception e) {
            System.err.println("Confirmation of storage failed: ");
        }
    }

    /**
     * Deletes all the files in a given folder directory
     * @param folderPath string of the folder directory to have its content deleted
     */
    public static void deleteFilesInFolder(String folderPath) {
        File folder = new File(folderPath);
        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        System.out.println("File deleted " + file.delete());
                    }
                }
            }
            System.out.println("Dstore folder emptied");
        } else {
            System.out.println("Invalid folder path!");
        }
    }

    public static void loadFile(Socket client, String fileName) {
        String path = fileFolderTxt + fileName;
        try {
            byte[] fileBytes = Files.readAllBytes(Paths.get(path));
            client.getOutputStream().write(fileBytes);
            System.out.println("File data sent");
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }


    static class DstoreThread implements Runnable {
        Socket client;

        DstoreThread(Socket s) {
            client = s;
        }

        public void run() {
            ReceiveRequest(client);
        }
    }
}