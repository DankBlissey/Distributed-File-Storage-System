import java.io.*;
import java.net.*;
import java.nio.Buffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Dstore class for storing files sent by client
 */
public class Dstore {
    static ServerSocket port = null;
    static String portText = null;
    static Integer cport = null;
    static Integer timeout = null;
    static String fileFolderTxt = null;
    static Socket controller = null;
    static BufferedReader IN;
    static PrintWriter OUT;
    static CountDownLatch cd;

    static synchronized CountDownLatch getCountDownLatch() {
        return cd;
    }

    static synchronized void countDown() {
        cd.countDown();
    }

    static synchronized void setCountDownLatch(int i) {
        cd = new CountDownLatch(i);
    }

    static synchronized Socket getController() {
        return controller;
    }

    static synchronized BufferedReader getIN() {
        return IN;
    }

    static synchronized  PrintWriter getOUT() {
        return OUT;
    }

    /**
     * Main method for running Dstore
     * @param args arguments given in command line, port is the main port for the client-Dstore connections. Cport is for the Dstore
     *             and the controller to connect by. Timeout is in milliseconds how long the Dstore will wait for a reply before cancelling
     *             the request. FileFolderTxt is the pathname of the storage location.
     */
    public static void main (String [] args) {
        try {
            portText = args[0];
            port = new ServerSocket(Integer.parseInt(portText));
            cport = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            fileFolderTxt = args[3];
            System.out.println("Initial variables set");

            deleteFilesInFolder(fileFolderTxt);
            System.out.println("Attempting to get address");
            InetAddress address = InetAddress.getLocalHost();
            System.out.println("Attempting to add controller socket");
            controller = new Socket(address, cport);
            if(getController().isClosed()) {
                System.out.println("controller socket closed after being made");
            }
            System.out.println("Attempting to create printwriter");
            OUT = new PrintWriter(getController().getOutputStream(), true);
            IN = new BufferedReader(new InputStreamReader(getController().getInputStream()));
            if(getController().isClosed()) {
                System.out.println("controller socket closed after in and out made");
            }
            System.out.println("Attempting to join");
            getOUT().println("JOIN " + portText);
            if(getController().isClosed()) {
                System.out.println("controller socket closed after out.println");
            }
            System.out.println("join attempted");

            new Thread(new DstoreControllerThread()).start();

            while(true) {
                try {
                    Socket c = port.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    PrintWriter out = new PrintWriter(c.getOutputStream());
                    new Thread(new DstoreThread(c,in,out)).start();
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
    public static void StoreFile(Socket client, String fileFolderTxt, String fileName, BufferedReader in, PrintWriter out) {
        try {
            System.out.println("Client Connected for storage: " + client.getInetAddress().getHostAddress() + " " + client.getPort());
            generalStore(client,fileFolderTxt,fileName,in,out);
            System.out.println("File received and saved to: " + fileFolderTxt);
            getOUT().println("STORE_ACK " + fileName);
            System.out.println("Storage Acknowledgement of file " + fileName + " sent to controller");
        } catch (Exception e) {
            System.err.println("error: " + e);
        }
    }

    /**
     * Waits for a STORE request through the socket, replies to confirm the requst has been recieved, and then runs
     * "storeFile" function. Times out if the file data is not sent within the specified timeout time.
     * @param client socket that receives the message
     */
    public static void ReceiveRequest(Socket client, BufferedReader in, PrintWriter out) {
        try {
            String line;
            line = in.readLine();
            String[] lines = line.split(" ");
            switch (lines[0]) {
                case "STORE" -> {
                    System.out.println("Storage request recieved:");
                    out.println("ACK");
                    out.flush();
                    client.setSoTimeout(timeout);
                    try {
                        StoreFile(client, fileFolderTxt, lines[1], in, out);
                    } catch (Exception e) {
                        System.err.println("Timeout occurred. Closing connection");
                    }
                }
                case "LOAD_DATA" -> {
                    System.out.println("Load request received:");
                    loadFile(client, lines[1]);
                }
                case "REBALANCE_STORE" -> {
                    System.out.println("Rebalance store request recieved");
                    out.println("ACK");
                    out.flush();
                    client.setSoTimeout(timeout);
                    rebalanceStore(client,fileFolderTxt, lines[1], in, out);
                }
                default -> System.err.println("Malformed client message received, message was: " + lines[0]);
            }
        } catch (Exception e) {
            System.err.println("Confirmation of storage failed: ");
        }
    }

    public static void generalStore(Socket client, String fileFolderTxt, String fileName, BufferedReader in, PrintWriter out) {
        try {
            InputStream inputStream = client.getInputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            Path destinationPath = Path.of(fileFolderTxt + fileName);
            FileOutputStream fileOutputStream = new FileOutputStream(destinationPath.toString());

            while ((bytesRead = inputStream.readNBytes(buffer, 0, buffer.length)) > 0) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
            fileOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void rebalanceStore(Socket client, String fileFolderTxt, String fileName, BufferedReader in, PrintWriter out) {
        try {
            System.out.println("Dstore Connected for rebalance storage: " + client.getInetAddress().getHostAddress() + " " + client.getPort());
            generalStore(client, fileFolderTxt, fileName, in, out);
            System.out.println("Rebalance file received and saved to: " + fileFolderTxt);
        } catch (Exception e) {
            System.err.println("error: " + e);
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

    public static void removeFile(String fileName) {
        String folderPath = fileFolderTxt + fileName;
        File file = new File(folderPath);
        try {
            if(file.exists()) {
                System.out.println("File deleted " + file.delete());
                getOUT().println("REMOVE_ACK " + fileName);
            } else {
                getOUT().println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void rebalanceRemove(String fileName) {
        String folderPath = fileFolderTxt + fileName;
        File file = new File(folderPath);
        try {
            if(file.exists()) {
                System.out.println("File deleted " + file.delete());
            } else {
                System.out.println("Error file " + fileName + " does not exist");
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes a filename, finds that file within the Dstore storage location, and then outputs its content to the client
     * @param client socket for the client connection
     * @param fileName name of the file they want to retrieve
     */
    public static void loadFile(Socket client, String fileName) {
        String path = fileFolderTxt + fileName;
        if(new File(path).exists()) {
            try {
                byte[] fileBytes = Files.readAllBytes(Paths.get(path));
                client.getOutputStream().write(fileBytes);
                System.out.println("File data sent");
            } catch (Exception e) {
                System.err.println("Error: " + e);
            }
        } else {
            try {
                client.close();
            } catch (Exception e) {
                System.err.println("Socket could not be closed/was already closed: " + e);
            }
        }
    }


    public static void listenController() {
        try {
            String input;
            while((input = getIN().readLine()) != null) {
                System.out.println("controller input is not null");
                System.out.println("Controller message received of: " + input);
                //while(!getIN().ready()) {
                    //System.out.println("in not ready in listenController");
                    //wait for IN to be ready
                    //may not be required once the programs are set up to not close the input or output streams
                //}
                System.out.println("buffered reader is ready");
                String[] lines = input.split(" ");
                switch (lines[0]) {
                    case "LIST" -> {
                        System.out.println("List request from controller received");
                        File location = new File(fileFolderTxt);
                        File[] fileList = location.listFiles();
                        synchronized (Dstore.class) {
                            getOUT().print("LIST");
                            getOUT().flush();
                            if(fileList != null) {
                                for(File x : fileList) {
                                    getOUT().print(" " + x.getName());
                                }
                            }
                            getOUT().println("");
                            getOUT().flush();
                        }
                    }
                    case "REMOVE" -> {
                        String fileName = lines[1];
                        removeFile(fileName);
                    }
                    case "REBALANCE" -> {
                        if(lines.length > 1) {
                            System.out.println("Rebalance message received from controller");
                            //index is the pointer that reads the lines of the received string
                            int index = 1;
                            // first number after the rebalance message says how many files are included in the files to send
                            int filesSent = Integer.parseInt(lines[index]);
                            // creates a list of the files to remove and a hashmap of the filesToSend as each file has a list of which Dstores to send to
                            HashMap<String, List<String>> filesToSend = new HashMap<>();
                            List<String> filesToRemove = new ArrayList<>();
                            //this for loop parses the part of the message that includes which files to send
                            for(int numberOfFilesSent = filesSent; numberOfFilesSent > 0; numberOfFilesSent--) {
                                index++;
                                String fileName = lines[index];
                                List<String> dstoresToSendList = new ArrayList<>();
                                index++;
                                int dstoreToSend = Integer.parseInt(lines[index]);
                                for(int numberOfDstoresToSend = dstoreToSend; numberOfDstoresToSend > 0; numberOfDstoresToSend--) {
                                    index++;
                                    dstoresToSendList.add(lines[index]);
                                }
                                filesToSend.put(fileName, dstoresToSendList);
                            }
                            index++;
                            int removesSent = Integer.parseInt(lines[index]);
                            //this for loop parsees the part of the message that includes which files to remove
                            for(int numberOfRemovesSent = removesSent; numberOfRemovesSent > 0; numberOfRemovesSent--) {
                                index++;
                                filesToRemove.add(lines[index]);
                            }
                            int totalSendRequests = multiplyHashMap(filesToSend);
                            setCountDownLatch(totalSendRequests);

                            InetAddress address = InetAddress.getLocalHost();
                            for(Map.Entry<String, List<String>> m : filesToSend.entrySet()) {
                                for(String po : m.getValue()) {
                                    Socket DstoreSo = new Socket(address, Integer.parseInt(po));
                                    PrintWriter out = new PrintWriter(DstoreSo.getOutputStream());
                                    BufferedReader in = new BufferedReader(new InputStreamReader(DstoreSo.getInputStream()));
                                    String fileName = m.getKey();
                                    String path = fileFolderTxt + fileName;
                                    File fileSending = new File(path);
                                    long fileLength = fileSending.length();
                                    new Thread(new RebalanceThread(DstoreSo, in, out, fileName, fileLength)).start();
                                }
                            }
                            boolean ack = getCountDownLatch().await(timeout, TimeUnit.MILLISECONDS);
                            if(ack) {
                                for(String f : filesToRemove) {
                                    System.out.println("Removing File " + f);
                                    rebalanceRemove(f);
                                }
                                OUT.println("REBALANCE_COMPLETE");
                            } else {
                                for(String f : filesToRemove) {
                                    System.out.println("Removing File " + f);
                                    rebalanceRemove(f);
                                }
                            }
                        } else {
                            System.out.println("Rebalance message received for nothing");
                            OUT.println("REBALANCE_COMPLETE");
                        }
                    }
                    default -> System.err.println("Malformed controller message received, message was: " + lines[0]);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int multiplyHashMap(HashMap<String, List<String>> hash) {
        int i = 0;
        for(Map.Entry<String, List<String>> m : hash.entrySet()) {
            i += m.getValue().size();
        }
        return i;
    }

    static class RebalanceThread implements Runnable {
        Socket Dstore;
        BufferedReader in;
        PrintWriter out;
        String fileName;
        long fileSize;


        RebalanceThread(Socket s, BufferedReader in, PrintWriter out, String fileName, long fileSize) {
            this.Dstore = s;
            this.in = in;
            this.out = out;
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        public void run() {
            try {
                System.out.println("Sending rebalance message to " + Dstore.getPort());
                out.println("REBALANCE_STORE " + fileName + " " + fileSize);
                byte[] fileBytes;
                String path = fileFolderTxt + fileName;
                fileBytes = Files.readAllBytes(Paths.get(path));
                Dstore.setSoTimeout(timeout);
                System.out.println("Reading acknowledgement line");
                String ack = in.readLine();
                System.out.println("Line read");
                if(ack.equals("ACK")) {
                    Dstore.getOutputStream().write(fileBytes);
                    countDown();
                    System.out.println("File data sent!");
                } else {
                    System.err.println("Error: malformed acknowledgement message from " + Dstore.getPort());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Thread for concurrency of client requests
     */
    static class DstoreThread implements Runnable {
        Socket client;
        BufferedReader in;
        PrintWriter out;

        /**
         * Creates the thread, with the client socket
         * @param s the socket the client connects to
         */
        DstoreThread(Socket s, BufferedReader in, PrintWriter out) {
            this.client = s;
            this.in = in;
            this.out = out;
        }

        /**
         * tries to run the method to receive a request
         */
        public void run() {
            ReceiveRequest(client, in, out);
            try {
                client.close();
            } catch (Exception e) {
                System.err.println("Error: " + e);
            }
        }
    }

    static class DstoreControllerThread implements Runnable {


        DstoreControllerThread() {

        }

        public void run() {
            listenController();
        }
    }
}