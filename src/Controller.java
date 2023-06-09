import java.net.ServerSocket;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;


public class Controller {
    static ServerSocket cport;
    static Integer R;
    static Integer timeout;
    static Integer rebalancePeriod;
    //static ArrayList<FileIndex> Index;
    static HashMap<String, FileIndex> Index = new HashMap<>();
    static HashMap<String, Socket> DstoreList = new HashMap<>();
    static ThreadLocal<Integer> indexToStore = new ThreadLocal<>();

    public static void setIndexToStore(int value) {
        indexToStore.set(value);
    }

    public static int getIndexToStore() {
        return indexToStore.get();
    }

    public static synchronized List<String> getIndexDstores(String fileName) {
        return Index.get(fileName).getDstoreAllocation();
    }

    public static synchronized void addToIndex(FileIndex f) {
        Index.put(f.getFileName(),f);
    }

    public static synchronized void removeFromIndex(String fileName) {
        Index.remove(fileName);
    }

    public static synchronized void updateIndexStatus(String fileName, String status) {
        try {
            Index.get(fileName).setStatus(status);
        } catch (Exception e) {
            System.err.println("Typo in status update for index");
        }
    }

    public static synchronized void countdownIndex(String fileName) {
        Index.get(fileName).getCountDownLatch().countDown();
    }

    public static synchronized CountDownLatch getLatch(String fileName) {
        return Index.get(fileName).getCountDownLatch();
    }

    public static synchronized void setLatch(String fileName, int i) {
        Index.get(fileName).setCountDownLatch(i);
    }

    public static synchronized ArrayList<FileIndex> getIndexFiles() {
        return new ArrayList<>(Index.values());
    }

    public static synchronized HashMap<String, FileIndex> getIndex() {
        return Index;
    }

    public static synchronized void addToDstoreList(String s, Socket c)  {
        DstoreList.put(s,c);
    }

    public static synchronized Socket getDstore(String s) {
        return DstoreList.get(s);
    }

    public static synchronized HashMap<String, Socket> getDstoreList() {
        return DstoreList;
    }

    public static synchronized FileIndex getIndexFile(String fileName) {
        return Index.get(fileName);
    }

    public static void main(String[] args) {
        try {
            cport = new ServerSocket(Integer.parseInt(args[0]));
            R = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            rebalancePeriod = Integer.parseInt(args[3]);
            System.out.println("Starting");

            while(true) {
                try {
                    System.out.println("New connection being accepted");
                    new Thread(new controllerThread(cport.accept())).start();
                    System.out.println("Client connected:");
                } catch (Exception e) {
                    System.err.println("Error: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }

    }

    public static void receive(Socket c) {
        try {
            System.out.println("Reading input");
            BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream()));
            System.out.println("Reading line");
            String line = in.readLine();
            System.out.println("Line read");
            String[] lines = line.split(" ");
            switch (lines[0]) {
                case "JOIN" -> {
                    System.out.println("Dstore connected with port " + lines[1]);
                    if(c.isClosed()) {
                        System.out.println("socket closed before adding to dstorelist");
                    }
                    addToDstoreList(lines[1], c);
                    if(c.isClosed()) {
                        System.out.println("socket closed after adding to dstorelist");
                    }
                    System.out.println("Added to the DstoreList, now closing input");
                    System.out.println("Input closed, now running receive method");
                    recieveDstoreMsg(c);

                }
                //c.setSoTimeout(timeout);
                //re-balance could go here?
                case "STORE" -> {
                    System.out.println("Client store request");
                    synchronized (Controller.class) {
                        if (getDstoreList().size() >= R) {
                            if (!isFileInIndex(lines[1])) {
                                List<String> Dgo = selectDstores();
                                String fileName = lines[1];
                                Integer fileSize = Integer.parseInt(lines[2]);
                                addToIndex(new FileIndex(fileName, fileSize, Dgo));
                                allocateDstores(c, Dgo);
                            } else {
                                System.err.println("Error with storage request: file already exists");
                                PrintWriter out = new PrintWriter(c.getOutputStream(), true);
                                out.println("ERROR_FILE_ALREADY_EXISTS");
                                c.close();
                            }
                        } else {
                            System.err.println("Error with storage request: not enough Dstores");
                            PrintWriter out = new PrintWriter(c.getOutputStream(), true);
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            c.close();
                        }
                    }
                    String fileName = lines[1];
                    try {
                        Boolean acknow = getLatch(fileName).await(timeout, TimeUnit.MILLISECONDS);
                        if(acknow.equals(true)) {
                            updateIndexStatus(fileName, "store complete");
                            PrintWriter out = new PrintWriter(c.getOutputStream(), true);
                            out.println("STORE_COMPLETE");
                            setLatch(fileName, R);
                            c.close();
                        } else {
                            removeFromIndex(fileName);
                            c.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        removeFromIndex(fileName);
                        c.close();
                    }
                }
                case "LOAD" -> {
                    setIndexToStore(0);
                    PrintWriter out = new PrintWriter(c.getOutputStream(), true);
                    String fileName = lines[1];
                    FileIndex file = new FileIndex(getIndexFile(fileName));
                    List<String> dstores = file.getDstoreAllocation();
                    if(!dstores.isEmpty()) {
                        if(getDstoreList().size() >= R) {
                            out.println("LOAD_FROM " + dstores.get(0) + " " + file.getFileSize().toString());
                            receive(c);
                        } else {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            c.close();
                        }
                    } else {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                        c.close();
                    }
                }
                case "RELOAD" -> {
                    setIndexToStore(getIndexToStore()+1);
                    PrintWriter out = new PrintWriter(c.getOutputStream(), true);
                    String fileName = lines[1];
                    FileIndex file = new FileIndex(getIndexFile(fileName));
                    List<String> dstores = file.getDstoreAllocation();
                    if(dstores.size() > getIndexToStore()) {
                        if(!dstores.isEmpty()) {
                            if(getDstoreList().size() >= R) {
                                out.println("LOAD_FROM " + dstores.get(getIndexToStore()) + " " + file.getFileSize().toString());
                                receive(c);
                            } else {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                c.close();
                            }
                        } else {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                            c.close();
                        }
                    } else {
                        out.println("ERROR_LOAD");
                        c.close();
                    }

                }
                case "REMOVE" -> {
                    System.out.println("client remove request: ");
                    String fileName = lines[1];
                    PrintWriter outC = new PrintWriter(c.getOutputStream());
                    synchronized (Controller.class) {
                        if (getDstoreList().size() >= R) {
                            if (isFileInIndex(lines[1])) {
                                if (getIndexFile(fileName).getStatus().equals("store complete")) {
                                    updateIndexStatus(fileName, "remove in progress");
                                    List<String> DstoresWFile = getIndexFile(fileName).getDstoreAllocation();
                                    for(String s : DstoresWFile) {
                                        PrintWriter out = new PrintWriter(getDstore(s).getOutputStream(), true);
                                        out.println("REMOVE " + fileName);
                                    }
                                } else {
                                    System.err.println("File requested to be removed was not a fully stored file");
                                }
                            } else {
                                outC.println("ERROR_FILE_DOES_NOT_EXIST");
                                outC.close();
                            }
                        } else {
                            outC.println("ERROR_NOT_ENOUGH_DSTORES");
                            outC.close();
                        }
                    }
                    try {
                        Boolean acknow = getLatch(fileName).await(timeout, TimeUnit.MILLISECONDS);
                        if(acknow.equals(true)) {
                            updateIndexStatus(fileName, "remove complete");
                            outC.println("REMOVE_COMPLETE");
                            c.close();
                        } else {
                            System.err.println("timeout with receiving acknowledgement of removal");
                            c.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        c.close();
                    }
                }
                case "LIST" -> {
                    PrintWriter out = new PrintWriter(c.getOutputStream());
                    ArrayList<FileIndex> index = new ArrayList<>(getIndexFiles());
                    for (FileIndex f : index) {
                        out.print(f.getFileName() + " ");
                    }
                    out.flush();
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }

    public static void recieveDstoreMsg(Socket c) {
        try {
            while(true) {
                System.out.println("Creating bufferedReader for recieving Dstore Messages");
                if(c.isClosed()) {
                    System.err.println("socket is closed in recieveDstoreMsg");
                    break;
                }
                BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream()));
                System.out.println("buffered reader created");
                while(!in.ready()) {

                }
                System.out.println("in is ready");
                String line = in.readLine();
                String[] lines = line.split(" ");
                switch(lines[0]) {
                    case "STORE_ACK", "REMOVE_ACK" -> {
                        System.out.println("Acknowledgement recieved");
                        String fileName = lines[1];
                        countdownIndex(fileName);
                    }
                    case "LIST" -> {
                        //stuff to do with re-balancing
                    }
                    case "REBALANCE_COMPLETE" -> {
                        //other stuff
                    }
                    case "ERROR_FILE_DOES_NOT_EXIST" -> {
                        System.err.println("Dstore " + c.getPort() + " Sent that the file doesn't exist for a remove request");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static synchronized boolean isFileInIndex(String fileName) {
        return getIndex().containsKey(fileName);
    }

    public static void allocateDstores(Socket c, List<String> Dgo) {
        try {
            PrintWriter out = new PrintWriter(c.getOutputStream(), true);
            String DstoresToGo = "STORE_TO " + String.join(" ", Dgo);
            out.println(DstoresToGo);
        } catch (Exception e) {
            System.err.println("Error : " + e);
        }
    }

    /*
    public static void recieveReplyFromDstores(Socket c, String fileName, List<String> Dgo) {
        try {
            PrintWriter out = new PrintWriter(c.getOutputStream(), true);
            HashMap<String, Socket> Dstores = new HashMap<>(getDstoreList());
            List<String> outPutScraper = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch countDownLatch = new CountDownLatch(R);
            List<Thread> workers = new ArrayList<>();
            ArrayList<String> target = new ArrayList<>();
            for(String s : Dgo) {
                workers.add(new Thread(new Worker(s, Dstores.get(s), fileName, outPutScraper, countDownLatch)));
                target.add("Counted down");
            }
            // theoretically here is where we could stop the synchronized block
            workers.forEach(Thread::start);
            boolean completed = countDownLatch.await(timeout * 2, TimeUnit.MILLISECONDS);
            outPutScraper.add("Latch released");
            target.add("Latch released");

            if(outPutScraper.containsAll(target)) {
                updateIndexStatus(fileName, "store complete");
                out.println("STORE_COMPLETE");
                c.close();
            } else {
                removeFromIndex(fileName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

     */

    public static List<String> selectDstores() {
        List<String> Dstores;
        List<String> DstoresWithFiles;
        synchronized (Controller.class) {
            Dstores = new ArrayList<>(getDstoreList().keySet());
            DstoresWithFiles = getLocationsWithLeastFiles();
        }
        Dstores.removeAll(DstoresWithFiles);
        int n = R - Dstores.size();
        Dstores.addAll(DstoresWithFiles.subList(0, Math.min(n, DstoresWithFiles.size())));
        return Dstores;
    }

    public static List<String> getLocationsWithLeastFiles() {
        HashMap<String, Integer> storageCounts = new HashMap<>();
        synchronized (Controller.class) {
            ArrayList<FileIndex> index = getIndexFiles();
            for (FileIndex entry : index) {
                List<String> locations = entry.getDstoreAllocation();
                for (String location : locations) {
                    storageCounts.put(location, storageCounts.getOrDefault(location, 0) + entry.getFileSize());
                }
            }
        }
        List<String> sortedLocations = new ArrayList<>(storageCounts.keySet());
        sortedLocations.sort(Comparator.comparingInt(storageCounts::get));
        return sortedLocations;
    }

    /*
    static class Worker implements Runnable {
        private List<String> outputScraper;
        private CountDownLatch countDownLatch;
        private String port;
        private Socket connector;
        private String fileName;

        public Worker(String port, Socket c, String s, List<String> outputScraper, CountDownLatch countDownLatch) {
            this.port = port;
            this.connector = c;
            this.fileName = s;
            this.outputScraper = outputScraper;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(connector.getInputStream()));
                String line;
                line = in.readLine();
                if(!line.equals("STORE_ACK " + fileName)) {
                    System.err.println("Thread: Storage acceptance message incorrect");
                    System.err.println("Dstore non-functional, removing Dstore");
                    //remove Dstore method goes here
                } else {
                    outputScraper.add("Counted down");
                    countDownLatch.countDown();
                }
            } catch (Exception e) {
                System.err.println("Error: " + e);
                outputScraper.add("failed for Dstore: " + port);
                countDownLatch.countDown();
            }
        }
    }
    */

    /*
    static class DstoreReplyJob implements Callable<Boolean> {
        Socket connector;
        String fileName;

        public DstoreReplyJob(Socket c, String s) {
            this.connector = c;
            this.fileName = s;
        }

        @Override
        public Boolean call() throws Exception {
            BufferedReader in = new BufferedReader(new InputStreamReader(connector.getInputStream()));
            String line;
            line = in.readLine();
            if(!line.equals("STORE_ACK " + fileName)) {
                System.err.println("Thread: Storage acceptance message incorrect");
                return false;
            } else {
                return true;
            }
        }
    }
     */
    static class controllerThread implements Runnable {

        Socket connector;
        private int threadVariable;


        controllerThread(Socket c) {
            connector = c;
            this.threadVariable = 0;
        }

        public void run() {
            Controller.setIndexToStore(threadVariable);
            receive(connector);
        }
    }

    public static class FileIndex {
        String fileName;
        Integer fileSize;
        List<String> DstoreAllocation;
        String status;
        CountDownLatch cdLatch;

        FileIndex(String name, Integer size, List<String> allocation) {
            fileName = name;
            fileSize = size;
            DstoreAllocation = allocation;
            status = "store in progress";
            cdLatch = new CountDownLatch(allocation.size());
        }

        FileIndex(FileIndex f) {
            fileName = f.getFileName();
            fileSize = f.getFileSize();
            DstoreAllocation = f.getDstoreAllocation();
            status = f.getStatus();
            cdLatch = f.getCountDownLatch();
        }

        public CountDownLatch getCountDownLatch() {
            return this.cdLatch;
        }

        public void setCountDownLatch(int i) {
            this.cdLatch = new CountDownLatch(i);
        }

        public void setFileName(String name) {
            this.fileName = name;
        }

        public String getFileName() {
            return this.fileName;
        }

        public void setFileSize(Integer size) {
            this.fileSize = size;
        }

        public Integer getFileSize() {
            return this.fileSize;
        }

        public void setDstoreAllocation(int index, String port) {
            this.DstoreAllocation.set(index, port);
        }

        public void addDstoreToAllocation(String port) {
            this.DstoreAllocation.add(port);
        }

        public void removeDstoreFromAllocation(String port) {
            this.DstoreAllocation.remove(port);
        }

        public List<String> getDstoreAllocation() {
            return this.DstoreAllocation;
        }

        public String getOneDstoreAllocation(int index) {
            return this.DstoreAllocation.get(index);
        }

        public void setStatus(String st) throws Exception {
            switch (st) {
                case "store in progress", "store complete", "remove in progress", "remove complete" -> this.status = st;
                default -> throw new Exception("Wrong status");
            }
        }

        public String getStatus() {
            return this.status;
        }
    }
}
