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
    static HashMap<String, FileIndex> Index = new HashMap<>();
    static HashMap<String, DStoreI> DstoreList = new HashMap<>();
    static ThreadLocal<Integer> indexToStore = new ThreadLocal<>();
    static final Object rebalanceObj = new Object();
    static List<DstoreFileList> rebalanceDstoreFiles = new ArrayList<>();
    static CountDownLatch rebalanceCd;

    public static synchronized CountDownLatch getRebalanceCountDownLatch() {
        return rebalanceCd;
    }

    public static synchronized void setRebalanceCountDownLatch(int i) {
        rebalanceCd = new CountDownLatch(i);
    }

    public static synchronized void countDownRebalance() {
        getRebalanceCountDownLatch().countDown();
    }

    public static synchronized void addRebalanceDstoreFiles(DstoreFileList s) {
        rebalanceDstoreFiles.add(s);
    }

    public static synchronized void wipeRebalanceDstoreFiles() {
        rebalanceDstoreFiles = new ArrayList<>();
    }

    public static synchronized List<DstoreFileList> getRebalanceDstoreFiles() {
        return rebalanceDstoreFiles;
    }

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

    public static synchronized void addToDstoreList(String s, DStoreI store)  {
        DstoreList.put(s,store);
    }

    public static synchronized Socket getDstore(String s) {
        return DstoreList.get(s).getSocket();
    }

    public static synchronized DStoreI getDstoreI(String s) {
        return DstoreList.get(s);
    }

    public static synchronized HashMap<String, DStoreI> getDstoreList() {
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
            new Thread(new RebalanceThread()).start();

            while(true) {
                try {
                    System.out.println("New connection being accepted");
                    Socket c = cport.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    PrintWriter out = new PrintWriter(c.getOutputStream());
                    new Thread(new controllerThread(c, in, out)).start();
                    System.out.println("Client connected:");
                } catch (Exception e) {
                    System.err.println("Error: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }

    }

    public static void receive(Socket c, BufferedReader in, PrintWriter out) {
        try {
            System.out.println("Reading line");
            String line;
            while((line = in.readLine()) != null) {
                System.out.println("Line read");
                String[] lines = line.split(" ");
                switch (lines[0]) {
                    case "JOIN" -> {
                        System.out.println("Dstore connected with port " + lines[1]);
                        //if(c.isClosed()) {
                        //    System.out.println("socket closed before adding to dstorelist");
                        //}
                        DStoreI store = new DStoreI(c, in, out);
                        addToDstoreList(lines[1], store);
                        //if(c.isClosed()) {
                        //    System.out.println("socket closed after adding to dstorelist");
                        //}
                        synchronized (rebalanceObj) {
                            rebalanceObj.notify();
                        }
                        recieveDstoreMsg(store);
                    }

                    case "STORE" -> {
                        System.out.println("Client store request");
                        synchronized (Controller.class) {
                            if (getDstoreList().size() >= R) {
                                if ((!isFileInIndex(lines[1])) || ((isFileInIndex(lines[1])) && (getIndex().get(lines[1]).getStatus().equals("remove complete")))) {
                                    System.out.println("selecting Dstores");
                                    List<String> Dgo = selectDstores();
                                    String fileName = lines[1];
                                    Integer fileSize = Integer.parseInt(lines[2]);
                                    System.out.println("Adding file " + fileName + " to index");
                                    addToIndex(new FileIndex(fileName, fileSize, Dgo));
                                    System.out.println("allocating Dstores for " + fileName);
                                    allocateDstores(c, Dgo, in, out);
                                } else {
                                    System.err.println("Error with storage request: file already exists");
                                    out.println("ERROR_FILE_ALREADY_EXISTS");
                                    out.flush();
                                    break;
                                    //c.close();
                                }
                            } else {
                                System.err.println("Error with storage request: not enough Dstores");
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                out.flush();
                                break;
                                //c.close();
                            }
                        }
                        String fileName = lines[1];
                        try {
                            Boolean acknow = getLatch(fileName).await(timeout, TimeUnit.MILLISECONDS);
                            if(acknow.equals(true)) {
                                updateIndexStatus(fileName, "store complete");
                                out.println("STORE_COMPLETE");
                                out.flush();
                                setLatch(fileName, R);
                                //c.close();
                            } else {
                                removeFromIndex(fileName);
                                //c.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            removeFromIndex(fileName);
                            //c.close();
                        }
                    }
                    case "LOAD" -> {
                        setIndexToStore(0);
                        String fileName = lines[1];
                        FileIndex file;
                        if((file = getIndexFile(fileName)) != null) {
                            List<String> dstores = file.getDstoreAllocation();
                            if(!dstores.isEmpty() || file.getStatus().equals("store complete")) {
                                if(getDstoreList().size() >= R) {
                                    out.println("LOAD_FROM " + dstores.get(0) + " " + file.getFileSize().toString());
                                    out.flush();
                                    receive(c,in,out);
                                } else {
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    out.flush();
                                    //c.close();
                                }
                            } else {
                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                out.flush();
                                //c.close();
                            }
                        } else {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                            out.flush();
                        }
                    }
                    case "RELOAD" -> {
                        setIndexToStore(getIndexToStore()+1);
                        String fileName = lines[1];
                        FileIndex file = new FileIndex(getIndexFile(fileName));
                        List<String> dstores = file.getDstoreAllocation();
                        if(dstores.size() > getIndexToStore()) {
                            if(!dstores.isEmpty() || file.getStatus().equals("store complete")) {
                                if(getDstoreList().size() >= R) {
                                    out.println("LOAD_FROM " + dstores.get(getIndexToStore()) + " " + file.getFileSize().toString());
                                    out.flush();
                                    receive(c,in,out);
                                } else {
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    out.flush();
                                    //c.close();
                                }
                            } else {
                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                out.flush();
                                //c.close();
                            }
                        } else {
                            out.println("ERROR_LOAD");
                            out.flush();
                            //c.close();
                        }

                    }
                    case "REMOVE" -> {
                        System.out.println("client remove request: ");
                        String fileName = lines[1];
                        synchronized (Controller.class) {
                            if (getDstoreList().size() >= R) {
                                if (isFileInIndex(lines[1])) {
                                    if (getIndexFile(fileName).getStatus().equals("store complete")) {
                                        updateIndexStatus(fileName, "remove in progress");
                                        List<String> DstoresWFile = getIndexFile(fileName).getDstoreAllocation();
                                        for(String s : DstoresWFile) {
                                            PrintWriter out1 = getDstoreI(s).getOut();
                                            out1.println("REMOVE " + fileName);
                                            out1.flush();
                                        }
                                    } else {
                                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                                        System.err.println("File requested to be removed was not a fully stored file");
                                        break;
                                    }
                                } else {
                                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                                    out.flush();
                                    break;
                                    //c.close();
                                }
                            } else {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                out.flush();
                                break;
                                //c.close();
                            }
                        }
                        try {
                            Boolean acknow = getLatch(fileName).await(timeout, TimeUnit.MILLISECONDS);
                            if(acknow.equals(true)) {
                                updateIndexStatus(fileName, "remove complete");
                                out.println("REMOVE_COMPLETE");
                                out.flush();
                                setLatch(fileName, R);
                                //c.close();
                            } else {
                                System.err.println("timeout with receiving acknowledgement of removal");
                                //c.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            //c.close();
                        }
                    }
                    case "LIST" -> {
                        if(getDstoreList().size() >= R) {
                            ArrayList<FileIndex> index = new ArrayList<>(getIndexFiles());
                            if(!index.isEmpty()) {
                                out.print("LIST");
                                for (FileIndex f : index) {
                                    if(f.getStatus().equals("store complete")) {
                                        out.print(" " + f.getFileName());
                                    }
                                }
                                out.println("");
                                out.flush();
                            } else {
                                out.println("LIST ");
                                out.flush();
                            }
                        } else {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            out.flush();
                            //c.close();
                        }
                    }
                    default -> System.err.println("Malformed client/Dstore message received, message was: " + lines[0]);
                }
            }
            System.err.println("Receive is null, attempting to close socket for safety");
            c.close();
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }

    public static synchronized void rebalance() {
        setRebalanceCountDownLatch(getDstoreList().size());
        for(DStoreI d : getDstoreList().values()) {
            d.getOut().println("LIST");
            d.getOut().flush();
        }
        try {
            Boolean listed = getRebalanceCountDownLatch().await(timeout, TimeUnit.MILLISECONDS);
            if(listed = true) {

            } else {
                System.err.println("");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        /*
        sort Dstores based on how many files they have in them, then take Dstores from the ends of the list and have them transfer a file from the biggest to the smallest,
        each transfer adds the file to the big Dstore's list to remove and adds the file to the small Dstore's list to add,this repeats until the most full and the least
        full dstore have 0-1 number of files between them.
         */
    }

    public static void recieveDstoreMsg(DStoreI store) {
        try {
            String input;
            BufferedReader in = store.getIn();
            PrintWriter out = store.getOut();
            Socket c = store.getSocket();
            while((input = in.readLine()) != null) {
                String[] lines = input.split(" ");
                switch(lines[0]) {
                    case "STORE_ACK", "REMOVE_ACK" -> {
                        System.out.println("Acknowledgement received");
                        String fileName = lines[1];
                        countdownIndex(fileName);
                    }
                    case "LIST" -> {
                        System.out.println("Dstore List reply received");
                        String portName = Integer.toString(c.getPort());
                        if(lines.length == 1) {
                            addRebalanceDstoreFiles(new DstoreFileList(portName, new ArrayList<>()));
                        } else {
                            List<String> files = new ArrayList<>(Arrays.asList(lines).subList(1,lines.length));
                            addRebalanceDstoreFiles(new DstoreFileList(portName,files));
                        }
                        countDownRebalance();
                        System.out.println("Dstore list for " + portName + " added to the rebalance file list");
                    }
                    case "REBALANCE_COMPLETE" -> {
                        //other stuff
                    }
                    case "ERROR_FILE_DOES_NOT_EXIST" -> {
                        String fileName = lines[1];
                        System.err.println("Dstore " + c.getPort() + " Sent that the file doesn't exist for a remove request for file: " + fileName);
                    }
                    default -> System.err.println("Malformed Dstore message received, message was: " + lines[0]);
                }
            }
            System.err.println("Dstore at port " + c.getPort() + "is returning null");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static synchronized boolean isFileInIndex(String fileName) {
        return getIndex().containsKey(fileName);
    }

    public static void allocateDstores(Socket c, List<String> Dgo, BufferedReader in, PrintWriter out) {
        try {
            String DstoresToGo = "STORE_TO " + String.join(" ", Dgo);
            out.println(DstoresToGo);
            out.flush();
        } catch (Exception e) {
            System.err.println("Error : " + e);
        }
    }

    public static synchronized List<String> selectDstores() {
        List<String> Dstores;
        List<String> DstoresWithFiles;
        System.out.println("getting list of All dstores");
        Dstores = new ArrayList<>(getDstoreList().keySet());
        System.out.println("getting dstore locations in order of least files");
        DstoresWithFiles = getLocationsWithLeastFiles();
        Dstores.removeAll(DstoresWithFiles);
        Dstores.addAll(DstoresWithFiles);
        return Dstores.subList(0, Math.min(R,Dstores.size()));
        //DstoresWithFiles.addAll(0, Dstores);
        //return DstoresWithFiles.subList(0, Math.min(R,DstoresWithFiles.size()));
        //original fix for selecting Dstores, not as efficient as inserting all is O(n+m)
    }

    public static synchronized List<String> getDstoresInOrder() {
        List<String> Dstores;
        List<String> DstoresWithFiles;
        System.out.println("getting list of All dstores");
        Dstores = new ArrayList<>(getDstoreList().keySet());
        System.out.println("getting dstore locations in order of least files");
        DstoresWithFiles = getLocationsWithLeastFiles();
        Dstores.removeAll(DstoresWithFiles);
        Dstores.addAll(DstoresWithFiles);
        return Dstores;
    }

    public static synchronized List<DstoreFileList> sortRebalanceList() {
        List<DstoreFileList> start = getRebalanceDstoreFiles();
        return null;
    }

    public static synchronized List<String> getLocationsWithLeastFiles() {
        HashMap<String, Integer> storageCounts = new HashMap<>();
        ArrayList<FileIndex> index = getIndexFiles();
        for (FileIndex entry : index) {
            List<String> locations = entry.getDstoreAllocation();
            for (String location : locations) {
                storageCounts.put(location, storageCounts.getOrDefault(location, 0) + entry.getFileSize());
                //might change entry.getFileSize() to 1 depending on if Dstores are to be allocated by amount of data or number of files stored.
            }
        }
        List<String> sortedLocations = new ArrayList<>(storageCounts.keySet());
        sortedLocations.sort(Comparator.comparingInt(storageCounts::get));
        return sortedLocations;
    }
    static class controllerThread implements Runnable {

        Socket connector;
        BufferedReader in;
        PrintWriter out;


        controllerThread(Socket c, BufferedReader in, PrintWriter out) {
            this.connector = c;
            this.in = in;
            this.out = out;
        }

        public void run() {
                setIndexToStore(0);
                receive(connector, in, out);
        }
    }

    static class RebalanceThread implements  Runnable {

        RebalanceThread() {

        }

        public void run() {
            while(true) {
                synchronized (rebalanceObj) {
                    try {
                        rebalanceObj.wait(rebalancePeriod.longValue());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                rebalance();
            }
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

        public void setAllDstoreAllocation(List<String> allocation) {
            this.DstoreAllocation = allocation;
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

    public static class DStoreI {
        Socket c;
        BufferedReader in;
        PrintWriter out;


        DStoreI(Socket c, BufferedReader in, PrintWriter out) {
            this.c = c;
            this.in = in;
            this.out = out;
        }

        public synchronized Socket getSocket() {
            return this.c;
        }

        public synchronized void setSocket(Socket c) {
            this.c = c;
        }

        public synchronized BufferedReader getIn() {
            return this.in;
        }

        public synchronized void setIn(BufferedReader in) {
            this.in = in;
        }

        public synchronized PrintWriter getOut() {
            return this.out;
        }

        public synchronized void setOut(PrintWriter out) {
            this.out = out;
        }
    }

    public static class DstoreFileList {
        String port;
        List<String> files;

        DstoreFileList(String port, List<String> files) {
            this.port = port;
            this.files = files;
        }
    }
}
