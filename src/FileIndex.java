import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class FileIndex {
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
        switch(st) {
            case "store in progress":
            case "store complete":
            case "remove in progress":
            case "remove complete":
                this.status = st;
                break;
            default:
                throw new Exception("Wrong status");
        }
    }

    public String getStatus() {
        return this.status;
    }
}
