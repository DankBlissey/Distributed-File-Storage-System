
public class FileIndex {
    String fileName;
    Integer fileSize;
    Integer[] DstoreAllocation;

    FileIndex(String name, Integer size, Integer[] allocation) {
        fileName = name;
        fileSize = size;
        DstoreAllocation = allocation;
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

    public void setDstoreAllocation(int index, Integer port) {
        this.DstoreAllocation[index] = port;
    }

    public Integer[] getDstoreAllocation() {
        return this.DstoreAllocation;
    }

    public Integer getOneDstoreAllocation(int index) {
        return this.DstoreAllocation[index];
    }
}
