package org.apache.spark.network.pmof;

public class ShuffleBlockInfo {
    public ShuffleBlockInfo() {}

    public ShuffleBlockInfo(String shuffleBlockId, long address, int length, long rkey) {
        this.shuffleBlockId = shuffleBlockId;
        this.address = address;
        this.length = length;
        this.rkey = rkey;
    }

    public String getShuffleBlockId() {
        return shuffleBlockId;
    }

    public void setShuffleBlockId(String shuffleBlockId) {
        this.shuffleBlockId = shuffleBlockId;
    }

    public long getAddress() {
        return address;
    }

    public void setAddress(long address) {
        this.address = address;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public long getRkey() {
        return rkey;
    }

    public void setRkey(long rkey) {
        this.rkey = rkey;
    }

    @Override
    public String toString() {
        return "ShuffleBlockInfo{" +
                "shuffleBlockId='" + shuffleBlockId + '\'' +
                ", address=" + address +
                ", length=" + length +
                ", rkey=" + rkey +
                '}';
    }

    private String shuffleBlockId;
    private long address;
    private int length;
    private long rkey;
}
