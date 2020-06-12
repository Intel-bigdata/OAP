package org.apache.spark.network.pmof;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ShuffleBlockInfoSerializer extends Serializer<ShuffleBlockInfo> {

    @Override
    public ShuffleBlockInfo read(Kryo kryo, Input input, Class type) {
        ShuffleBlockInfo shuffleBlockInfo = new ShuffleBlockInfo();
        shuffleBlockInfo.setShuffleBlockId(input.readString());
        shuffleBlockInfo.setAddress(input.readLong());
        shuffleBlockInfo.setLength(input.readInt());
        shuffleBlockInfo.setRkey(input.readLong());
        return shuffleBlockInfo;
    }

    @Override
    public void write(Kryo kryo, Output output, ShuffleBlockInfo shuffleBlockInfo) {
        output.writeString(shuffleBlockInfo.getShuffleBlockId());
        output.writeLong(shuffleBlockInfo.getAddress());
        output.writeInt(shuffleBlockInfo.getLength());
        output.writeLong(shuffleBlockInfo.getRkey());
    }
}
