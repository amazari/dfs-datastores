package com.backtype.hadoop.pail;

public abstract class BinaryPailStructure extends PailStructure<byte[]> {
    public byte[] deserialize(byte[] serialized) {
        return serialized;
    }

    public byte[] serialize(byte[] object) {
        return object;
    }

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public Class getType() {
        return EMPTY_BYTE_ARRAY.getClass();
    }
}
