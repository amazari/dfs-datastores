package com.backtype.hadoop.pail;

import java.io.Serializable;
import java.util.List;

/**
 * Shouldn't take any args
 */
public abstract class PailStructure<T> implements Serializable {
    abstract public boolean isValidTarget(String... dirs);
    abstract public T deserialize(byte[] serialized);
    abstract public byte[] serialize(T object);
    abstract public List<String> getTarget(T object);
    abstract public Class getType();
    public String getSerializationClassName() {
        return getClass().getName();
    }
}

