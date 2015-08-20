package com.backtype.hadoop.pail;

import com.google.common.base.Function;

import java.lang.reflect.ParameterizedType;
import java.util.List;

public class TransparentConvertionPailStructure<T, U> extends PailStructure<U> {

    private final PailStructure<T> wrappedStructure;
    private final Function<U, T> preSerialisation;
    private final Function<T, U> postDeserialization;
    public Class<U> typeOfRecord;

    /**
     *
     * @param wrappedStructure underlying structure used to de/serialize and targeting.
     * @param preSerialisation U -> T conversion applied to the result of
     *        {@code wrappedStructure.serialize}
     * @param postDeserialization T -> U conversion applied to the result of
     *        {@code wrappedStructure.deserialize}
     * @return a PailStructure that behaves like {@code wrappedStructure} with the
     * added behaviour of the two functions parameters. From outside (in the pail metadata file),
     * the resulting {@code PailStructure} will mimic the {@code wrappedStructure}
     */
    public TransparentConvertionPailStructure(PailStructure<T> wrappedStructure,
                                              Function<U, T> preSerialisation,
                                              Function<T, U> postDeserialization) {
        this.wrappedStructure = wrappedStructure;
        this.preSerialisation = preSerialisation;
        this.postDeserialization = postDeserialization;
    }

    @Override
    public boolean isValidTarget(String... dirs) {
        return wrappedStructure.isValidTarget(dirs);
    }

    @Override
    public U deserialize(byte[] serialized) {
        return postDeserialization.apply(wrappedStructure.deserialize(serialized));
    }

    @Override
    public byte[] serialize(U object) {
        T t = preSerialisation.apply(object);

        return wrappedStructure.serialize(t);
    }

    @Override
    public List<String> getTarget(U object) {
        T t = preSerialisation.apply(object);
        return wrappedStructure.getTarget(t);
    }

    @Override
    public Class getType() {
        return wrappedStructure.getType();
    }

    @Override
    public String getSerializationClassName() {
        return wrappedStructure.getSerializationClassName();
    }
}
