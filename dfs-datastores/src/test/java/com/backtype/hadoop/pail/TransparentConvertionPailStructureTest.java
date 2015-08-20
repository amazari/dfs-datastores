package com.backtype.hadoop.pail;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.List;

public class TransparentConvertionPailStructureTest extends TestCase {

    public void testIsValidTargetIdentityLaw() throws Exception {
        checkIdentityLaw("isValid", new Function<PailStructure<String>, Object>() {
            @Override
            public Object apply(PailStructure<String> pailStructure) {
                return pailStructure.isValidTarget("/path/to/smth");
            }
        });
    }

    public void testDeserializeIdentityLaw() throws Exception {
        checkIdentityLaw("deserialize", new Function<PailStructure<String>, String>() {
            @Override
            public String apply(PailStructure<String> pailStructure) {
                return pailStructure.deserialize("bytesBytesBYTES".getBytes());
            }
        });
    }

    public void testSerializeIdentityLaw() throws Exception {
        checkIdentityLaw("serialize", new Function<PailStructure<String>, String>() {
            @Override
            public String apply(PailStructure<String> pailStructure) {
                return new String(pailStructure.serialize("bytesBytesBYTES"));
            }
        });
    }

    public void testGetTargetIdentityLaw() throws Exception {
        checkIdentityLaw("getTarget", new Function<PailStructure<String>, List<String>>() {
           @Override
            public List<String> apply(PailStructure<String> pailStructure) {
                return pailStructure.getTarget("Objectify me");
            }
        });
    }

    public void testGetTypeIdentityLaw() throws Exception {
        checkIdentityLaw("getType", new Function<PailStructure<String>, Class>() {
            @Override
            public Class apply(PailStructure<String> pailStructure) {
                return pailStructure.getType();
            }
        });
    }

    public void testGetSerializationClassNameIdentityLaw() throws Exception {
        checkIdentityLaw("getSerializationClassName", new Function<PailStructure<String>, String>() {
            @Override
            public String apply(PailStructure<String> pailStructure) {
                return pailStructure.getSerializationClassName();
            }
        });
    }

    public void testDeserializeComposition() {
        Function<String, String> uppercase = new Function<String, String>() {
            @Override
            public String apply(String o) {
                return o.toUpperCase();
            }
        };
        PailStructure<String> structure = new PailOpsTest.StringStructure();
        PailStructure<String> conversionStructure = new TransparentConvertionPailStructure<String, String>(structure,
                uppercase,
                Functions.<String>identity()
        );


        String aTestString = "un dos tres";

        assertTrue("serialize . preSerilize x == serialize(preSerialize(x)",
                Arrays.equals(structure.serialize(uppercase.apply(aTestString)),
                        conversionStructure.serialize(aTestString)));
    }

    public void testSerializeComposition() {
        Function<String, String> uppercase = new Function<String, String>() {
            @Override
            public String apply(String o) {
                return o.toUpperCase();
            }
        };
        PailStructure<String> structure = new PailOpsTest.StringStructure();
        PailStructure<String> conversionStructure = new TransparentConvertionPailStructure<String, String>(structure,
                Functions.<String>identity(),
                uppercase
        );


        byte[] aTestString = "un dos tres".getBytes();

        assertEquals("serialize . preSerilize x == serialize(preSerialize(x)",
                uppercase.apply(structure.deserialize(aTestString)),
                conversionStructure.deserialize(aTestString));
    }


    static public <U> void checkIdentityLaw(String name, Function<PailStructure<String>, U> f) {
        PailStructure<String> structure = new PailOpsTest.StringStructure();
        PailStructure<String> conversionStructure = new TransparentConvertionPailStructure<String, String>(structure,
                Functions.<String>identity(),
                Functions.<String>identity()
        );
        assertTrue (name + " should satisfy identity law",
                f.apply(structure).equals(f.apply(conversionStructure)));
    }
}