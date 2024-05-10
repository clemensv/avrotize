package com.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import twotypeunion.com.example.avro.TopLevelType;

public class App {

    static String jsonRecord1 = "{\"document\":{\"field1\":\"Value1\",\"field2\":10,\"field3\":\"hey\",\"field4\":5.5,\"field5\":100,\"fieldB\":\"OptionalB\"}}";
    static String jsonRecord2 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"field4\":null,\"field5\":null,\"fieldA\":\"MandatoryA\"}}";
    static String jsonRecord3 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":\"MandatoryA\"}}";
    static String jsonInvalid = "{\"document\":{\"invalidField\":\"Value3\"}}";

    public static void testObjectMapper() throws JsonMappingException, Exception {
        ObjectMapper mapper = new ObjectMapper();

        TopLevelType tl1 = mapper.readValue(jsonRecord1, TopLevelType.class);
        if (tl1 == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl1.getDocument().getRecordType1() == null) {
            throw new Exception("RecordType1 is null");
        }
        TopLevelType tl2 = mapper.readValue(jsonRecord2, TopLevelType.class);
        if (tl2 == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl2.getDocument().getRecordType2() == null) {
            throw new Exception("RecordType2 is null");
        }
        TopLevelType tl3 = mapper.readValue(jsonRecord3, TopLevelType.class);
        if (tl3 == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl3.getDocument().getRecordType2() == null) {
            throw new Exception("RecordType2 is null");
        }
        try {
            mapper.readValue(jsonInvalid, TopLevelType.class);
            throw new Exception("Expected exception was not thrown");
        } catch (JsonMappingException e) {
            // expected
        }
    }

    public static void testReadWriteJson() throws JsonMappingException, Exception {
        byte[] encoded = null;
        TopLevelType tl1 = TopLevelType.fromData(jsonRecord1, "application/json");
        if (tl1 == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl1.toByteArray("application/json");
        tl1 = TopLevelType.fromData(encoded, "application/json");
        if (tl1 == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl1.getDocument().getRecordType1() == null) {
            throw new Exception("RecordType1 is null");
        }
        TopLevelType tl2 = TopLevelType.fromData(jsonRecord2, "application/json");
        if (tl2 == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl2.getDocument().getRecordType2() == null) {
            throw new Exception("RecordType2 is null");
        }
        TopLevelType tl3 = TopLevelType.fromData(jsonRecord3, "application/json");
        if (tl3 == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl3.getDocument().getRecordType2() == null) {
            throw new Exception("RecordType2 is null");
        }
        try {
            TopLevelType.fromData(jsonInvalid, "application/json");
            throw new Exception("Expected exception was not thrown");
        } catch (JsonMappingException e) {
            // expected
           }
    }


    public static void main(String[] args) {
        try {
            testObjectMapper();
            testReadWriteJson();
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
        System.exit(0);
    }
}
