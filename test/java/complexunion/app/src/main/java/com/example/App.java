package com.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.example.avro.TopLevelType;

public class App {

    public static void test() throws JsonMappingException, Exception {
        String jsonRecord1 = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value1\",\"field2\":10,\"field3\":\"hey\",\"field4\":5.5,\"field5\":100,\"fieldB\":\"OptionalB\"}}";
        String jsonRecord2 = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"field4\":null,\"field5\":null,\"fieldA\":\"MandatoryA\"}}";
        String jsonRecord3a = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": \"value\"}}}";
        String jsonRecord3b = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": 1}}}";
        String jsonRecord4a = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": {\"field1\":\"Value1\",\"field2\":10}}}}";
        String jsonRecord4b = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": {\"field1\":10,\"field2\":\"Value1\"}}}}";
        String jsonInvalid = "{\"version\":\"1.0\",\"document\":{\"invalidField\":\"Value3\"}}";

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
        TopLevelType tl3a = mapper.readValue(jsonRecord3a, TopLevelType.class);
        if (tl3a == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl3a.getDocument().getRecordType3a() == null) {
            throw new Exception("RecordType3a is null");
        }
        TopLevelType tl3b = mapper.readValue(jsonRecord3b, TopLevelType.class);
        if (tl3b == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl3b.getDocument().getRecordType3b() == null) {
            throw new Exception("RecordType3b is null");
        }
        TopLevelType tl4a = mapper.readValue(jsonRecord4a, TopLevelType.class);
        if (tl4a == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl4a.getDocument().getRecordType4a() == null) {
            throw new Exception("RecordType4a is null");
        }
        TopLevelType tl4b = mapper.readValue(jsonRecord4b, TopLevelType.class);
        if (tl4b == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl4b.getDocument().getRecordType4b() == null) {
            throw new Exception("RecordType4b is null");
        }
        try {
            mapper.readValue(jsonInvalid, TopLevelType.class);
            throw new Exception("Expected exception was not thrown");
        } catch (JsonMappingException e) {
            // expected
        }
    }

    public static void main(String[] args) {
        try {
            test();
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
        System.exit(0);
    }
}
