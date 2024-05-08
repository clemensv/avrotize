package com.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.example.avro.TopLevelType;

public class App {

    public static void test() throws JsonMappingException, Exception {
        String jsonRecord1 = "{\"document\":{\"field1\":\"Value1\",\"field2\":10,\"field3\":\"hey\",\"field4\":5.5,\"field5\":100,\"fieldB\":\"OptionalB\"}}";
        String jsonRecord2 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"field4\":null,\"field5\":null,\"fieldA\":\"MandatoryA\"}}";
        String jsonRecord3 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":\"MandatoryA\"}}";
        String jsonInvalid = "{\"document\":{\"invalidField\":\"Value3\"}}";

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
