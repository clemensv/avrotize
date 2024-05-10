package com.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import complexunion.com.example.avro.TopLevelType;

public class App {

    static String jsonRecord1 = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value1\",\"field2\":10,\"field3\":\"hey\",\"field4\":5.5,\"field5\":100,\"fieldB\":\"OptionalB\"}}";
    static String jsonRecord2 = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"field4\":null,\"field5\":null,\"fieldA\":\"MandatoryA\"}}";
    static String jsonRecord3a = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": \"value\"}}}";
    static String jsonRecord3b = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": 1}}}";
    static String jsonRecord4a = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": {\"field1\":\"Value1\",\"field2\":10}}}}";
    static String jsonRecord4b = "{\"version\":\"1.0\",\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":{\"key\": {\"field1\":10,\"field2\":\"Value1\"}}}}";
    static String jsonInvalid = "{\"version\":\"1.0\",\"document\":{\"invalidField\":\"Value3\"}}";

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

    public static void testFromData() throws JsonMappingException, Exception {
        ObjectMapper mapper = new ObjectMapper();

        TopLevelType tl1 = TopLevelType.fromData(jsonRecord1, "application/json");
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
        TopLevelType tl3a = TopLevelType.fromData(jsonRecord3a, "application/json");
        if (tl3a == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl3a.getDocument().getRecordType3a() == null) {
            throw new Exception("RecordType3a is null");
        }
        TopLevelType tl3b = TopLevelType.fromData(jsonRecord3b, "application/json");
        if (tl3b == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl3b.getDocument().getRecordType3b() == null) {
            throw new Exception("RecordType3b is null");
        }
        TopLevelType tl4a = TopLevelType.fromData(jsonRecord4a, "application/json");
        if (tl4a == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl4a.getDocument().getRecordType4a() == null) {
            throw new Exception("RecordType4a is null");
        }
        TopLevelType tl4b = TopLevelType.fromData(jsonRecord4b, "application/json");
        if (tl4b == null) {
            throw new Exception("Deserialization failed");
        }
        if (tl4b.getDocument().getRecordType4b() == null) {
            throw new Exception("RecordType4b is null");
        }
        try {
            TopLevelType.fromData(jsonInvalid, "application/json");
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
        encoded = tl2.toByteArray("application/json");
        tl2 = TopLevelType.fromData(encoded, "application/json");
        if (tl2.getDocument().getRecordType2() == null) {
            throw new Exception("RecordType2 is null");
        }
        TopLevelType tl3a = TopLevelType.fromData(jsonRecord3a, "application/json");
        if (tl3a == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl3a.toByteArray("application/json");
        tl3a = TopLevelType.fromData(encoded, "application/json");
        if (tl3a.getDocument().getRecordType3a() == null) {
            throw new Exception("RecordType3a is null");
        }
        TopLevelType tl3b = TopLevelType.fromData(jsonRecord3b, "application/json");
        if (tl3b == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl3b.toByteArray("application/json");
        tl3b = TopLevelType.fromData(encoded, "application/json");
        if (tl3b.getDocument().getRecordType3b() == null) {
            throw new Exception("RecordType3b is null");
        }
        TopLevelType tl4a = TopLevelType.fromData(jsonRecord4a, "application/json");
        if (tl4a == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl4a.toByteArray("application/json");
        tl4a = TopLevelType.fromData(encoded, "application/json");
        if (tl4a.getDocument().getRecordType4a() == null) {
            throw new Exception("RecordType4a is null");
        }
        TopLevelType tl4b = TopLevelType.fromData(jsonRecord4b, "application/json");
        if (tl4b == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl4b.toByteArray("application/json");
        tl4b = TopLevelType.fromData(encoded, "application/json");
        if (tl4b.getDocument().getRecordType4b() == null) {
            throw new Exception("RecordType4b is null");
        }
        try {
            TopLevelType.fromData(jsonInvalid, "application/json");
            throw new Exception("Expected exception was not thrown");
        } catch (JsonMappingException e) {
            // expected
        }
    }

    public static void testReadWriteAvroBinary() throws JsonMappingException, Exception {

        byte[] encoded = null;
        TopLevelType tl1 = TopLevelType.fromData(jsonRecord1, "application/json");
        if (tl1 == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl1.toByteArray("application/vnd.apache.avro+avro");
        tl1 = TopLevelType.fromData(encoded, "application/vnd.apache.avro+avro");
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
        encoded = tl2.toByteArray("application/vnd.apache.avro+avro");
        tl2 = TopLevelType.fromData(encoded, "application/vnd.apache.avro+avro");
        if (tl2.getDocument().getRecordType2() == null) {
            throw new Exception("RecordType2 is null");
        }
        TopLevelType tl3a = TopLevelType.fromData(jsonRecord3a, "application/json");
        if (tl3a == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl3a.toByteArray("application/vnd.apache.avro+avro");
        tl3a = TopLevelType.fromData(encoded, "application/vnd.apache.avro+avro");
        if (tl3a.getDocument().getRecordType3a() == null) {
            throw new Exception("RecordType3a is null");
        }
        TopLevelType tl3b = TopLevelType.fromData(jsonRecord3b, "application/json");
        if (tl3b == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl3b.toByteArray("application/vnd.apache.avro+avro");
        tl3b = TopLevelType.fromData(encoded, "application/vnd.apache.avro+avro");
        if (tl3b.getDocument().getRecordType3b() == null) {
            throw new Exception("RecordType3b is null");
        }
        TopLevelType tl4a = TopLevelType.fromData(jsonRecord4a, "application/json");
        if (tl4a == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl4a.toByteArray("application/vnd.apache.avro+avro");
        tl4a = TopLevelType.fromData(encoded, "application/vnd.apache.avro+avro");
        if (tl4a.getDocument().getRecordType4a() == null) {
            throw new Exception("RecordType4a is null");
        }
        TopLevelType tl4b = TopLevelType.fromData(jsonRecord4b, "application/json");
        if (tl4b == null) {
            throw new Exception("Deserialization failed");
        }
        encoded = tl4b.toByteArray("application/vnd.apache.avro+avro");
        tl4b = TopLevelType.fromData(encoded, "application/vnd.apache.avro+avro");
        if (tl4b.getDocument().getRecordType4b() == null) {
            throw new Exception("RecordType4b is null");
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
            testFromData();
            testReadWriteJson();
            testReadWriteAvroBinary();
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
        System.exit(0);
    }
}
