using System;
using System.Text;
using System.Text.Json;
using TwoTypeUnion.Com.Example.Avro;

public class TestProgram
{
    const string jsonRecord1 = "{\"document\":{\"field1\":\"Value1\",\"field2\":10,\"field3\":\"hey\",\"field4\":5.5,\"field5\":100,\"fieldB\":\"OptionalB\"}}";
    const  string jsonRecord2 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"field4\":null,\"field5\":null,\"fieldA\":\"MandatoryA\"}}";
    const  string jsonRecord3 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":\"MandatoryA\"}}";
    const  string jsonInvalid = "{\"document\":{\"invalidField\":\"Value3\"}}";

    public static void testJsonDeserialize()
    {
        var tl1 = JsonSerializer.Deserialize<TopLevelType>(jsonRecord1);
        if (tl1 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl1.Document.RecordType1 == null)
        {
            throw new Exception("RecordType1 is null");
        }
        var tl2 = JsonSerializer.Deserialize<TopLevelType>(jsonRecord2);
        if (tl2 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl2.Document.RecordType2 == null)
        {
            throw new Exception("RecordType2 is null");
        }
        var tl3 = JsonSerializer.Deserialize<TopLevelType>(jsonRecord2);
        if (tl3 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl3.Document.RecordType2 == null)
        {
            throw new Exception("RecordType2 is null");
        }
        try
        {
            JsonSerializer.Deserialize<TopLevelType>(jsonInvalid);
            throw new Exception("Expected exception was not thrown");
        }
        catch (NotSupportedException)
        {
            // expected
        }
    }

    public static void testJsonFromData()
    {
        var tl1 = TopLevelType.FromData(jsonRecord1, "application/json");
        if (tl1 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl1.Document.RecordType1 == null)
        {
            throw new Exception("RecordType1 is null");
        }
        var tl2 = TopLevelType.FromData(jsonRecord2, "application/json");
        if (tl2 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl2.Document.RecordType2 == null)
        {
            throw new Exception("RecordType2 is null");
        }
        var tl3 = TopLevelType.FromData(jsonRecord2, "application/json");
        if (tl3 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl3.Document.RecordType2 == null)
        {
            throw new Exception("RecordType2 is null");
        }
        try
        {
            TopLevelType.FromData(jsonInvalid, "application/json");
            throw new Exception("Expected exception was not thrown");
        }
        catch (NotSupportedException)
        {
            // expected
        }
    }

    public static void testReadWrite(string mediaType)
    {
        byte[] bytes = null;
        var tl1 = JsonSerializer.Deserialize<TopLevelType>(jsonRecord1);
        if (tl1 == null)
        {
            throw new Exception("Deserialization failed");
        }
        bytes = tl1.ToByteArray(mediaType);
        tl1 = TopLevelType.FromData(bytes, mediaType);
        if (tl1 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl1.Document.RecordType1 == null)
        {
            throw new Exception("RecordType1 is null");
        }
        var tl2 = JsonSerializer.Deserialize<TopLevelType>(jsonRecord2);
        if (tl2 == null)
        {
            throw new Exception("Deserialization failed");
        }
        bytes = tl2.ToByteArray(mediaType);
        tl2 = TopLevelType.FromData(bytes, mediaType);
        if (tl2 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl2.Document.RecordType2 == null)
        {
            throw new Exception("RecordType2 is null");
        }
        var tl3 = JsonSerializer.Deserialize<TopLevelType>(jsonRecord2);
        if (tl3 == null)
        {
            throw new Exception("Deserialization failed");
        }
        bytes = tl3.ToByteArray(mediaType);
        tl3 = TopLevelType.FromData(bytes, mediaType);
        if (tl3 == null)
        {
            throw new Exception("Deserialization failed");
        }
        if (tl3.Document.RecordType2 == null)
        {
            throw new Exception("RecordType2 is null");
        }
        try
        {
            bytes = Encoding.UTF8.GetBytes(jsonInvalid);
            TopLevelType.FromData(bytes, mediaType);
            throw new Exception("Expected exception was not thrown");
        }
        catch (Exception)
        {
            // expected
        }
    }


    public static int Main()
    {
        try
        {
            testJsonDeserialize();
            testJsonFromData();
            testReadWrite("application/json");
            testReadWrite("application/vnd.apache.avro+avro");
            testReadWrite("application/vnd.apache.avro+json");
            testReadWrite("application/json+gzip");
            testReadWrite("application/vnd.apache.avro+avro+gzip");
            testReadWrite("application/vnd.apache.avro+json+gzip");
            return 0;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return 1;
        }
    }
}
