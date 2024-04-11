using System;
using System.Text.Json;
using Com.Example.Avro;

public class TestProgram
{
    public static int Main()
    {
        try
        {
            string jsonRecord1 = "{\"document\":{\"field1\":\"Value1\",\"field2\":10,\"field3\":\"hey\",\"field4\":5.5,\"field5\":100,\"fieldB\":\"OptionalB\"}}";
            string jsonRecord2 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"field4\":null,\"field5\":null,\"fieldA\":\"MandatoryA\"}}";
            string jsonRecord3 = "{\"document\":{\"field1\":\"Value2\",\"field2\":20,\"field3\":false,\"fieldA\":\"MandatoryA\"}}";
            string jsonInvalid = "{\"document\":{\"invalidField\":\"Value3\"}}";

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
            return 0;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return 1;
        }
    }
}
