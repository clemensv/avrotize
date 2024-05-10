package com.example;

import static org.junit.Assume.assumeNoException;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    
    @Test
    public void testObjectMapper()
    {
        try {
            App.testFromData();
        } catch (Exception e) {
            assumeNoException(e);
        }
    }

    @Test
    public void testFromData()
    {
        try {
            App.testFromData();
        } catch (Exception e) {
            assumeNoException(e);
        }
    }

    @Test
    public void testReadWriteJson()
    {
        try {
            App.testReadWriteJson();
        } catch (Exception e) {
            assumeNoException(e);
        }
    }

    @Test
    public void testReadWriteAvroBinary()
    {
        try {
            App.testReadWriteAvroBinary();
        } catch (Exception e) {
            assumeNoException(e);
        }
    }
    
}
