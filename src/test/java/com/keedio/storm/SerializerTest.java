package com.keedio.storm;

import java.io.IOException;

import org.apache.flume.interceptor.EnrichedEventBody;
import org.apache.flume.serialization.JSONStringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class SerializerTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SerializerTest.class);
	
    @Test
    public void testNonEmptySerialization() {
        String message = "hello";
        String messageAsJsonString = "{\"fecha\":\"[0,1,2,3]\\d-[0,1]\\d-\\d\\d\\d\\d\",\"cuentas\":\"\\d{4} \\d{4} \\d{2} \\d{10}\",\"correo\":\"[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})\"}";

        try {
            // build an event from bytes with no extra_data and get its JSON string
            EnrichedEventBody fromBytes = new EnrichedEventBody(message);
            String jsonFromBytes = JSONStringSerializer.toJSONString(fromBytes);

            logger.info("json fromBytes is: " + jsonFromBytes);
            logger.info("Original message is: " + messageAsJsonString);
            Assert.assertEquals(jsonFromBytes, messageAsJsonString);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
