package com.vesense.antiddos;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Administrator on 2017/7/8.
 */
public class IpExtractBoltTest {

    @Test
    public void testValidLogFormat() {
        IpExtractBolt ipExtractBolt = new IpExtractBolt();
        String log = "192.168.1.1 xxx";
        boolean isValid = ipExtractBolt.isLogFormatValid(log);
        Assert.assertTrue(isValid);

        log = "192.16 xxx";
        isValid = ipExtractBolt.isLogFormatValid(log);
        Assert.assertFalse(isValid);
    }

    @Test
    public void testExtractIp() {
        IpExtractBolt ipExtractBolt = new IpExtractBolt();

        // log is empty
        String log = "";
        String ip = ipExtractBolt.extractIp(log);
        Assert.assertNull(ip);

        // log is invalid
        log = "a b c";
        ip = ipExtractBolt.extractIp(log);
        Assert.assertNull(ip);

        // log is ok
        log = "192.168.1.1 xxx";
        ip = ipExtractBolt.extractIp(log);
        Assert.assertEquals("192.168.1.1", ip);
    }
}
