package com.vesense.antiddos;

import org.apache.commons.mail.EmailException;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Administrator on 2017/7/8.
 */
public class EmailAlertBoltTest {

    @Test
    public void testSendEmail() {
//        Properties properties = new Properties();
//        properties.setProperty("email.host", "smtp.163.com");
//        properties.setProperty("email.user", "bigdatarenliang@163.com");
//        properties.setProperty("email.password", "dashuju123");
//        properties.setProperty("email.to", "xinwang@apache.org");
//        properties.setProperty("email.subject", "Hi Xin");
//        properties.setProperty("email.content", "Hi Xin, <h2>come on!</h2>");

        Properties emailConfig = new Properties();
        try {
            emailConfig.load(TopologyEntry.class.getResourceAsStream("/email.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        EmailAlertBolt emailAlertBolt = new EmailAlertBolt(emailConfig);
        try {
            emailAlertBolt.sendEmail("91.200.12.10");
        } catch (EmailException e) {
            e.printStackTrace();
        }
    }
}
