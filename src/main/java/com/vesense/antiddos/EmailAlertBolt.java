package com.vesense.antiddos;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Administrator on 2017/7/8.
 */
public class EmailAlertBolt extends BaseBasicBolt {
    private Properties properties;

    private static Logger LOG = LoggerFactory.getLogger(EmailAlertBolt.class);

    public EmailAlertBolt(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String ips = tuple.getStringByField("ips");
        LOG.debug(ips);

        if (StringUtils.isEmpty(ips)) {
            return;
        }

        System.out.println("send email: ------------");
        System.out.println(properties.getProperty("email.content").replace("{ip}",ips));
        System.out.println("------------------------");
        try {
            sendEmail(ips);
        } catch (EmailException e) {
            collector.reportError(e);
            e.printStackTrace();
        }
    }

    public void sendEmail(String ip) throws EmailException {


        HtmlEmail email = new HtmlEmail();

        email.setHostName(properties.getProperty("email.host"));

        email.setAuthentication(properties.getProperty("email.user"), properties.getProperty("email.password"));


        email.setFrom(properties.getProperty("email.user"));
        email.addTo(properties.getProperty("email.to").split(","));

        email.setSubject(properties.getProperty("email.subject"));
        email.setHtmlMsg(properties.getProperty("email.content").replace("{ip}",ip));

        email.send();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //noop
    }
}
