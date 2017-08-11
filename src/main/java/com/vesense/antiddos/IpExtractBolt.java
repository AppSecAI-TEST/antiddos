package com.vesense.antiddos;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/7/8.
 */
public class IpExtractBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String log = tuple.getStringByField("value");
        String ip = extractIp(log);
        if (ip != null) {
            collector.emit(new Values(ip));
        }
    }

    public String extractIp(String log) {
        String ip = null;
        if (StringUtils.isNotEmpty(log) && isLogFormatValid(log)) {
            ip = log.substring(0, log.indexOf(" "));
        }
        return ip;
    }

    // SimpleDateFormat
    static Pattern pattern = Pattern.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3} .*");
    public boolean isLogFormatValid(String log) {
        return pattern.matcher(log).find();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip"));
    }
}
