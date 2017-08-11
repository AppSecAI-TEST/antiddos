package com.vesense.antiddos;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/7/8.
 */
public class AnalysisBolt extends BaseBasicBolt {

    private int period;
    private int limit;
    private Map<String,Long> cache;

    /**
     *
     * @param period IP汇总的时间间隔
     * @param limit IP次数允许的最大阀值
     */
    public AnalysisBolt(int period, int limit) {
        this.period = period;
        this.limit = limit;
        this.cache = new HashMap<>();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {

        Config config = new Config();

        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, period);

        return config;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        if (TupleUtils.isTick(tuple)) {

            System.out.println("Cache:"+cache);

            StringBuilder sb = new StringBuilder();
            cache.forEach((k,v) -> {
                if (v > limit) {
                    sb.append("<br/>");
                    sb.append(k);
                }
            });
            cache.clear();

            if (sb.length() > 0) {
                collector.emit(new Values(sb.toString()));
            }
        } else {
            String ip = tuple.getStringByField("ip");

            Long count = cache.get(ip);

            if (count == null) {
                cache.put(ip, 1L);
            } else {
                cache.put(ip, count+1);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ips"));
    }
}
