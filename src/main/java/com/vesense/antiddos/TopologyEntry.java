package com.vesense.antiddos;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.util.Properties;

public class TopologyEntry {

    public static void main(String[] args) {

        Properties properties = new Properties();

        //载入配置文件
        try {
            properties.load(TopologyEntry.class.getResourceAsStream("/topology.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        //邮箱配置文件
        Properties emailConfig = new Properties();
        try {
            emailConfig.load(TopologyEntry.class.getResourceAsStream("/email.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        String host = properties.getProperty("kafka.bootstrap.servers");
        String topic = properties.getProperty("kafka.topic");
        String groupId = properties.getProperty("kafka.group.id");

        // 创建SpoutConfig 对象
        KafkaSpoutConfig spoutConfig = KafkaSpoutConfig.builder(host, topic)
                .setGroupId(groupId)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .setOffsetCommitPeriodMs(5000)
                .build();
        // 创建KafkaSpout对象
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout,Utils.getInt(properties.getProperty("kafkaSpout.parallelism")));
        builder.setBolt("ipExtractBolt", new IpExtractBolt(),
                Utils.getInt(properties.getProperty("ipExtractBolt.parallelism"))).shuffleGrouping("kafkaSpout");
        int period = Utils.getInt(properties.getProperty("analysisBolt.period"));
        int limit = Utils.getInt(properties.getProperty("analysisBolt.limit"));
        builder.setBolt("analysisBolt", new AnalysisBolt(period, limit),
                Utils.getInt(properties.getProperty("analysisBolt.parallelism"))).shuffleGrouping("ipExtractBolt");
        builder.setBolt("emailAlertBolt", new EmailAlertBolt(emailConfig),
                Utils.getInt(properties.getProperty("emailAlertBolt.parallelism"))).shuffleGrouping("analysisBolt");




        // topology config
        Config config = new Config();

        config.setNumWorkers(Utils.getInt(properties.getProperty("topology.workers")));

        config.setMessageTimeoutSecs(Utils.getInt(properties.getProperty("topology.message.timeout.secs")));

        config.setMaxSpoutPending(Utils.getInt(properties.getProperty("topology.max.spout.pending")));

        config.setDebug(Utils.getBoolean(properties.getProperty("topology.debug")));

        boolean isLocal = Utils.getBoolean(properties.getProperty("topology.deplogy.local"));
        String name = properties.getProperty("topology.name");
        if (isLocal) {
            // local storm cluster
            LocalCluster cluster = new LocalCluster();
            // submit topology
            cluster.submitTopology(name, config, builder.createTopology());
        } else {
            // submit topology
            try {
                StormSubmitter.submitTopology(name, config, builder.createTopology());
            } catch (AlreadyAliveException e) {
                System.out.println("already exists");
            } catch (InvalidTopologyException e) {
                System.out.println("invalid topology");
            } catch (AuthorizationException e) {
                System.out.println("auth failed");
            }
        }
    }
}
