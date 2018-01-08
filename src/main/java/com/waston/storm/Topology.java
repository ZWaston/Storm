package com.waston.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class Topology {
	public static void main(String[] args) {
		//创建一个TopologyBuilder
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("SpoutBolt", new CreateSentenceSpout(), 2);
        tb.setBolt("SplitBolt", new SpiltWordBolt(), 2).shuffleGrouping("SpoutBolt");
        tb.setBolt("CountBolt", new CountBolt(), 4).fieldsGrouping("SplitBolt", new Fields("word"));
        //创建配置
        Config conf = new Config();
        //设置worker数量
        conf.setNumWorkers(2);//每一个Worker 进程默认都会对应一个Acker 线程,用于容错
        //提交任务
        //集群提交
        //StormSubmitter.submitTopology("myWordcount", conf, tb.createTopology());
        //本地提交
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("myWordcount", conf, tb.createTopology());
	}

}
