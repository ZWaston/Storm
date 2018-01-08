package com.waston.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 对sentence按照空格进行划分，发送给下游
 * @author zhang
 *
 */
public class SpiltWordBolt extends BaseRichBolt{
	OutputCollector collector;
	/**
	 * 每读取一个tuple就会执行一次
	 */
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		for(String word:sentence.split(" ")) {
			this.collector.emit(new Values(word));
		}
		
	}

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}

}
