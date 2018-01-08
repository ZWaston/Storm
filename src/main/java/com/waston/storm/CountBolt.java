package com.waston.storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class CountBolt extends BaseRichBolt{
	
	OutputCollector collector;
    Map<String, Integer> map = new HashMap<String, Integer>();

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		if(map.containsKey(word)){
            Integer c = map.get(word);
            map.put(word, c+1);
        }else{
            map.put(word, 1);
        }
        //测试输出
        System.out.println(word+":"+map.get(word));
	}

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		
	}

}
