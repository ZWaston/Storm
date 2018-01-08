package com.waston.storm;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.daemon.acker;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import clojure.lang.Util;

/**
 * @author zhang
 *
 */
public class CreateSentenceSpout extends BaseRichSpout{
	
	SpoutOutputCollector collector;
	Random random;
	String[] sentences = null;
	private ConcurrentHashMap<UUID,Values> waitAcker; //用来记录tuple的msgID，和tuple

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//声明发送数据的域名
		outputFieldsDeclarer.declare(new Fields("sentence"));
	}
	
	/**
	 * 初始化，在调用nextTuple()之前被调用
	 */
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		random = new Random();
		sentences = new String[] {"this is a Strom test","apple orange","pig dog cat"};
		
	}
    
	/**
	 * 会重复调用此方法
	 */
	public void nextTuple() {
		Utils.sleep(10*1000);//10s发送一次数据
		String sentence = sentences[random.nextInt(sentences.length)];
		System.out.println("线程名："+Thread.currentThread().getName()+"  "+
		            new DateTime().toString("yyyy-MM-dd HH:mm:ss  ")+"10s发射一次数据："+sentence);
		
		UUID uuid=UUID.randomUUID();
		Values value = new Values(sentence);
		waitAcker.put(uuid, value);
		//向下游发送数据
		this.collector.emit(value,uuid);//uuid是msgid，指定msgid可以用于重发
	}


	/**
	 * 每当一个tuple成功处理后，调用此函数，一般用于作清除工作
	 */
	@Override
	public void ack(Object msgId) {
		System.out.println("消息处理成功:" + msgId);
        System.out.println("删除缓存中的数据...");
        waitAcker.remove(msgId);//tuple接收成功就删除数据
	}

	/**
	 * 当tuple处理失败后，则会调用此函数，一般用于重发tuple，保证数据容错
	 */
	@Override
	public void fail(Object msgId) {
		System.out.println("消息处理失败:" + msgId);
        System.out.println("重新发送失败的信息...");
        //重发如果不开启ackfail机制，那么spout的map对象中的该数据不会被删除的。
        collector.emit(new Values(waitAcker.get(msgId)),msgId);
	}

	

}
