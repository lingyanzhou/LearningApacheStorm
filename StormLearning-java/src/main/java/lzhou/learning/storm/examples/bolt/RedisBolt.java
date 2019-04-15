package lzhou.learning.storm.examples.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisBolt implements IRichBolt {
    private OutputCollector outputCollector = null;
    private Jedis jedis = null;
    private String listName = "storm_out";

    public RedisBolt(String listName) {
        this.listName = listName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        jedis = new Jedis("redis");
    }

    @Override
    public void execute(Tuple tuple) {
        jedis.lpush(listName, tuple.toString());
        jedis.ltrim(listName, 0, 1000);
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {
        jedis.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
