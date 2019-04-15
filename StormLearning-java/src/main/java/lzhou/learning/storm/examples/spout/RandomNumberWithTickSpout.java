package lzhou.learning.storm.examples.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.time.Instant;
import java.util.Map;
import java.util.Random;

/**
 * Demonstrate how to generate two streams; 'CALC_SUM' stream is a control stream.
 */
public class RandomNumberWithTickSpout implements IRichSpout {
    private SpoutOutputCollector spoutOutputCollector = null;
    private Random rand = new Random();
    private long prevEpoch = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }


    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
        long now = Instant.now().getEpochSecond();
        if (prevEpoch==0 || now - prevEpoch > 1) {
            prevEpoch = now;
            spoutOutputCollector.emit("TICK", new Values(now));
            return;
        }
        Utils.sleep(10);
        spoutOutputCollector.emit(new Values(rand.nextLong()));
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("text"));
        outputFieldsDeclarer.declareStream("TICK", new Fields("epoch"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}