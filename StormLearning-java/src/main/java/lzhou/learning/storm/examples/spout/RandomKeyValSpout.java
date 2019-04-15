package lzhou.learning.storm.examples.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.function.UnaryOperator;


public class RandomKeyValSpout extends BaseRichSpout {
    private UnaryOperator<Integer> valFunc = null;
    private Random rand = new Random(0);
    private SpoutOutputCollector spoutOutputCollector = null;

    public RandomKeyValSpout withValFunc(UnaryOperator<Integer> valFunc) {
        this.valFunc = valFunc;
        return this;
    }
    public RandomKeyValSpout withRandomSeed(long seed) {
        this.rand.setSeed(seed);
        return this;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(10);
        Integer key = rand.nextInt();
        Integer val = key;
        if (valFunc!=null) {
            val = valFunc.apply(key);
        }
        spoutOutputCollector.emit(new Values(key, val));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "value"));
    }
}
