package lzhou.learning.storm.examples;

import lzhou.learning.storm.examples.bolt.RedisBolt;
import lzhou.learning.storm.examples.spout.RandomNumberSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates count-based windowed operations on a stream.
 *
 * More windows bolt examples can be found here:
 *   - https://www.jianshu.com/p/8e8552299be7
 *   - https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/org/apache/storm/starter/StatefulWindowingTopology.java
 */
public class MovingAverageExample {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomNumberSpout(), 1);
        builder.setBolt("movingaverage", new MovingAverageBolt().withWindow(BaseWindowedBolt.Count.of(100),BaseWindowedBolt.Count.of(50)), 1).globalGrouping("spout");
        builder.setBolt("redis", new RedisBolt("movingaverage"), 5).shuffleGrouping("movingaverage");
        builder.setBolt("movingaverage_2", new MovingAverageBolt2().withWindow(100, 50), 1).globalGrouping("spout");
        builder.setBolt("redis_2", new RedisBolt("movingaverage_2"), 5).shuffleGrouping("movingaverage_2");

        Config config = new Config();
        config.setDebug(false);

        config.setNumWorkers(8);

        StormSubmitter.submitTopology("MovingAverageExampleTopology", config, builder.createTopology());
    }
}

/**
 * Demonstrates the use of BaseWindowedBolt.
 */
class MovingAverageBolt extends BaseWindowedBolt {
    private BigDecimal sum = BigDecimal.ZERO;
    private int length = 0;
    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(TupleWindow tupleWindow) {
        /**
         * tupleWindow.getNew() returns new tuples since the last window.
         * tupleWindow.getExpired() returns newly expired tuples since the last window.
         * tupleWindow.getNew() returns all tuples in the current window.
         */
        List<Tuple> newTuples = tupleWindow.getNew();
        List<Tuple> expiredTuples = tupleWindow.getExpired();
        List<Tuple> allTuples = tupleWindow.get();
        BigDecimal newSum = newTuples.stream().map(x-> BigDecimal.valueOf(x.getLong(0)))
                .reduce(BigDecimal.ZERO, (sum, x)->sum.add(x));
        BigDecimal expiredSum = expiredTuples.stream().map(x-> BigDecimal.valueOf(x.getLong(0)))
                .reduce(BigDecimal.ZERO, (sum, x)->sum.add(x));
        sum = sum.add(newSum).subtract(expiredSum);
        length = length+newTuples.size()-expiredTuples.size();
        /*
        sum = allTuples.stream().map(x-> BigDecimal.valueOf(x.getLong(0)))
                .reduce(BigDecimal.ZERO, (sum, x)->sum.add(x));
        length = BigDecimal.valueOf(allTuples.size())
        */
        collector.emit(new Values(sum, sum.divide(BigDecimal.valueOf(length), 4), length));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sum", "average", "length"));
    }
}

/**
 * Demonstrates a simple implementation of WindowedBolt.
 */
class MovingAverageBolt2 implements IRichBolt {
    private int windowLength = 0;
    private int countInterval = 0;
    private int count = 0;
    private LinkedList<Long> window = new LinkedList<>();
    private BigDecimal sum = BigDecimal.ZERO;
    private OutputCollector outputCollector = null;

    public MovingAverageBolt2() {
        sum = BigDecimal.ZERO;
        windowLength = 1;
        countInterval = 1;
        count = 1;
        window = new LinkedList<>();
    }

    public MovingAverageBolt2 withWindow(int windowLength, int countInterval) {
        this.windowLength = windowLength;
        this.countInterval = countInterval;
        count = countInterval;
        return this;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        count -= 1;
        if (window.size()==windowLength) {
            sum = sum.add(BigDecimal.valueOf(-window.removeLast()));
        }
        Long val = tuple.getLong(0);
        sum = sum.add(BigDecimal.valueOf(val));
        window.addFirst(val);
        BigDecimal movingAvg = sum.divide(BigDecimal.valueOf(window.size()), 4);

        if (count == 0) {
            count = countInterval;
            outputCollector.emit(new Values(sum, movingAvg, window.size()));
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sum", "average", "length"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
