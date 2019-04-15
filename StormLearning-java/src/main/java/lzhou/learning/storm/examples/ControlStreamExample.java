package lzhou.learning.storm.examples;

import lzhou.learning.storm.examples.bolt.RedisBolt;
import lzhou.learning.storm.examples.spout.RandomNumberWithTickSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.math.BigDecimal;
import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.Map;

/**
 * Demonstrates sending and receiving multiple streams.
 *
 */
public class ControlStreamExample {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomNumberWithTickSpout(), 1);

        /**
         * Grouping:
         * Global grouping:
         * All grouping: Replicate stream
         * Shuffle grouping: Distribute data randomly for load balancing.
         */
        /**
         * A spout can generate multiple streams, and a bolt can receive multiple streams. Use streamId to tell them apart.
         */
        builder.setBolt("stddev", new StddevBolt(), 1).globalGrouping("spout").allGrouping("spout", "TICK");
        builder.setBolt("redis", new RedisBolt("stddev"), 10).shuffleGrouping("stddev");

        Config config = new Config();
        config.setDebug(false);

        config.setNumWorkers(8);

        StormSubmitter.submitTopology("ControlStreamExampleTopology", config, builder.createTopology());
    }
}

/**
 * Demonstrate how to receive from two streams.
 */
class StddevBolt implements IRichBolt {
    private BigDecimal sum = BigDecimal.ZERO;
    private BigDecimal sumSq = BigDecimal.ZERO;
    private BigDecimal count = BigDecimal.ZERO;
    private OutputCollector outputCollector = null;

    public StddevBolt() {
        sum = BigDecimal.ZERO;
        sumSq = BigDecimal.ZERO;
        count = BigDecimal.ZERO;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        /**
         * Use streamId to distinguish different streams.
         */
        if (tuple.getSourceStreamId().equals("TICK")) {
            if (count.compareTo(BigDecimal.valueOf(0))>0) {
                this.outputCollector.emit(new Values(count, sum.divide(count, 4), sumSq.divide(count, 4).subtract(sum.divide(count, 4).pow(2))));
            }
        } else {
            count = count.add(BigDecimal.ONE);
            sum = sum.add(BigDecimal.valueOf(tuple.getLong(0)));
            sumSq = sumSq.add(BigDecimal.valueOf(tuple.getLong(0)).pow(2));

            /**
             * Ack is required, otherwise Storm will consider the tuple is lost.
             */
            this.outputCollector.ack(tuple);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("count", "average", "variance"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

