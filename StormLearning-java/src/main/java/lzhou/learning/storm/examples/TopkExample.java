package lzhou.learning.storm.examples;

import lzhou.learning.storm.examples.bolt.RedisBolt;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Demonstrates TopK pattern, ie. using intermediate top K level(s). Demonstrates System Tick streams.
 */
public class TopkExample {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new BinomialRandomSpout(), 3);
        builder.setBolt("count", new CountBolt(), 5).fieldsGrouping("spout", new Fields("value"));

        builder.setBolt("topk-interm", new IntermediateTopKBolt(), 5).shuffleGrouping("count");
        builder.setBolt("topk", new FullTopKBolt(), 5).globalGrouping("topk-interm");

        builder.setBolt("redis", new RedisBolt("topk"), 10).shuffleGrouping("topk");

        Config config = new Config();
        config.setDebug(false);

        config.setNumWorkers(8);

        StormSubmitter.submitTopology("TopkExample", config, builder.createTopology());
    }
}

class TupleUtils {
    /**
     * Demonstrates how to detect system tick tuple.
     *
     * @param tuple
     * @return
     */
    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID);
    }
}

class BinomialRandomSpout extends BaseRichSpout {
    private Random rand = new Random();
    private SpoutOutputCollector spoutOutputCollector = null;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(10);
        spoutOutputCollector.emit(new Values(IntStream.range(1,100).map(x->rand.nextInt(2)).sum()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value"));
    }
}

class CountBolt implements IRichBolt {
    private int emitFrequencyInSeconds = 1;
    private OutputCollector collector;
    private Map<Integer, Integer> countTable = new HashMap<Integer, Integer>();



    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            for (Integer k : countTable.keySet()) {
                collector.emit(new Values(k, countTable.get(k)));
            }
            countTable = new HashMap<>();
        } else {
            countTable.put(tuple.getInteger(0), 1+ countTable.getOrDefault(tuple.getInteger(0), 0));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value", "count"));
    }

    /**
     * Demonstrates how to subscribe to system tick streams.
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}

class IntermediateTopKBolt implements IRichBolt {
    private OutputCollector collector;
    private int emitFrequencyInSeconds = 1;
    private int k = 3;
    private PriorityQueue<Integer[]> pq = null;

    public IntermediateTopKBolt withK(int k) {
        this.k = k;
        return this;
    }
    public IntermediateTopKBolt withFrequency(int frequency) {
        this.emitFrequencyInSeconds = frequency;
        return this;
    }

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        /**
         * Non-serializable objects must be initialized on or after calling `prepare` method.
         */
        pq = new PriorityQueue<Integer[]>(k+1,(a,b)-> {
            if (a[1]>b[1]) {
                return 1;
            } else if (a[1]<b[1]) {
                return -1;
            } else {
                return 0;
            }
        });
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            while (pq.size()>0) {
                Integer[] tmp = pq.poll();
                Integer value = tmp[0];
                Integer count = tmp[1];
                collector.emit(new Values(value,count));
            }
        } else {
            pq.offer(new Integer[]{tuple.getInteger(0), tuple.getInteger(1)});
            if (pq.size()==k+1) {
                pq.poll();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}

class FullTopKBolt implements IRichBolt {
    private OutputCollector collector;
    private int emitFrequencyInSeconds = 1;
    private int k = 3;
    private PriorityQueue<Integer[]> pq = null;

    public FullTopKBolt withK(int k) {
        this.k = k;
        return this;
    }
    public FullTopKBolt withFrequency(int frequency) {
        this.emitFrequencyInSeconds = frequency;
        return this;
    }

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        pq = new PriorityQueue<Integer[]>(k+1,(a,b)-> {
            if (a[1]>b[1]) {
                return 1;
            } else if (a[1]<b[1]) {
                return -1;
            } else {
                return 0;
            }
        });
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            int rank = 0;
            while (pq.size()>0) {
                Integer[] tmp = pq.poll();
                Integer value = tmp[0];
                Integer count = tmp[1];
                rank += 1;
                collector.emit(new Values(rank,value, count));
            }
        } else {
            pq.offer(new Integer[]{tuple.getInteger(0), tuple.getInteger(1)});
            if (pq.size()==k+1) {
                pq.poll();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("rank", "value", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}