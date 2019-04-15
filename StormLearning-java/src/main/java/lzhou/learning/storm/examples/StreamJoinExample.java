package lzhou.learning.storm.examples;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.*;
import lzhou.learning.storm.examples.bolt.RedisBolt;
import lzhou.learning.storm.examples.spout.RandomKeyValSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/**
 * Demonstrates joining two streams, with Guava cache. Storm's TimeCacheMap is deprecated.
 */
public class StreamJoinExample {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout_1", new RandomKeyValSpout().withRandomSeed(0).withValFunc(new ValFunc1()), 1);
        builder.setSpout("spout_2", new RandomKeyValSpout().withRandomSeed(0).withValFunc(new ValFunc2()), 1);
        builder.setBolt("stream_join", new StreamJoinBolt(), 10).fieldsGrouping("spout_1", new Fields("key")).fieldsGrouping("spout_2", new Fields("key"));
        builder.setBolt("redis", new RedisBolt("stream_join"), 10).shuffleGrouping("stream_join");

        Config config = new Config();
        config.setDebug(false);

        config.setNumWorkers(8);
        StormSubmitter.submitTopology("StreamJoinExample", config, builder.createTopology());
    }
}

class StreamJoinBolt extends BaseRichBolt {
    private OutputCollector outputCollector = null;
    private Cache<Integer, Tuple> timedCache1 = null;
    private Cache<Integer, Tuple> timedCache2 = null;

    /**
     * Notify storm of the failed tuples in caches removal listeners.
     */
    class StreamJoinRemovalListener implements RemovalListener<Integer, Tuple> {
        @Override
        public void onRemoval(RemovalNotification removalNotification) {
            if (removalNotification.getCause()!= RemovalCause.EXPLICIT) {
                Tuple t = (Tuple) removalNotification.getValue();
                outputCollector.fail(t);
            }
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        timedCache1 = CacheBuilder.newBuilder().maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.MINUTES).removalListener(new StreamJoinRemovalListener()).build();
        timedCache2 = CacheBuilder.newBuilder().maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.MINUTES).removalListener(new StreamJoinRemovalListener()).build();
    }


    @Override
    public void execute(Tuple tuple) {
        Integer key = tuple.getInteger(0);
        if (tuple.getSourceComponent().equals("spout_1")) {
            /**
             * If the cache already has the tuple, the old one is removed.
             */
            if (timedCache1.getIfPresent(key)!=null) {
                timedCache1.invalidate(key);
            }
            Tuple tuple2 = timedCache2.getIfPresent(key);
            if (tuple2==null) {
                timedCache1.put(key, tuple);
            } else {
                timedCache2.invalidate(key);
                Integer value1 = tuple.getInteger(1);
                Integer value2 = tuple2.getInteger(1);
                outputCollector.emit(new Values(key, value1, value2));
                outputCollector.ack(tuple);
                outputCollector.ack(tuple2);
            }
        } else if (tuple.getSourceComponent().equals("spout_2")) {
            if (timedCache2.getIfPresent(key)!=null) {
                timedCache2.invalidate(key);
            }
            Tuple tuple1 = timedCache1.getIfPresent(key);
            if (tuple1==null) {
                timedCache2.put(key, tuple);
            } else {
                timedCache1.invalidate(key);
                Integer value1 = tuple1.getInteger(1);
                Integer value2 = tuple.getInteger(1);
                outputCollector.emit(new Values(key, value1, value2));
                outputCollector.ack(tuple1);
                outputCollector.ack(tuple);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "value_1", "value_2"));
    }
}

class ValFunc1
        implements UnaryOperator<Integer>, Serializable {
    @Override
    public Integer apply(Integer v) {
        return v/2;
    }
}
class ValFunc2
        implements UnaryOperator<Integer>, Serializable{
    @Override
    public Integer apply(Integer v) {
        return v/3;
    }
}

