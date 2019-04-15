package lzhou.learning.storm.examples;

import lzhou.learning.storm.examples.spout.RandomNumberSpout;
import org.apache.storm.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.storm.Config;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Demonstrates LocalCluster and a simple topology.
 *
 * A localcluster can run without a real Storm cluster. But the maven dependency must be compile scoped.
 */
public class LocalClusterExample {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        /**
         * A spout is a data source.
         */
        builder.setSpout("spout", new RandomNumberSpout(), 1);

        /**
         * A bolt is used to process / store data.
         */
        builder.setBolt("print", new PrintBolt(), 10).shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(false);

        config.setNumWorkers(8);

        /**
         * LocalCluster is used for testing / developing. It has a built-in local cluster. Run it by calling the main method.
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalClusterExampleTopology", config, builder.createTopology());
        Utils.sleep(20000);
        cluster.shutdown();
    }
}

/**
 * Demonstrates a simple terminal bolt.
 */
class PrintBolt implements IRichBolt {
    private OutputCollector outputCollector = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println(tuple.toString());
        this.outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
