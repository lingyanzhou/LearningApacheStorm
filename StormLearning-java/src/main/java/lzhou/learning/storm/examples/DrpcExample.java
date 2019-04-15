package lzhou.learning.storm.examples;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Demonstrates drpc.
 *
 * All numbus and supervisor nodes must have `drpc.servers` configured in the configuration file. And the drpc server (started by `storm drpc` command) must be running.
 */
public class DrpcExample {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("HelloDrpc");
        /**
         * Equivolant to :
         * TopologyBuilder builder = new TopologyBuilder();
         * builder.setSpout(new DRPCSpout())
         * builder.setBolt(<some processing bolt>)
         * builder.setBolt(new ReturnResults())
         */
        builder.addBolt(new HelloBolt());
        Config config = new Config();
        config.setNumWorkers(8);
        StormSubmitter.submitTopology("DrpcExample", config, builder.createRemoteTopology());
    }
}

/**
 * Demonstrates the last bolt in the LinearDRPCTopology
 */
class HelloBolt extends BaseBatchBolt {
    private BatchOutputCollector batchOutputCollector = null;
    private Object id=  null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, Object id) {
        this.id = id;
        this.batchOutputCollector = batchOutputCollector;
    }

    /**
     * The last bolt must return [id, result].
     * The id is the 0-th element of the tuple, or the id from the prepare method.
     */
    @Override
    public void execute(Tuple tuple) {
        batchOutputCollector.emit(new Values(id, "Hello "+tuple.getString(1)+"!"));
    }

    @Override
    public void finishBatch() {
        //Finished batch with id==this.id
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "value"));
    }
}



