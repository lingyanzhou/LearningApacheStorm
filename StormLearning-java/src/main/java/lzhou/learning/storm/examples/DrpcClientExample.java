package lzhou.learning.storm.examples;

import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class DrpcClientExample {
    public static void main(String[] args) throws TException {
        String host = "nimbus1";
        if (args.length>0) {
            host = args[0];
        }
        Map config = Utils.readDefaultConfig();

        DRPCClient client = new DRPCClient(config,host,3772);
        for (String arg : args) {
            System.out.println("Result for \"" + arg + "\": " + client.execute("HelloDrpc", arg));
        }
    }
}