package kr.ac.knu.sslab.storm.examples.topology;

import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import kr.ac.knu.sslab.storm.examples.topology.bolt.ReportBolt;
import kr.ac.knu.sslab.storm.examples.topology.bolt.SplitSentenceBolt;
import kr.ac.knu.sslab.storm.examples.topology.bolt.WordCountBolt;
import kr.ac.knu.sslab.storm.examples.topology.spout.RandomSentenceSpout;

public class WordCountTopology extends ConfigurableTopology {
    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new WordCountTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        String topologyName = "word-count";
        Number numSpout = 5;
        Number numSplit = 8;
        Number numCount = 12;
        Number numWorkers = 1;
        Integer dataSize = 250;

        if (args != null && args.length > 0) {
            topologyName = args[0];
            numWorkers = Integer.parseInt(args[1]);
            numSpout = Integer.parseInt(args[2]);
            numSplit = Integer.parseInt(args[3]);
            numCount = Integer.parseInt(args[4]);
            dataSize = Integer.parseInt(args[5]);
        }

        builder.setSpout("spout", new RandomSentenceSpout(dataSize), numSpout).setNumTasks(numSpout);
        builder.setBolt("split", new SplitSentenceBolt(), numSplit).shuffleGrouping("spout").setNumTasks(numSplit);
        builder.setBolt("count", new WordCountBolt(), numCount).fieldsGrouping("split", new Fields("word")).setNumTasks(numCount);
        builder.setBolt("report", new ReportBolt()).globalGrouping("count");

        conf.setDebug(true);
        conf.setNumWorkers(numWorkers.intValue());
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

        return submit(topologyName, conf, builder);
    }
}
