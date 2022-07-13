package kr.ac.knu.sslab.storm.examples.topology;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import kr.ac.knu.sslab.storm.examples.topology.bolt.ReportBolt2;
import kr.ac.knu.sslab.storm.examples.topology.bolt.SplitSentenceBolt2;
import kr.ac.knu.sslab.storm.examples.topology.bolt.WordCountBolt2;
import kr.ac.knu.sslab.storm.examples.topology.spout.RandomSentenceSpout2;

public class WordCountTopology2 extends ConfigurableTopology {
    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new WordCountTopology2(), args);
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
        // ms sleep
        Integer dataRate = 1000;

        if (args != null && args.length > 0) {
            topologyName = args[0];
            numWorkers = Integer.parseInt(args[1]);
            numSpout = Integer.parseInt(args[2]);
            numSplit = Integer.parseInt(args[3]);
            numCount = Integer.parseInt(args[4]);
            dataSize = Integer.parseInt(args[5]);
            dataRate = Integer.parseInt(args[6]);
        }

        Date currentTime = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        String outputDirectory = "/data/storm/wordcount2/" + topologyName + "_" + format.format(currentTime).toString();
        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        builder.setSpout("spout", new RandomSentenceSpout2(outputDirectory, dataSize, dataRate), numSpout).setNumTasks(numSpout);
        builder.setBolt("split", new SplitSentenceBolt2(outputDirectory), numSplit).shuffleGrouping("spout").setNumTasks(numSplit);
        builder.setBolt("count", new WordCountBolt2(outputDirectory), numCount).fieldsGrouping("split", new Fields("word")).setNumTasks(numCount);
        builder.setBolt("report", new ReportBolt2(outputDirectory)).globalGrouping("count");

        conf.setDebug(true);
        conf.setNumWorkers(numWorkers.intValue());
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

        return submit(topologyName, conf, builder);
    }
}
