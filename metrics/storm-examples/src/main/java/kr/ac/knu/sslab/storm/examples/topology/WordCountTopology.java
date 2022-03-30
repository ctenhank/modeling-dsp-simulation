package kr.ac.knu.sslab.storm.examples.topology;

import java.io.File;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.storm.metric.LoggingMetricsConsumer;

//import java.util.Map;

//import org.apache.storm.task.ShellBolt;

// Reference: https://github.com/apache/storm/blob/v2.3.0/examples/storm-starter/src/jvm/org/apache/storm/starter/WordCountTopology.java

import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

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
        Date currentTime = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

        //String outputDirectory = "../storm_latency/wordcount/test-" + format.format(currentTime).toString();
        //String outputDirectory = "/data/storm_latency/wordcount/test-" + format.format(currentTime).toString();
        
        Number numSpout = 5;
        Number numSplit = 8;
        Number numCount = 12;
        Number numWorkers = 1;
        Integer dataSize = 250;
        Long sleepTimeMili = 1L;
        Integer sleepTimeNano = 0;


        if (args != null && args.length > 0) {
            topologyName = args[0];
            numWorkers = Integer.parseInt(args[1]);
            numSpout = Integer.parseInt(args[2]);
            numSplit = Integer.parseInt(args[3]);
            numCount = Integer.parseInt(args[4]);
            //sleepTimeMili = Long.parseLong(args[5]);
            //sleepTimeNano = Integer.parseInt(args[6]);
            dataSize = Integer.parseInt(args[5]);
        }

        String outputDirectory = "/data/storm/wordcount/" + topologyName + format.format(currentTime).toString();

        //outputDirectory += numWorkers.toString() + "_" + numSpout.toString() + "_" + numSplit.toString() + "_"
        //        + numCount.toString() + "_" + dataSize.toString() + "_" + format.format(currentTime).toString();
        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        builder.setSpout("spout", new RandomSentenceSpout(outputDirectory, dataSize, (long) sleepTimeMili, (int) sleepTimeNano), numSpout).setNumTasks(numSpout);
        builder.setBolt("split", new SplitSentenceBolt(outputDirectory), numSplit).shuffleGrouping("spout").setNumTasks(numSplit);
        builder.setBolt("count", new WordCountBolt(outputDirectory), numCount).fieldsGrouping("split", new Fields("word")).setNumTasks(numCount);

        conf.setDebug(true);
        conf.setNumWorkers(numWorkers.intValue());
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

        return submit(topologyName, conf, builder);
    }
}
