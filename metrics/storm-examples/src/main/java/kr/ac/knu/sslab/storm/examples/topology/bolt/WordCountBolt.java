package kr.ac.knu.sslab.storm.examples.topology.bolt;

// Reference: https://github.com/apache/storm/blob/v2.3.0/examples/storm-starter/src/jvm/org/apache/storm/starter/bolt/WordCountBolt.java

import java.util.Map;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;

import org.apache.storm.metric.api.AssignableMetric;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class WordCountBolt extends BaseBasicBolt {
    public  String outputDirectory;
    public  File file;
    public  BufferedWriter writer;
    Map<String, Long> counts = new HashMap<String, Long>();
    //private transient AssignableMetric assignableMetric;
    //private transient CombinedMetric combinedMetricl;
    //private transient CountMetric countMetric;
    //private transient ReducedMetric reducedMetric;


    public WordCountBolt(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        //File outputDir = new File(outputDirectory);
        //if (!outputDir.exists()) {
        //    outputDir.mkdirs();
        //}
        //file = new File(Paths.get(outputDirectory, "wordcount_latency.txt").toString());
        //try {
        //    writer =  new BufferedWriter(new FileWriter(file, true));
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}
    }

    @Override
    public void cleanup() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        //countMetric = new CountMetric();
        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        file = new File(Paths.get(outputDirectory, "wordcount_latency.txt").toString());
        try {
            writer =  new BufferedWriter(new FileWriter(file, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Long currentTimeNano = System.nanoTime();
        String word = tuple.getString(0);
        Long count = counts.get(word);

        if (count == null) {
            count = 0L;
        }

        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count));
        //countMetric.incr();
        Long latencyNono = System.nanoTime() - currentTimeNano;
        try {
            writer.append(Long.toString(latencyNono) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
