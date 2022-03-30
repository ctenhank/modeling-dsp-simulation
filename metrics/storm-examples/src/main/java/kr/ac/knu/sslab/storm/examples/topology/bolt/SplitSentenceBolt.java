package kr.ac.knu.sslab.storm.examples.topology.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.storm.metric.api.CountMetric;
//import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseBasicBolt {
    public String outputDirectory;
    public File file;
    public BufferedWriter writer;
    //private transient CountMetric countMetric;

    public SplitSentenceBolt(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        //File outputDir = new File(outputDirectory);
        //if (!outputDir.exists()) {
        //    outputDir.mkdirs();
        //}
        //file = new File(Paths.get(outputDirectory, "split_latency.txt").toString());
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
        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        file = new File(Paths.get(outputDirectory, "split_latency.txt").toString());
        try {
            writer =  new BufferedWriter(new FileWriter(file, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //countMetric = new CountMetric();
        //context.registerMetric("execute_count", countMetric, 60);
        //context.registerMetric

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //countMetric.incr();
        Long currentTimeNano = System.nanoTime();
        String sentence = tuple.getString(0);
        String words[] = sentence.split(" ");
        for (String w : words) {
            collector.emit(new Values(w));
        }
        Long latencyNono = System.nanoTime() - currentTimeNano;
        try {
            writer.append(Long.toString(latencyNono) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
