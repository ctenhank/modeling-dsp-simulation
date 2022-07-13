package kr.ac.knu.sslab.storm.examples.topology.bolt;

import java.util.ArrayList;
import java.util.Map;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

public class SplitSentenceBolt2 extends BaseBasicBolt {
    private int taskId;
    private Long measuredTime;
    private Long previousArrivalTime;
    private ArrayList<Long> serviceTime;
    private ArrayList<Long> arrivalInterval;
    private String outputDirectory;

    File file;
    BufferedWriter writer;

    public SplitSentenceBolt2(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }
    
    @Override
    public void cleanup() {
        this.measuredTime = System.nanoTime() - this.measuredTime;
        try {
            writer.append("SPLIT END: " + Long.toString(System.nanoTime()) + "\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        this.taskId = context.getThisTaskId();
        this.measuredTime = System.nanoTime();
        this.previousArrivalTime = this.measuredTime;
        this.serviceTime = new ArrayList<Long>();
        this.arrivalInterval = new ArrayList<Long>();

        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        String filename = "split" + Integer.toString(this.taskId) + ".txt";
        file = new File(Paths.get(outputDirectory, filename).toString());
        try {
            writer = new BufferedWriter(new FileWriter(file, true));
            writer.append("SPLIT START: " + Long.toString(this.previousArrivalTime) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Long currentTimeNano = System.nanoTime();

        String sentence = tuple.getValueByField("words").toString();
        Long sendTime = tuple.getLongByField("timestamp");
        String words[] = sentence.split(" ");
        for (String w : words) {
            collector.emit(new Values(w, System.nanoTime()));
        }

        Long endTimeNano = System.nanoTime();
        try {
            writer.append("S:" + Long.toString(currentTimeNano) + "\n");
            writer.append("I: "+Long.toString(currentTimeNano - this.previousArrivalTime) + "\n");
            writer.append("P: "+Long.toString(endTimeNano - currentTimeNano) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.previousArrivalTime = currentTimeNano;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
