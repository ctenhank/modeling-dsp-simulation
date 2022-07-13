package kr.ac.knu.sslab.storm.examples.topology.bolt;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ReportBolt2 extends BaseRichBolt {
    private HashMap<String, Long> counts = null;
    //private OutputCollector collector;
    private int taskId;
    private Long measuredTime;
    private Long previousArrivalTime;
    private ArrayList<Long> serviceTime;
    private ArrayList<Long> arrivalInterval;
    private String outputDirectory;

    File file;
    BufferedWriter writer;

    public ReportBolt2(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
        //this.collector = outputCollector;
        this.taskId = topologyContext.getThisTaskId();
        this.measuredTime = System.nanoTime();
        this.previousArrivalTime = this.measuredTime;
        this.serviceTime = new ArrayList<Long>();
        this.arrivalInterval = new ArrayList<Long>();

        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        String filename = "report" + Integer.toString(this.taskId) + ".txt";
        file = new File(Paths.get(outputDirectory, filename).toString());
        try {
            writer = new BufferedWriter(new FileWriter(file, true));
            writer.append("REPORT START: " + Long.toString(this.previousArrivalTime) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Long currentTimeNano = System.nanoTime();
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        //Long totalCount = this.counts.get(word);
        this.counts.put(word, count);

        Long endTimeNano = System.nanoTime();
        Long sendTime = tuple.getLongByField("timestamp");
        //this.arrivalInterval.add(currentTimeNano - this.previousArrivalTime);
        //this.serviceTime.add(endTimeNano - currentTimeNano);

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
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // do not need to create Output Stream;
    }

    @Override
    public void cleanup() {
        this.measuredTime = System.nanoTime() - this.measuredTime;
        try {
            writer.append("REPORT END: " + Long.toString(System.nanoTime()) + "\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
