package kr.ac.knu.sslab.storm.examples.topology.bolt;

import java.util.ArrayList;
import java.util.HashMap;

// Reference: https://github.com/apache/storm/blob/v2.3.0/examples/storm-starter/src/jvm/org/apache/storm/starter/bolt/WordCountBolt.java

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


public class WordCountBolt2 extends BaseBasicBolt {
    private Map<String, Long> counts = new HashMap<String, Long>();
    private int taskId;
    private Long measuredTime;
    private Long previousArrivalTime;
    private ArrayList<Long> serviceTime;
    private ArrayList<Long> arrivalInterval;
    private String outputDirectory;

    File file;
    BufferedWriter writer;

    public WordCountBolt2(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        this.counts = new HashMap<String, Long>();
        this.taskId = context.getThisTaskId();
        this.measuredTime = System.nanoTime();
        this.previousArrivalTime = this.measuredTime;
        this.serviceTime = new ArrayList<Long>();
        this.arrivalInterval = new ArrayList<Long>();

        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        String filename = "count" + Integer.toString(this.taskId) + ".txt";
        file = new File(Paths.get(outputDirectory, filename).toString());
        try {
            writer = new BufferedWriter(new FileWriter(file, true));
            writer.append("COUNT START: " + Long.toString(this.previousArrivalTime) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Long currentTimeNano = System.nanoTime();

        String word = tuple.getStringByField("word");
        Long count = counts.get(word);

        if (count == null) {
            count = 0L;
        }

        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count, System.nanoTime()));

        Long endTimeNano = System.nanoTime();
        Long sendTime = tuple.getLongByField("timestamp");
        Long latency = endTimeNano - currentTimeNano;

        try {
            writer.append("S:" + Long.toString(currentTimeNano) + "\n");
            writer.append("I: "+Long.toString(currentTimeNano - this.previousArrivalTime) + "\n");
            writer.append("P: "+Long.toString(endTimeNano - currentTimeNano) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //this.arrivalInterval.add(currentTimeNano - this.previousArrivalTime);
        //this.serviceTime.add(endTimeNano - currentTimeNano);
        this.previousArrivalTime = currentTimeNano;
        
    }
    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count", "timestamp"));
    }

    @Override
    public void cleanup() {
        this.measuredTime = System.nanoTime() - this.measuredTime;
        try {
            writer.append("COUNT END: " + Long.toString(System.nanoTime()) + "\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //this.measuredTime = System.nanoTime() - this.measuredTime;
        //String filename = "count" + Integer.toString(this.taskId) + ".txt";
        //File file = new File(Paths.get(outputDirectory, filename).toString());
        //try {
        //    BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
        //    writer.append("COUNT\n");
        //    writer.append("Total Measure Time START\n");
        //    writer.append(Long.toString(this.measuredTime) + "\n");
        //    writer.append("Total Measure Time END\n");
        //    writer.append("Service Time START\n");
        //    for (Long time : serviceTime) {
        //        writer.append(Long.toString(time) + "\n");
        //    }
        //    writer.append("Service Time END\n");
        //    writer.append("Arrival Interval START\n");
        //    for (Long time : arrivalInterval) {
        //        writer.append(Long.toString(time) + "\n");
        //    }
        //    writer.append("Arrival Interval END\n");

        //    writer.close();
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}
    }
}
