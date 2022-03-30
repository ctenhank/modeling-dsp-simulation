package kr.ac.knu.sslab.storm.examples.topology.bolt;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> counts = null;
    OutputCollector collector;
    public  String outputDirectory;
    public  File latency_file;
    public  BufferedWriter latency_writer;
    public  File result_file;
    public  BufferedWriter result_writer;

    public ReportBolt(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        //latency_file = new File(Paths.get(outputDirectory, "report_latency.txt").toString());
        //result_file = new File(Paths.get(outputDirectory, "result.txt").toString());
        //try {
        //    latency_writer = new BufferedWriter(new FileWriter(latency_file, true));
        //    result_writer =  new BufferedWriter(new FileWriter(result_file, true));
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
        this.collector = outputCollector;
        latency_file = new File(Paths.get(outputDirectory, "report_latency.txt").toString());
        result_file = new File(Paths.get(outputDirectory, "result.txt").toString());
        try {
            latency_writer = new BufferedWriter(new FileWriter(latency_file, true));
            result_writer =  new BufferedWriter(new FileWriter(result_file, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Long currentTimeNano = System.nanoTime();
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
        Long latencyNono = System.nanoTime() - currentTimeNano;
        try {
            latency_writer.append(Long.toString(latencyNono) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // do not need to create Output Stream;
    }

    @Override
    public void cleanup() {

        try {
            result_writer.append("---- FINAL COUNT ----\n");
            // System.out.println("---- FINAL COUNT ----");

            for (String key : this.counts.keySet()) {
                result_writer.append(key + ": " + this.counts.get(key));
                // System.out.println(key + ": " + this.counts.get(key));
            }
            result_writer.append("---------------------\n");
            // System.out.println("---------------------");
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            latency_writer.close();
            result_writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
