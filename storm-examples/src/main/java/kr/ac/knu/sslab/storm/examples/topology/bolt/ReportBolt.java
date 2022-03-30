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

public class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> counts = null;
    //private OutputCollector collector;
    private int taskId;
    private Meter reportMeter;
    private Counter reportCounter;
    private Histogram reportLatencyHistogram;
    private Histogram reportDeliveryHistogram;

    public ReportBolt() {
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
        //this.collector = outputCollector;
        this.taskId = topologyContext.getThisTaskId();
        this.reportMeter = topologyContext.registerMeter("wc-reportMeter-" + Integer.toString(this.taskId));
        this.reportCounter = topologyContext.registerCounter("wc-reportCounter-"  + Integer.toString(this.taskId));
        this.reportLatencyHistogram = topologyContext
                .registerHistogram("wc-reportLatencyHistogram-" + Integer.toString(this.taskId));
        this.reportDeliveryHistogram = topologyContext.registerHistogram("wc-reportDeliveryHistogram-"  + Integer.toString(this.taskId));
    }

    @Override
    public void execute(Tuple tuple) {
        Long currentTimeNano = System.nanoTime();
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);

        Long endTimeNano = System.nanoTime();
        Long sendTime = tuple.getLongByField("timestamp");
        // latency
        this.reportLatencyHistogram.update(endTimeNano - currentTimeNano);
        // delivery time
        this.reportDeliveryHistogram.update(currentTimeNano - sendTime);
        this.reportMeter.mark();
        this.reportCounter.inc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // do not need to create Output Stream;
    }

    @Override
    public void cleanup() {
    }

}
