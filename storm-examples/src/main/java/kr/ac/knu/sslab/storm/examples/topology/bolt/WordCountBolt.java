package kr.ac.knu.sslab.storm.examples.topology.bolt;

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


public class WordCountBolt extends BaseBasicBolt {
    private Map<String, Long> counts = new HashMap<String, Long>();
    private int taskId;
    private Meter countMeter;
    private Counter countCounter;
    private Histogram countLatencyHistogram;
    private Histogram countDeliveryHistogram;


    public WordCountBolt() {
    }

    @Override
    public void cleanup() {
    }


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        this.taskId = context.getThisTaskId();
        this.countMeter = context.registerMeter("wc-countMeter-" + Integer.toString(this.taskId));
        this.countCounter = context.registerCounter("wc-countCounter-"  + Integer.toString(this.taskId));
        this.countLatencyHistogram = context.registerHistogram("wc-countLatencyHistogram-"  + Integer.toString(this.taskId));
        this.countDeliveryHistogram = context.registerHistogram("wc-countDeliveryHistogram-"  + Integer.toString(this.taskId));
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
        this.countLatencyHistogram.update(latency);
        this.countDeliveryHistogram.update(currentTimeNano - sendTime);
        this.countMeter.mark();
        this.countCounter.inc();
    }
    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count", "timestamp"));
    }
}
