package kr.ac.knu.sslab.storm.examples.topology.bolt;

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

public class SplitSentenceBolt extends BaseBasicBolt {
    private int taskId;
    private Meter splitMeter;
    private Counter splitCounter;
    private Histogram splitLatencyHistogram;
    private Histogram splitDeliveryHistogram;

    public SplitSentenceBolt() {
    }
    
    @Override
    public void cleanup() {
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        this.taskId = context.getThisTaskId();
        this.splitMeter = context.registerMeter("wc-splitMeter-" + Integer.toString(this.taskId));
        this.splitCounter = context.registerCounter("wc-splitCounter-" + Integer.toString(this.taskId));
        this.splitLatencyHistogram = context.registerHistogram("wc-splitLatencyHistogram-"  + Integer.toString(this.taskId));
        this.splitDeliveryHistogram = context.registerHistogram("wc-splitDeliveryHistogram-"  + Integer.toString(this.taskId));
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
        // latency
        this.splitLatencyHistogram.update(endTimeNano - currentTimeNano);
        // delivery time
        this.splitDeliveryHistogram.update(currentTimeNano - sendTime);
        this.splitMeter.mark();
        this.splitCounter.inc();
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
