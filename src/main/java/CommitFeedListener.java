import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.zookeeper.common.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


/**
 * Created by Administrator on 2016/12/10.
 */
public class CommitFeedListener extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;
    private List<String> commits;

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("commit"));
    }

    public void open(Map configMap, TopologyContext topologyContext, SpoutOutputCollector OutputCollector) {
        this.outputCollector = OutputCollector;
        try {
            commits = org.apache.commons.io.IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"), Charset.defaultCharset().name());
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    public void nextTuple() {
        for (String commit:commits){
            outputCollector.emit(new Values(commit));
        }

    }
}
