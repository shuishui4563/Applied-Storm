import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by Administrator on 2016/12/10.
 */
public class EmailExtractor extends BaseBasicBolt{
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare( new Fields("emial"));
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String commit = tuple.getStringByField("commit");
        String[] parts = commit.split(" ");
        basicOutputCollector.emit(new Values(parts[1]));
    }
}
