import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/12/10.
 */
public class EmailCounter extends BaseBasicBolt {
    private Map<String,Integer> counts;
    public void prepare(Map config, TopologyContext context){
        counts = new HashMap<String,Integer>();
    }
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String email = tuple.getStringByField("emial");
        counts.put(email,countFor(email)+1);
        printCounts();
    }

    private Integer countFor(String email){
        Integer count = counts.get(email);
        return count==null ? 0 : count;
    }

    private void printCounts(){
        for(String email : counts.keySet()){
            System.out.println(String.format("%s has count of %d",email,counts.get(email)));
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
