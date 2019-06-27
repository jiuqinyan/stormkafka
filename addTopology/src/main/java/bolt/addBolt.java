package bolt;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
public class addBolt extends BaseRichBolt   {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	OutputCollector _collector;
	JedisPool pool;
	Jedis jedis;
    @SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        this.pool = new JedisPool(new JedisPoolConfig(), "localhost",6379,2 * 60000,"123456");
        jedis = pool.getResource();
    }
    public void execute(Tuple tuple) {
    	System.out.println("从kafka接受的数据===========================>"+tuple.getString(0));
    	System.out.println("从kafka接受的数据===========================>"+tuple.toString());
    	int sum=Integer.parseInt(jedis.get("add"));
    	sum=sum+Integer.parseInt(tuple.getString(4));
    	jedis.set("add", String.valueOf(sum));
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }    
   
	
}
