package topology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import bolt.addBolt;

public class Topology {
	
	public static void main(String[] args) {
		KafkaSpoutConfig<String,String> kafkaSpoutConfig = KafkaSpoutConfig
	            .builder("192.168.234.131:9092", "test")
	            .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
	            .setProp(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000)
	            .setProp(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,30000)
	            .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getCanonicalName())
	            .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getCanonicalName())
	            .setOffsetCommitPeriodMs(10000)//控制spout多久向kafka提交一次offset
	            .setMaxUncommittedOffsets(250)
	            .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST)
	            .build();
	    KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(kafkaSpoutConfig);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout",kafkaSpout,1).setNumTasks(8);
		builder.setBolt("addBolt", new addBolt(), 8).shuffleGrouping("kafkaSpout");
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
        conf.setMaxSpoutPending(100000);   
        conf.setMessageTimeoutSecs(1000);
		conf.setDebug(true);
				try {
					StormSubmitter.submitTopology("add", conf, builder.createTopology());
				} catch (AlreadyAliveException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidTopologyException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (org.apache.storm.generated.AuthorizationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		
	}
}
