package cn.yahoo.redis.manager;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

/**
 * @author guangyi.kou *
 */
public class RedisManager {
	private static Logger log = LoggerFactory.getLogger(RedisManager.class);
	private static List<String> groups = null;
	private static String zookeeperConnectionString = "";// zk链接
	private static CuratorFramework client = null;
	private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,
			3);
	private static final String rootPath = "/redis";

	private static Watcher groupWatcher = new Watcher() {
		// watcher
		public void process(WatchedEvent event) {
			System.out.println("groups event:" + event.getType() + " path:"
					+ event.getPath());
			try {
				// 先获得当前的groups
				List<String> it = client.getChildren().forPath(rootPath);
				if (groups != null && groups.size() > 0) {// 干掉之前就有的，因为基本上来说就只有+的可能，不能减少group
					it.removeAll(groups);
				}
				handleGroups(it);
			} catch (Exception e) {
				e.printStackTrace();
			}
			// 继续观察
			addZKGroupWatcher();
		}
	};

	/**
	 * 它的任务是监控zk的/redis下的所有group，发现有改动，立刻修改redis的主从关系
	 *
	 * @param args
	 * @author guangyi.kou
	 */
	public static void main(String[] args) {
		// 参数请自己保证正确
		if (args == null || args.length != 2) {
			log.error("参数有误！");
			System.exit(0);
		}
		zookeeperConnectionString = args[0];
		PropertyConfigurator.configure(args[1]);
		// 1.得到groups并存起来
		client = getGroups();
		if (client == null) {
			log.error("zk连接创建有误！");
			System.exit(0);
		}
		// 2.\redis+watcher
		addZKGroupWatcher();
		// 3.处理每个group
		handleGroups(groups);
	}

	/**
	 * 1.得到groups并存起来
	 *
	 * @return
	 */
	private static CuratorFramework getGroups() {
		try {
			client = CuratorFrameworkFactory.newClient(
					zookeeperConnectionString, retryPolicy);
			client.start();
			groups = client.getChildren().forPath(rootPath);
			log.info("初始化...");
			return client;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 2.\redis+watcher
	 */
	private static void addZKGroupWatcher() {
		try {
			client.getChildren().usingWatcher(groupWatcher).forPath(rootPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 3.处理groups
	 *
	 * @param groups
	 */
	private static void handleGroups(List<String> groups) {
		for (String group : groups) {
			handleGroup(rootPath + "/" + group);
		}
	}

	/**
	 * 处理group
	 *
	 * @param path
	 */
	private static void handleGroup(String path) {
		String data = "";
		String[] datas = null;
		String[] mdatas = null;
		String master = "";
		Watcher watcher = null;
		try {
			List<String> it = client.getChildren().forPath(path);
			Collections.sort(it);
			for (int i = 0; i < it.size(); i++) {
				data = new String(client.getData().forPath(
						path + "/" + it.get(i)));
				// 默认刚上来时是ip:host，正常主应该是host:port_M，从应该是host:port_S_host:port，后面的host:port是它的M
				datas = data.split("_");
				if (i == 0) {// 这个是主
					if (datas.length != 2) {// 初始或刚从S升上来，要设置成主
						setRedisM(datas[0]);
						client.setData().forPath(path + "/" + it.get(i),
								(datas[0] + "_M").getBytes());
					}
				} else {// 从
					if (datas.length == 1) {// 初始化，要给它加个M
						setRedisS(datas[0], mdatas[0]);
						client.setData().forPath(path + "/" + it.get(i),
								(datas[0] + "_S_" + mdatas[0]).getBytes());
					} else {
						// 得到M
						master = datas[2];
						// 和上个去判断，不一样要重新设置
						if (!master.equals(mdatas[0])) {
							setRedisS(datas[0], mdatas[0]);
							client.setData().forPath(path + "/" + it.get(i),
									(datas[0] + "_S_" + mdatas[0]).getBytes());
						}
					}
				}
				mdatas = datas;
			}
			watcher = new Watcher() {
				// watcher
				public void process(WatchedEvent event) {
					System.out.println("event:" + event.getType() + " path:"
							+ event.getPath());
					handleGroup(event.getPath());
					// 继续观察
					addWatcher(this, event.getPath());
				}
			};
			addWatcher(watcher, path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 添加watcher
	 */
	private static void addWatcher(Watcher watcher, String path) {
		try {
			client.getChildren().usingWatcher(watcher).forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 设置M
	 */
	private static void setRedisM(String redis) {
		try {
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			jedis.slaveofNoOne();
			jedis.configSet("appendonly", "no");
			jedis.configSet("save", "");
			jedis.disconnect();
			log.info("redis服务器 " + redis + "设为主");
		} catch (Exception e) {
			log.error("redis服务器 " + redis + " 失去连接");
		}
	}

	/**
	 * 设置S
	 */
	private static void setRedisS(String redis, String m_redis) {
		try {
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			jedis.slaveof(getHost(m_redis), getPort(m_redis));
			jedis.configSet("appendonly", "yes");
			// TODO 需要实际算一下规则
			jedis.configSet("appendfsync", "everysec");
			jedis.configSet("save", "900 1 300 10 60 10000");
			jedis.disconnect();
			log.info("redis服务器 " + redis + "设为" + m_redis + "的从");
		} catch (Exception e) {
			log.error("redis服务器 " + redis + " 失去连接");
		}
	}

	/**
	 * 得到ip:host字段的ip
	 *
	 * @param redisString
	 * @return
	 */
	private static String getHost(String redisString) {
		return redisString.split(":")[0];
	}

	/**
	 * 得到ip:host字段的port
	 *
	 * @param redisString
	 * @return
	 */
	private static Integer getPort(String redisString) {
		return Integer.valueOf(redisString.split(":")[1]);
	}
}
