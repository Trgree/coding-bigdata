package org.ace.mr.redis.write;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 *  Redis集群连接工具
 *  使用和hadoop配置格式一样的配置文件，并加入到配置配置Configuration中
 */
public class JedisClusterTool {

	private JedisCluster jedisCluster;
	private Pattern p = Pattern.compile("^.+[:]\\d{1,5}\\s*$");
	private Configuration hadoopConfig;
	private GenericObjectPoolConfig conf = new GenericObjectPoolConfig();

    /**
     * 初始化
	 * @param hadoopConfig 包含redis配置的Configuration
	 * @throws Exception
	 */
	public JedisClusterTool(Configuration hadoopConfig) throws Exception {
		this.hadoopConfig = hadoopConfig;
		initConf();
		initCluster();
	}

	private void initConf() {
		conf.setMinIdle(hadoopConfig.getInt("redis.minIdle", 2));
		conf.setMaxIdle(hadoopConfig.getInt("redis.maxIdle", 100));
		conf.setMaxTotal(hadoopConfig.getInt("redis.maxTotal", 1000));
		conf.setMaxWaitMillis(hadoopConfig.getInt("redis.maxWaitMillis", -1));
	}

	/**
     * 获取JedisCluster
	 * @return
     */
	public JedisCluster getCluster() {
		return jedisCluster;
	}

	private Set<HostAndPort> parseHostAndPort() throws Exception {
		try {
			Set<HostAndPort> nodes = new HashSet<HostAndPort>();
			String hosts = hadoopConfig.get("redis.hosts").trim();
			for(String host : hosts.split(",")){
				boolean isIpPort = p.matcher(host.trim()).matches();
				if (!isIpPort) {
					throw new IllegalArgumentException("ip 或 port 不合法");
				}
				String[] ipAndPort = host.split(":");
				HostAndPort hap = new HostAndPort(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
				nodes.add(hap);
			}
			return nodes;
		} catch (IllegalArgumentException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new Exception("解析 jedis 配置文件失败", ex);
		}
	}

	public void initCluster() throws Exception {
		Set<HostAndPort> haps = this.parseHostAndPort();
		int connectionTimeout = hadoopConfig.getInt("redis.connectionTimeout", 2000);
		int soTimeout = hadoopConfig.getInt("redis.soTimeout", 2000);
		int maxAttempts = hadoopConfig.getInt("redis.maxAttempts", 5);
		String password = hadoopConfig.get("redis.password");
		if(password == null || password.trim().equals("")){
			jedisCluster = new JedisCluster(haps, connectionTimeout, soTimeout, maxAttempts, conf);
		} else {
			jedisCluster = new JedisCluster(haps, connectionTimeout, soTimeout, maxAttempts, password, conf);
		}
	}
}
