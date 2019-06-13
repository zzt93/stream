package cn.superid.streamer.config;

import cn.superid.id_client.IdClient;
import cn.superid.id_client.core.EnableIdGenerator;
import com.alibaba.druid.pool.DruidDataSource;
import org.exemodel.cache.ICache;
import org.exemodel.cache.impl.RedisTemplate;
import org.exemodel.session.AbstractSession;
import org.exemodel.session.impl.JdbcSessionFactory;
import org.exemodel.transation.spring.TransactionManager;
import org.exemodel.util.StringUtil;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import redis.clients.jedis.JedisPoolConfig;

import javax.sql.DataSource;

/**
 * DataSourceConfig
 * 数据库配置
 * @author xuan
 * @date 2018/8/2
 */
@Configuration
@EnableIdGenerator
@EnableTransactionManagement
@Order(1)
public class DataSourceConfig implements CommandLineRunner {


    @Value("${spring.profiles.active}")
    private String env;


    @Bean
    @ConfigurationProperties(prefix = "druid.datasource")
    public DataSource DataSource() {
        return new DruidDataSource();
    }

    @Bean
    public JedisPoolConfig JedisPoolConfig(
            @Value("${redis.pool.min-idle}") int minIdle,
            @Value("${redis.pool.max-idle}") int maxIdle,
            @Value("${redis.pool.max-wait}") int maxWaitMillis,
            @Value("${redis.pool.block-when-exhausted}") boolean blockWhenExhausted,
            @Value("${redis.pool.max-total}") int maxTotal) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(minIdle);
        config.setMaxIdle(maxIdle);
        config.setMaxWaitMillis(maxWaitMillis);
        config.setMaxTotal(maxTotal);
        config.setBlockWhenExhausted(blockWhenExhausted);
        return config;
    }

    @Bean
    public ICache Cache(
            @Qualifier("JedisPoolConfig") JedisPoolConfig config,
            @Value("${redis.host}") String host,
            @Value("${redis.port}") int port,
            @Value("${redis.password}") String password,
            @Value("${redis.timeout}") int timeout,
            @Value("${redis.database}") int database,
            @Value("${redis.ssl}") boolean ssl) {
        if(StringUtil.isEmpty(password)){
            password = null;
        }
        return new RedisTemplate(config, host, port, timeout, password, database, ssl);
    }


    @Bean
    RedissonClient redissonSingle(
            @Value("${redis.host}") String host,
            @Value("${redis.port}") int port,
            @Value("${redis.password}") String password,
            @Value("${redis.timeout}") int timeout,
            @Value("${redis.pool.min-idle}") int minIdle,
            @Value("${redis.pool.max-total}") int maxTotal
    ){

        Config config = new Config();
        SingleServerConfig singleServerConfig = config.useSingleServer()
                .setAddress("redis://"+host+":"+port)
                .setTimeout(timeout)
                .setConnectionPoolSize(maxTotal)
                .setConnectionMinimumIdleSize(minIdle)
                .setPassword(password);
        return Redisson.create(config);
    }


    @Bean
    public JdbcSessionFactory JdbcSessionFactory(
            @Qualifier("DataSource") DataSource dataSource,
            @Qualifier("Cache") ICache cache) {
        return new JdbcSessionFactory(dataSource, cache);
    }


    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("JdbcSessionFactory")JdbcSessionFactory jdbcSessionFactory) {
        TransactionManager transactionManager = new TransactionManager(){
            @Override
            protected void doBegin(Object o, TransactionDefinition transactionDefinition) throws TransactionException {
                super.doBegin(o, transactionDefinition);
                if("release".equals(env)){
                    AbstractSession.currentSession().execute("set drds_transaction_policy = 'flexible'");
                }
            }
        };
        transactionManager.setSessionFactory(jdbcSessionFactory);
        return transactionManager;
    }


    @Autowired
    private IdClient idClient;
    @Value("${spring.cloud.consul.discovery.serviceName:business}")
    private String service;

    @Override
    public void run(String... strings) throws Exception {
        DStatement.setIdClient(idClient);
        DStatement.setService(service);
    }
}
