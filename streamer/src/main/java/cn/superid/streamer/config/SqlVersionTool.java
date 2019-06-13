package cn.superid.streamer.config;

import cn.superid.common.utils.lock.RedisLock;
import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import lombok.extern.slf4j.Slf4j;
import org.exemodel.cache.ICache;
import org.exemodel.session.AbstractSession;
import org.exemodel.session.Session;
import org.exemodel.util.ParameterBindings;
import org.exemodel.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

/**
 * SqlVersionTool
 * sql版本控制
 * @author xuan
 * @date 2018/8/22
 */
@Configuration
@Slf4j
@Order(1)
public class SqlVersionTool implements CommandLineRunner {
    @Value("${sqlVersion.path:/sql-change-set.yml}")
    private String path;
    @Value("${sqlVersion.executeBaseline:false}")
    private boolean executeBaseline;
    @Value("${sqlVersion.tag:default}")
    private String tag;
    @Value("${spring.application.name}")
    private String serviceName;
    @Autowired
    private ICache cache;
    private int baseline = 1;

    private final static String FIND_MAX_VERSION = "select max(id) from sql_version";
    private final static String INSERT_VERSION = " insert into sql_version (id,update_sql,ip) values (?,?,?)";
    private final static String defaultTag = "default";
    private final static String INIT_TABLE_SQL = "create table if not exists  `sql_version` (\n" +
            "  `id` int(11) NOT NULL,\n" +
            "  `update_sql` text NOT NULL,\n" +
            "  `execute_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  `ip` varchar(100) NOT NULL,\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";


    @Override
    public void run(String... strings) throws Exception {
        RedisLock redisLock = new RedisLock(() -> cache.getJedis(), serviceName);
        redisLock.tryLock();
        try (Session session = AbstractSession.currentSession()) {
            session.execute(INIT_TABLE_SQL);
            InetAddress addr = InetAddress.getLocalHost();
            Integer curVersion = session.findOneByNativeSql(Integer.class, FIND_MAX_VERSION);
            seekSql(curVersion == null ? 0 : curVersion).forEach(entry -> {
                Map<String, String> change = entry.getValue();
                String sql = getSqlByTag(change, tag);
                if (StringUtil.notEmpty(sql)) {
                    Arrays.stream(sql.split(";")).filter(StringUtil::notEmpty).forEach(s -> {
                        log.info("Execute sql " + s);
                        session.execute(s);
                    });
                    session.executeUpdate(INSERT_VERSION,
                            new ParameterBindings(Integer.parseInt(entry.getKey()), sql, addr.toString()));
                }
            });
        }
        redisLock.unlock();
    }

    private List<Map.Entry<String, Map<String, String>>> seekSql(int version) {
        InputStream in = SqlVersionTool.class.getResourceAsStream(path);
        if(in==null){
            return new ArrayList<>();
        }
        YamlReader reader = new YamlReader(new BufferedReader(new InputStreamReader(in)));
        try {
            Map<String, Map<String, String>> map = reader.read(Map.class);
            List<Map.Entry<String, Map<String, String>>> res =  map.entrySet().stream()
                    .filter(entry -> {
                        int key = Integer.parseInt(entry.getKey());
                        if (key==baseline&& "drds".equals(tag)) {
                            return false;
                        }
                        if (version == 0 && key==baseline) {
                            return executeBaseline;
                        }
                        return Integer.parseInt(entry.getKey()) > version;
                    })
                    .sorted(Comparator.comparingInt(e -> Integer.parseInt(e.getKey())))
                    .collect(Collectors.toList());
            return res;
        } catch (YamlException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
                reader.close();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }

    private String getSqlByTag(Map<String, String> change, String tag) {
        if(change==null){
            return null;
        }
        String sql = change.get(tag);
        if (StringUtil.isEmpty(sql)) {
            sql = change.get(defaultTag);
        }
        if (StringUtil.notEmpty(sql)) {
            return loadSqlFile(sql);
        } else {
            if ("mysql".equals(tag)) {
                sql = loadSqlFile(change.get("drds"));
                if (sql != null) {
                    sql = sql.replaceAll("dbpartition.*;", ";");
                    sql = sql.replaceAll("BY GROUP|BY SIMPLE", "");
                }
            }
            return sql;
        }
    }

    private String loadSqlFile(String sql) {
        if (sql.contains(".sql")) {
            try {
                InputStream in = SqlVersionTool.class.getResourceAsStream(sql);
                if(in == null){
                    throw new RuntimeException("file "+sql+" not exist");
                }
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));

                String line;
                StringBuilder stringBuilder = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    stringBuilder.append(line);
                    stringBuilder.append("\n");
                }
                reader.close();
                in.close();
                return stringBuilder.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return sql;
    }
}
