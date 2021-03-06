package cn.superid.collector.service.impl;

import cn.superid.collector.entity.option.MobileOption;
import cn.superid.collector.entity.view.MobilePageView;
import cn.superid.collector.entity.option.Option;
import cn.superid.collector.entity.view.PageView;
import cn.superid.collector.service.CollectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.util.StringUtils;

/**
 * @author dufeng
 * @create: 2018-07-18 17:27
 */
@Service
public class CollectorServiceImpl implements CollectorService {
    private final Logger LOGGER = LoggerFactory.getLogger(CollectorServiceImpl.class);
    private final int TWENTY = 20;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MongoTemplate mongo;

    /**
     * mongo上的集合名字，用于存放页面浏览信息
     */
    @Value("${collector.mongo.page}")
    private String pageCollection;
    /**
     * mongo上的集合名字，用于存放用户操作信息
     */
    @Value("${collector.mongo.option}")
    private String optionCollection;

    @Value("${kafka.topic.page}")
    private String pageTopic;

    @Value("${kafka.topic.option}")
    private String optionTopic;

    @Autowired
    public CollectorServiceImpl(KafkaTemplate<String, String> kafkaTemplate, MongoTemplate mongo) {
        this.kafkaTemplate = kafkaTemplate;
        this.mongo = mongo;
    }

    private final ConcurrentLinkedDeque<PageView> pageQueue = new ConcurrentLinkedDeque<>();
    private final AtomicLong pageEstimatedSize = new AtomicLong();

    private final ConcurrentLinkedDeque<Option> optionQueue = new ConcurrentLinkedDeque<>();
    private final AtomicLong optionEstimatedSize = new AtomicLong();

    @Async
    @Override
    public void save(PageView pageView) {
        LOGGER.debug("save pageView to mongo: {}", pageView);
        saveMongoAndMemory(pageView, pageQueue, pageEstimatedSize, pageCollection);
    }

    @Async
    @Override
    public void save(Option option) {
        LOGGER.debug("save option to mongo : {}", option);
        saveMongoAndMemory(option, optionQueue, optionEstimatedSize, optionCollection);
    }

    private <T> void saveMongoAndMemory(T option, ConcurrentLinkedDeque<T> queue, AtomicLong c, String collection) {
        try {
            mongo.insert(option, collection);
        } catch (Exception e) {
            LOGGER.error("", e);
        }

        queue.addLast(option);
        if (queue.size() >= TWENTY) {
            queue.removeFirst();
        }
        c.incrementAndGet();
    }

    @Override
    public List<Option> extractOption(MobileOption mobileOption) {
        List<Option> options = new ArrayList<>();
        for (MobileOption.OptionEntry innerEntry : mobileOption.getInnerEntries()) {
            options.add(new Option.Builder().viewId(mobileOption.getViewId())
                    .userId(mobileOption.getUserId())
                    .devType(mobileOption.getDevType())
                    .appVer(mobileOption.getAppVer())
                    .uploadTime(mobileOption.getUploadTime())
                    .businessLine(innerEntry.getBusinessLine())
                    .pageUri(innerEntry.getPageUri())
                    .eleId(innerEntry.getEleId())
                    .attrs(innerEntry.getAttrs())
                    .clientIp(innerEntry.getClientIp())
                    .opTime(innerEntry.getOpTime())
                    .build());
        }

        if(CollectionUtils.isEmpty(options)){
            return Collections.emptyList();
        }

        return options;
    }

    @Override
    public List<PageView> extractPageView(MobilePageView mobilePageView, String ip) {
        List<PageView> views = new ArrayList<>();
        for (MobilePageView.ViewEntry view : mobilePageView.getInnerEntries()) {
            views.add(new PageView.PageBuilder().setId(mobilePageView.getViewId())
                    .setUserId(mobilePageView.getUserId())
                    .setDevType(mobilePageView.getDevType())
                    .setDevice(mobilePageView.getDevice())
                    .setDomain(mobilePageView.getDomain())
                    .setAppVer(mobilePageView.getAppVer())
                    .setEpoch(mobilePageView.getEpoch())
                    .setServerIp(mobilePageView.getServerIp())
                    .setUploadTime(mobilePageView.getUploadTime())
                    .setUserAgent(mobilePageView.getUserAgent())
                    .setBusinessLine(view.getBusinessLine())
                    .setPageUri(view.getPageUri())
                    .setReferer(view.getReferer())
                    .setCollectTime(view.getCollectTime())
                    .setClientIp(StringUtils.isEmpty(view.getClientIp()) ? ip : view.getClientIp())
                    .setResources(view.getResources())
                    .build());
        }

        if(CollectionUtils.isEmpty(views)){
            return Collections.emptyList();
        }

        return views;
    }


    @Override
    public String peekPage() {
        return peek(pageEstimatedSize,pageQueue);
    }

    @Override
    public String peekOption() {
        return peek(optionEstimatedSize,optionQueue);
    }

    private String peek(AtomicLong estimatedSize, ConcurrentLinkedDeque queue){
        StringBuilder sb = new StringBuilder(2000);
        sb.append("[").append(estimatedSize.get()).append("]");
        Iterator it = queue.iterator();
        for (int i = 0; i < Math.min(TWENTY, estimatedSize.get()) && it.hasNext(); i++) {
            sb.append(it.next());
        }
        return sb.toString();
    }

    @Override
    public void sendMessage(String topicName, Object msg) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(KafkaHeaders.TOPIC, topicName);
        LOGGER.debug("send message to kafka : {}", msg.toString());
        kafkaTemplate.send(new GenericMessage<>(msg.toString(), map));
    }
}
