package cn.superid.collector.service;

import cn.superid.collector.entity.MobileOption;
import cn.superid.collector.entity.Option;
import cn.superid.collector.entity.PageView;

import java.util.List;

/**
 * @author dufeng
 * @create: 2018-07-18 17:26
 */
public interface CollectorService {

    void save(PageView pageView);
    void save(Option option);

//    void save(MobileOption option);

    List<Option> extractOption(MobileOption mobileOption);

    String peekPage();

    String peekOption();

    void sendMessage(String topicName, Object msg);
}
