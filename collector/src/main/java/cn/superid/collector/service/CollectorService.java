package cn.superid.collector.service;

import cn.superid.collector.entity.option.MobileOption;
import cn.superid.collector.entity.view.MobilePageView;
import cn.superid.collector.entity.option.Option;
import cn.superid.collector.entity.view.PageView;

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

    List<PageView> extractPageView(MobilePageView mobilePageView);

    String peekPage();

    String peekOption();

    void sendMessage(String topicName, Object msg);
}
