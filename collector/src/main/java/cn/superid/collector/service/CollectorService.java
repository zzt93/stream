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
    /**
     * 保存页面浏览信息到mongodb
     * @param pageView
     */
    void save(PageView pageView);

    /**
     * 保存用户操作信息到mongodb
     * @param option
     */
    void save(Option option);

//    void save(MobileOption option);

    List<Option> extractOption(MobileOption mobileOption);

    List<PageView> extractPageView(MobilePageView mobilePageView, String ip);

    String peekPage();

    String peekOption();

    /**
     * 发送消息到Kafka
     * @param topicName
     * @param msg
     */
    void sendMessage(String topicName, Object msg);
}
