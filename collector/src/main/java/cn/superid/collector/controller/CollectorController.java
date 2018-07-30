package cn.superid.collector.controller;

import cn.superid.collector.annotation.RequestBodyNeedDecrypt;
import cn.superid.collector.entity.*;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import javax.servlet.http.HttpServletRequest;

import cn.superid.collector.entity.option.MobileOption;
import cn.superid.collector.entity.option.Option;
import cn.superid.collector.entity.view.MobilePageView;
import cn.superid.collector.entity.view.PageView;
import cn.superid.collector.service.CollectorService;
import cn.superid.collector.util.TimeUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzt
 */
@Api(value = "用户浏览及操作信息采集")
@RestController
@RequestMapping("/collector")
public class CollectorController {

  private static final Logger logger = LoggerFactory.getLogger(CollectorController.class);

  @Autowired
  private CollectorService collectorService;

  /**
   * mongo上的集合名字，用于存放页面浏览信息
   */
  @Value("${collector.mongo.page}")
  private String pages;
  /**
   * mongo上的集合名字，用于存放用户操作信息
   */
  @Value("${collector.mongo.option}")
  private String options;

  @Value("${kafka.topic.page}")
  private String pageTopic;

  @Value("${kafka.topic.option}")
  private String optionTopic;


  @ApiOperation(value = "上报用户浏览页面信息接口（要对请求body做base64加密）", notes = "", response = SimpleResponse.class)
  @CrossOrigin(origins = "*")
  @PostMapping("/page")
//  @RequestBodyNeedDecrypt
  public SimpleResponse queryFile(@RequestBody PageView pageView, HttpServletRequest request) {
    System.out.println("Controller pageView="+pageView);
    LocalDateTime now = LocalDateTime.now();
    pageView.setEpoch(Timestamp.valueOf(now));
    pageView.setUploadTime(TimeUtil.getDateTimeStr(now));

    if(StringUtils.isEmpty(pageView.getClientIp())){
      pageView.setClientIp(request.getRemoteAddr());
    }
    if(StringUtils.isEmpty(pageView.getUserAgent())){
      pageView.setClientIp(request.getHeader("User-Agent"));
    }

    pageView.setServerIp(request.getHeader("Host"));

    pageView.setDevice(request.getHeader("User-Agent"));

    pageView.setDomain(request.getHeader("x-original"));

    collectorService.save(pageView);
    collectorService.sendMessage(pageTopic, pageView);
    return new SimpleResponse(0);
  }

  /**
   * 移动端上报的用户浏览记录接口
   * @param view
   * @param request
   * @return
   */
  @ApiOperation(value = "移动端上报用户浏览记录接口（要对请求body做base64加密）", notes = "", response = SimpleResponse.class)
  @CrossOrigin(origins = "*")
  @PostMapping("/mobile_page")
//  @RequestBodyNeedDecrypt
  public SimpleResponse uploadMobilePageView(@RequestBody MobilePageView view, HttpServletRequest request) {
    System.out.println("Controller MobilePageView="+view);
    LocalDateTime now = LocalDateTime.now();
    view.setEpoch(Timestamp.valueOf(now));
    view.setUploadTime(TimeUtil.getDateTimeStr(now));

    if(StringUtils.isEmpty(view.getClientIp())){
      view.setClientIp(request.getRemoteAddr());
    }

    view.setServerIp(request.getHeader("Host"));

    view.setDevice(request.getHeader("User-Agent"));

    view.setDomain(request.getHeader("x-original"));

    List<PageView> views = collectorService.extractPageView(view);

    for(PageView v: views){
      collectorService.save(v);
      collectorService.sendMessage(pageTopic, v);
    }

    return new SimpleResponse(0);
  }

  @ApiOperation(value = "web端上报用户操作记录接口（要对请求body做base64加密）", notes = "", response = SimpleResponse.class)
  @CrossOrigin(origins = "*")
  @PostMapping("/option")
//  @RequestBodyNeedDecrypt
  public SimpleResponse uploadOption(@RequestBody Option option, HttpServletRequest request) {
    System.out.println("Controller option="+option);
    LocalDateTime now = LocalDateTime.now();
    option.setEpoch(Timestamp.valueOf(now));
    option.setUploadTime(TimeUtil.getDateTimeStr(now));

    if(StringUtils.isEmpty(option.getClientIp())){
      option.setClientIp(request.getRemoteAddr());
    }

    collectorService.save(option);
    collectorService.sendMessage(optionTopic, option);
    return new SimpleResponse(0);
  }

  /**
   * 移动端上报的操作记录接口
   * @param option
   * @param request
   * @return
   */
  @ApiOperation(value = "移动端上报用户操作记录接口（要对请求body做base64加密）", notes = "", response = SimpleResponse.class)
  @CrossOrigin(origins = "*")
  @PostMapping("/mobile_option")
//  @RequestBodyNeedDecrypt
  public SimpleResponse uploadMobileOption(@RequestBody MobileOption option, HttpServletRequest request) {
    System.out.println("Controller option="+option);
    LocalDateTime now = LocalDateTime.now();
    option.setEpoch(Timestamp.valueOf(now));
    option.setUploadTime(TimeUtil.getDateTimeStr(now));

    if(StringUtils.isEmpty(option.getClientIp())){
      option.setClientIp(request.getRemoteAddr());
    }

    List<Option> options = collectorService.extractOption(option);

    for(Option o: options){
      collectorService.save(o);
      collectorService.sendMessage(optionTopic, o);
    }

    return new SimpleResponse(0);
  }

  @GetMapping("/peek")
  public String peek() {
    return collectorService.peekPage();
  }

  @GetMapping("/peekOption")
  public String peekOption() {
    return collectorService.peekOption();
  }


}
