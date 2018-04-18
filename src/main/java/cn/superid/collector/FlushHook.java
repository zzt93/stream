package cn.superid.collector;

/**
 * @author zzt
 */
public class FlushHook implements Runnable {

  private final CollectorController controller;

  FlushHook(CollectorController controller) {
    this.controller = controller;
  }

  @Override
  public void run() {
    controller.flush();
  }
}
