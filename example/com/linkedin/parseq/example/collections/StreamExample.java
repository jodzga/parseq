/* $Id$ */
package com.linkedin.parseq.example.collections;

import static com.linkedin.parseq.example.common.ExampleUtil.fetchUrl;

import java.util.Arrays;
import java.util.List;

import com.linkedin.parseq.collection.Collections;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;
import com.linkedin.parseq.stream.Subscriber;
import com.linkedin.parseq.stream.Subscription;
import com.linkedin.parseq.task.Task;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class StreamExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new StreamExample().runExample();
  }

  static final List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com");
  static final List<String> paths = Arrays.asList("/p1", "/p2");

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();

    Task<?> task = Collections.fromValues(urls)
      .flatMap(base -> Collections.fromValues(paths)
          .map(path -> base + path)
          .mapTask(url -> fetchUrl(httpClient, url)))
          .subscribe(new Subscriber<String>() {
            
            @Override
            public void onSubscribe(Subscription subscription) {
            }
            
            @Override
            public void onNext(String element) {
              System.out.println(element);
            }
            
            @Override
            public void onError(Throwable cause) {
            }
            
            @Override
            public void onComplete() {
            }
          });
          

    engine.run(task);

    task.await();

    ExampleUtil.printTracingResults(task);
  }
}
