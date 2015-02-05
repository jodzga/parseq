package com.linkedin.parseq.stream;

import com.linkedin.parseq.task.Task;

public interface PublisherTask<T> extends Task<PublisherTask<T>>, Publisher<T> {
}
