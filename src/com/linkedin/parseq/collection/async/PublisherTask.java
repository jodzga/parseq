package com.linkedin.parseq.collection.async;

import com.linkedin.parseq.task.Task;

public interface PublisherTask<T> extends Task<PublisherTask<T>>, Publisher<T> {
}
