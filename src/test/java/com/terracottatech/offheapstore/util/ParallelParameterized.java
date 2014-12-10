package com.terracottatech.offheapstore.util;

import org.junit.runner.Runner;
import org.junit.runners.Parameterized;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.RunnerScheduler;

public class ParallelParameterized extends Parameterized {

  public ParallelParameterized(Class<?> klass) throws Throwable {
    super(klass);
  }

  @Override
  public void setScheduler(RunnerScheduler scheduler) {
    for (Runner child : getChildren()) {
      if (child instanceof ParentRunner<?>) {
        ((ParentRunner<?>) child).setScheduler(scheduler);
      }
    }
  }
  
}
