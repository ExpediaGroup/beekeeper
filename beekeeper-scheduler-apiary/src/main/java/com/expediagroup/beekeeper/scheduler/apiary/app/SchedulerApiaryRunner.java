/**
 * Copyright (C) 2019-2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.beekeeper.scheduler.apiary.app;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.scheduler.apiary.service.SchedulerApiary;

@Component
public class SchedulerApiaryRunner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(SchedulerApiaryRunner.class);
  private static final long RUNNER_DESTROY_TIMEOUT_SECONDS = 11L;

  private final ReentrantLock lock;
  private final SchedulerApiary schedulerApiary;

  private final AtomicBoolean running = new AtomicBoolean(false);

  @Autowired
  public SchedulerApiaryRunner(SchedulerApiary schedulerApiary) {
    this.schedulerApiary = schedulerApiary;
    lock = new ReentrantLock();
  }

  @Override
  public void run(ApplicationArguments args) {
    lock.lock();
    running.set(true);
    log.info("Starting application runner");
    while (running.get()) {
      try {
        schedulerApiary.scheduleBeekeeperEvent();
      } catch (Exception e) {
        log.error("Error while scheduling path", e);
      }
    }
    log.info("Runner has stopped");
    lock.unlock();
  }

  @PreDestroy
  public void destroy() {
    try {
      log.info("Shutting down runner");
      running.set(false);
      if (!lock.tryLock(RUNNER_DESTROY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new BeekeeperException("Runner taking too long to shut down");
      }
    } catch (InterruptedException e) {
      throw new BeekeeperException("Runner taking too long to shut down", e);
    } finally {
      try {
        schedulerApiary.close();
      } catch (IOException e) {
        throw new BeekeeperException("Problem closing resources", e);
      }
    }
    log.info("Runner is shutdown");
  }
}
