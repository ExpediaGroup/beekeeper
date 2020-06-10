package com.expediagroup.beekeeper.scheduler.service;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingTable;
import com.expediagroup.beekeeper.core.model.Housekeeping;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingTableRepository;

@Service
public class ExpiredTableSchedulerService implements SchedulerService {

  private final LifecycleEventType LIFECYCLE_EVENT_TYPE = EXPIRED;
  private final HousekeepingTableRepository housekeepingTableRepository;

  @Autowired
  public ExpiredTableSchedulerService(HousekeepingTableRepository housekeepingTableRepository) {
    this.housekeepingTableRepository = housekeepingTableRepository;
  }

  @Override
  public LifecycleEventType getLifecycleEventType() {
    return LIFECYCLE_EVENT_TYPE;
  }

  @Override
  @TimedTaggable("tables-scheduled")
  public void scheduleForHousekeeping(Housekeeping housekeepingEntity) {
    EntityHousekeepingTable housekeepingTable = (EntityHousekeepingTable) housekeepingEntity;
    try {
      housekeepingTableRepository.save(housekeepingTable);
    } catch (Exception e) {
      throw new BeekeeperException(
          format("Unable to schedule table '%s.%s' for deletion", housekeepingTable.getDatabaseName(),
              housekeepingTable.getTableName()), e);
    }
  }
}
