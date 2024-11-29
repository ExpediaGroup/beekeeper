package com.expediagroup.beekeeper.vacuum.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;
import com.expediagroup.beekeeper.core.repository.BeekeeperHistoryRepository;

@Repository
public interface BeekeeperEventsHistoryRepository extends BeekeeperHistoryRepository {

  @Query(value = "from BeekeeperHistory t where t.lifecycleType = :lifecycle")
  Slice<BeekeeperHistory> findRecordsByLifecycleType(
      @Param("lifecycle") String lifecycle,
      Pageable pageable);
}
