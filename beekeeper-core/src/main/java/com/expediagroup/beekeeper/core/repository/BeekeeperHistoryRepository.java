package com.expediagroup.beekeeper.core.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;

public interface BeekeeperHistoryRepository extends PagingAndSortingRepository<BeekeeperHistory, Long>,
    JpaSpecificationExecutor<BeekeeperHistory> {

  @Query(value = "from BeekeeperHistory t where t.lifecycleType = :lifecycle")
  Slice<BeekeeperHistory> findRecordsByLifecycleType(
      @Param("lifecycle") String lifecycle,
      Pageable pageable);
}
