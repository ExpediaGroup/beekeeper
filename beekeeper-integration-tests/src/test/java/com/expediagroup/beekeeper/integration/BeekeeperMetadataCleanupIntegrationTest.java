package com.expediagroup.beekeeper.integration;

public class BeekeeperMetadataCleanupIntegrationTest {

  // TODO
  // add some metadata to the HousekeepingMetadata table

  // setup
  // need some kind of hive object can test with

  // utils
  // need to have some hive test utils
  // will have a hive metastore
  // like circus train
  // // create unpartitioned table
  // // create partitioned table

  // will also need to make use of the amazon s3 stuff
  // set up some paths
  // make sure theyre deleted

  // use the integration test base
  // insertExpiredMetadata

  // TODO - tests
  // test the cleanup for partitioned tables
  // test the cleanup for unpartitioned tables
  // // isnt deleted if there are existing partitions
  // // is deleted when there are no existing partitions
  // test the cleanup for a partition on a table
  // check that paths are still deleted if the table doesnt exist
  // check that paths are still deleted if the partition doesnt exist
}
