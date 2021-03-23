package com.expediagroup.beekeeper.api;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    classes = {WebConfig.class},
    loader = AnnotationConfigContextLoader.class)
public class SpringContextIntegrationTest {

  @Test
  public void contextLoads(){
    // When
  }
}
