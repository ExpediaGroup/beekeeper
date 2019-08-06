/**
 * Copyright (C) 2019 Expedia, Inc.
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
package com.expediagroup.beekeeper.integration;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class ContainerTestUtils {

  public static MySQLContainer mySqlContainer() {
    MySQLContainer container = new MySQLContainer("mysql:8.0.15").withDatabaseName("beekeeper");
    container.withCommand("--default-authentication-plugin=mysql_native_password");
    return container;
  }

  public static LocalStackContainer awsContainer(LocalStackContainer.Service service) {
    return new LocalStackContainer().withServices(service);
  }

  public static String awsServiceEndpoint(LocalStackContainer awsContainer, LocalStackContainer.Service service) {
    return awsContainer.getEndpointConfiguration(service)
        .getServiceEndpoint();
  }

  public static String queueUrl(LocalStackContainer awsContainer, String queue) {
    return awsServiceEndpoint(awsContainer, SQS) + "/queue/" + queue;
  }

  public static AmazonSQS sqsClient(LocalStackContainer awsContainer, String region) {
    EndpointConfiguration endpointConfiguration = new EndpointConfiguration(awsServiceEndpoint(awsContainer, SQS),
        region);
    return AmazonSQSClientBuilder.standard()
        .withEndpointConfiguration(endpointConfiguration)
        .build();
  }

  public static AmazonS3 s3Client(LocalStackContainer awsContainer, String region) {
    EndpointConfiguration endpointConfiguration = new EndpointConfiguration(awsServiceEndpoint(awsContainer, S3),
        region);

    // build with disableChunkedEncoding to be able to create empty files
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(endpointConfiguration)
        .disableChunkedEncoding()
        .build();
  }
}
