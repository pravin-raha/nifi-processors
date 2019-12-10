/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.pravin.raha.processors.aws.kinesis;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.github.pravin.raha.processors.aws.descriptor.CredentialPropertyDescriptors.*;
import static io.github.pravin.raha.processors.aws.descriptor.KinesisPropertyDescriptor.*;

@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"amazon", "aws", "kinesis", "get", "stream"})
@CapabilityDescription("Reads data from the specified AWS Kinesis stream. " +
        "Uses DynamoDB for check pointing and CloudWatch (optional) for metrics. " +
        "Ensure that the credentials provided have access to DynamoDB and CloudWatch (if used) along with Kinesis.")
public class GetKinesisProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to success relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to failure relationship").build();

    public static final List<PropertyDescriptor> descriptors = Collections.unmodifiableList(
            Arrays.asList(KINESIS_STREAM_NAME, REGION, ACCESS_KEY, SECRET_KEY,
                    APPLICATION_NAME, INITIAL_STREAM_POSITION, DYNAMODB_ENDPOINT_OVERRIDE, DISABLE_CLOUDWATCH));
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    private static final Logger LOGGER = LoggerFactory.getLogger(GetKinesisProcessor.class);
    private Scheduler scheduler;

    @Override
    protected void init(final ProcessorInitializationContext context) {
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (scheduler == null) {
            scheduler = initializeScheduler(context, session);
            scheduler.run();
        }
    }

    @OnStopped
    public void onStopped() {
        try {
            LOGGER.info("OnStopped invoke");
            final Future<Boolean> shutdownCallback = scheduler.startGracefulShutdown();
            waitForShutdownToComplete(shutdownCallback);

            if (shutdownCallback.get()) {
                LOGGER.info("Successfully shutdown KCL");
            } else {
                LOGGER.error("Some error occurred during shutdown of KCL. Logs may have more details.");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Shutdown interrupted", e);
        } catch (ExecutionException e) {
            LOGGER.error("Error during KCL processing", e);
        }
    }

    private void waitForShutdownToComplete(final Future<Boolean> shutdownCallback) throws InterruptedException {
        while (!shutdownCallback.isDone()) {
            Thread.sleep(3_000);
        }
    }

    private Scheduler initializeScheduler(final ProcessContext context, final ProcessSession session) {
        final Region region = Region.of(context.getProperty(REGION).evaluateAttributeExpressions().getValue());
        final AwsCredentials awsCredentials = AwsBasicCredentials
                .create(context.getProperty(ACCESS_KEY).evaluateAttributeExpressions().getValue()
                        , context.getProperty(SECRET_KEY).evaluateAttributeExpressions().getValue());
        final AwsCredentialsProvider creds = StaticCredentialsProvider.create(awsCredentials);

        final KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient
                        .builder()
                        .credentialsProvider(creds)
                        .region(region));
        final DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient
                .builder()
                .region(region)
                .credentialsProvider(creds)
                .build();
        final CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient
                .builder()
                .region(region)
                .credentialsProvider(creds)
                .build();
        final ConfigsBuilder configsBuilder = new ConfigsBuilder(
                context.getProperty(KINESIS_STREAM_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(APPLICATION_NAME).evaluateAttributeExpressions().getValue(),
                kinesisClient,
                dynamoClient,
                cloudWatchClient, UUID.randomUUID().toString(), new KinesisRecordProcessorFactory(session));

        return new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(context.getProperty(KINESIS_STREAM_NAME).evaluateAttributeExpressions().getValue(), kinesisClient))
        );
    }
}
