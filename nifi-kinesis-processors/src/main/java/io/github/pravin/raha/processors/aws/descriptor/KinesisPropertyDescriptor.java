package io.github.pravin.raha.processors.aws.descriptor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisPropertyDescriptor {

    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Application Name")
            .name("amazon-kinesis-stream-application-name")
            .description("The stream reader application name.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true).build();

    public static final PropertyDescriptor INITIAL_STREAM_POSITION = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Initial Stream Position")
            .name("amazon-kinesis-stream-initial-position")
            .description("Initial position to read streams.")
            .allowableValues(InitialPositionInStream.LATEST.toString(),
                    InitialPositionInStream.TRIM_HORIZON.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(InitialPositionInStream.LATEST.toString())
            .required(true).build();

    public static final PropertyDescriptor DYNAMODB_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis DynamoDB Override")
            .name("amazon-kinesis-stream-dynamodb-override")
            .description("DynamoDB override to use non AWS deployments")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false).build();

    public static final PropertyDescriptor DISABLE_CLOUDWATCH = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Cloudwatch Flag")
            .name("amazon-kinesis-stream-cloudwatch-flag")
            .description("Disable kinesis logging to cloud watch.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .required(false).build();

    public static final PropertyDescriptor KINESIS_STREAM_NAME = new PropertyDescriptor
            .Builder().name("KINESIS_STREAM_NAME")
            .displayName("Stream Name")
            .description("Kinesis stream name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

}
