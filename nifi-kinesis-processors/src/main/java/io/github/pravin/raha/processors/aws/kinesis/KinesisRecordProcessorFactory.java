package io.github.pravin.raha.processors.aws.kinesis;

import org.apache.nifi.processor.ProcessSession;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class KinesisRecordProcessorFactory implements ShardRecordProcessorFactory {
    private ProcessSession session;

    public KinesisRecordProcessorFactory(ProcessSession session) {
        this.session = session;
    }

    public ShardRecordProcessor shardRecordProcessor() {
        return new KinesisRecordProcessor(session);
    }
}