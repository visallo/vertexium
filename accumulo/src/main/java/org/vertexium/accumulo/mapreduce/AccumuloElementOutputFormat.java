package org.vertexium.accumulo.mapreduce;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class AccumuloElementOutputFormat extends OutputFormat<Text, Mutation> {
    private AccumuloOutputFormat accumuloOutputFormat = new AccumuloOutputFormat();

    public static void setOutputInfo(Job job, String instanceName, String zooKeepers, String principal, AuthenticationToken token) throws AccumuloSecurityException {
        AccumuloOutputFormat.setConnectorInfo(job, principal, token);
        ClientConfiguration clientConfig = ClientConfiguration.create()
            .withInstance(instanceName)
            .withZkHosts(zooKeepers);
        AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);
    }

    @Override
    public RecordWriter<Text, Mutation> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return accumuloOutputFormat.getRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        accumuloOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return accumuloOutputFormat.getOutputCommitter(context);
    }
}
