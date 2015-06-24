package org.vertexium.titan.hadoop.accumulo;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurers;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class AccumuloVertexiumJobClasspathConfigurer implements JobClasspathConfigurer {
    private static final String JOB_JAR = "titan-hadoop-2-" + TitanConstants.VERSION + "-job.jar";
    private static final String MAPRED_JAR = "mapred.jar";
    private JobClasspathConfigurer defaultJobClasspathConfigurer;

    @Override
    public void configure(Job job) throws IOException {
        if (defaultJobClasspathConfigurer == null) {
            defaultJobClasspathConfigurer = JobClasspathConfigurers.get(null, job.getConfiguration().get(MAPRED_JAR), JOB_JAR);
        }
        AccumuloVertexiumInputFormat.configure(job);
        defaultJobClasspathConfigurer.configure(job);
    }
}
