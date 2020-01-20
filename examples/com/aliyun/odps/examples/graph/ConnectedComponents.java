package com.aliyun.odps.examples.graph;

import java.io.IOException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.*;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.WritableRecord;
/**
 * Compute the connected component membership of each vertex and output
 * each vertex which's value containing the smallest id in the connected
 * component containing that vertex.
 *
 * Algorithm: propagate the smallest vertex id along the edges to all
 * vertices of a connected component.
 *
 */
public class ConnectedComponents {
    public static class CCVertex extends
            Vertex<LongWritable, LongWritable, NullWritable, LongWritable> {
        @Override
        public void compute(
                ComputeContext<LongWritable, LongWritable, NullWritable, LongWritable> context,
                Iterable<LongWritable> msgs) throws IOException {
            if (context.getSuperstep() == 0L) {
                this.setValue(getId());
                context.sendMessageToNeighbors(this, getValue());
                return;
            }
            long minID = Long.MAX_VALUE;
            for (LongWritable id : msgs) {
                if (id.get() < minID) {
                    minID = id.get();
                }
            }
            if (minID < this.getValue().get()) {
                this.setValue(new LongWritable(minID));
                context.sendMessageToNeighbors(this, getValue());
            } else {
                this.voteToHalt();
            }
        }
        /**
         * Output Table Description:
         * +-----------------+----------------------------------------+
         * | Field | Type    | Comment                                |
         * +-----------------+----------------------------------------+
         * | v     | bigint  | vertex id                              |
         * | minID | bigint  | smallest id in the connected component |
         * +-----------------+----------------------------------------+
         */
        @Override
        public void cleanup(
                WorkerContext<LongWritable, LongWritable, NullWritable, LongWritable> context)
                throws IOException {
            context.write(getId(), getValue());
        }
    }
    /**
     * Input Table Description:
     * +-----------------+----------------------------------------------------+
     * | Field | Type    | Comment                                            |
     * +-----------------+----------------------------------------------------+
     * | v     | bigint  | vertex id                                          |
     * | es    | string  | comma separated target vertex id of outgoing edges |
     * +-----------------+----------------------------------------------------+
     *
     * Example:
     * For graph:
     *       1 ----- 2
     *       |       |
     *       3 ----- 4
     * Input table:
     * +-----------+
     * | v  | es   |
     * +-----------+
     * | 1  | 2,3  |
     * | 2  | 1,4  |
     * | 3  | 1,4  |
     * | 4  | 2,3  |
     * +-----------+
     */
    public static class CCVertexReader extends
            GraphLoader<LongWritable, LongWritable, NullWritable, LongWritable> {
        @Override
        public void load(
                LongWritable recordNum,
                WritableRecord record,
                MutationContext<LongWritable, LongWritable, NullWritable, LongWritable> context)
                throws IOException {
            CCVertex vertex = new CCVertex();
            vertex.setId((LongWritable) record.get(0));
            String[] edges = record.get(1).toString().split(",");
            for (int i = 0; i < edges.length; i++) {
                long destID = Long.parseLong(edges[i]);
                vertex.addEdge(new LongWritable(destID), NullWritable.get());
            }
            context.addVertexRequest(vertex);
        }
    }
    //Combiner的setCombinerClass设置Combiner。
    public static class MinLongCombiner extends Combiner<LongWritable, LongWritable> {

        @Override
        public void combine(LongWritable vertexId, LongWritable combinedMessage,
                            LongWritable messageToCombine) throws IOException {
            if (combinedMessage.get() > messageToCombine.get()) {
                combinedMessage.set(messageToCombine.get());
            }
        }

    }
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: <input> <output>");
            System.exit(-1);
        }
        GraphJob job = new GraphJob();
        job.setGraphLoaderClass(CCVertexReader.class);
        job.setVertexClass(CCVertex.class);
        job.setCombinerClass(MinLongCombiner.class);
        job.addInput(TableInfo.builder().tableName(args[0]).build());
        job.addOutput(TableInfo.builder().tableName(args[1]).build());
        long startTime = System.currentTimeMillis();
        job.run();
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
}