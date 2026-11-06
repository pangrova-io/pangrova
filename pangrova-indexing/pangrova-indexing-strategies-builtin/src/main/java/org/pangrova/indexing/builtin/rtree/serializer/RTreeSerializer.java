package org.pangrova.indexing.builtin.rtree.serializer;

import org.pangrova.indexing.api.data.spatial.BoundingBox;
import org.pangrova.indexing.api.data.spatial.RTreeIndexData;
import org.pangrova.indexing.api.data.spatial.RTreeNode;

import java.util.ArrayList;
import org.pangrova.indexing.codec.data.SerdeCodec;
import org.pangrova.indexing.spi.codec.TypeSerializer;

/**
 * Serializer for {@link RTreeIndexData}.
 * Format: [leafCount:4B] [nodeCount:4B] {node}*
 * Node: [leaf:1B] [dims:4B] [minValues:8B*dims] [maxValues:8B*dims] [childIndices:intList]
 */
public final class RTreeSerializer implements TypeSerializer<RTreeIndexData> {

    @Override
    public byte[] serialize(RTreeIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeInt(data.leafCount());
            out.writeInt(data.nodes().size());
            for (RTreeNode node : data.nodes()) {
                out.writeBoolean(node.leaf());
                BoundingBox bb = node.boundingBox();
                out.writeInt(bb.dimensions());
                for (int d = 0; d < bb.dimensions(); d++) out.writeDouble(bb.minValue(d));
                for (int d = 0; d < bb.dimensions(); d++) out.writeDouble(bb.maxValue(d));
                SerdeCodec.writeIntList(out, node.childIndices());
            }
        });
    }

    @Override
    public RTreeIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            int leafCount = in.readInt();
            int nodeCount = in.readInt();
            var nodes = new ArrayList<RTreeNode>(nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                boolean leaf = in.readBoolean();
                int dims = in.readInt();
                double[] mins = new double[dims];
                double[] maxs = new double[dims];
                for (int d = 0; d < dims; d++) mins[d] = in.readDouble();
                for (int d = 0; d < dims; d++) maxs[d] = in.readDouble();
                var childIndices = SerdeCodec.readIntList(in);
                nodes.add(new RTreeNode(new BoundingBox(mins, maxs), childIndices, leaf));
            }
            return new RTreeIndexData(nodes, leafCount);
        });
    }
}
