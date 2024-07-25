package org.apache.flink.cep.dynamic.impl.json.spec;

import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class GraphSpec {
    private final List<NodeSpec> nodes;
    private final List<EdgeSpec> edges;

    public GraphSpec(
            @JsonProperty("nodes") List<NodeSpec> nodes,
            @JsonProperty("edges") List<EdgeSpec> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }

    public static GraphSpec fromPattern(Pattern<?, ?> pattern) {
        // Build nested pattern sequence
        List<NodeSpec> nodes = new ArrayList<>();
        List<EdgeSpec> edges = new ArrayList<>();
        while (pattern != null) {
            if (pattern instanceof GroupPattern) {
                // Process sub graph recursively
                GroupNodeSpec subgraphSpec =
                        GroupNodeSpec.fromPattern(pattern);
                nodes.add(subgraphSpec);
            } else {
                // Build nodeSpec
                NodeSpec nodeSpec = NodeSpec.fromPattern(pattern);
                nodes.add(nodeSpec);
            }
            if (pattern.getPrevious() != null) {
                edges.add(
                        new EdgeSpec(
                                pattern.getPrevious().getName(),
                                pattern.getName(),
                                pattern.getQuantifier().getConsumingStrategy()));
            }
            pattern = pattern.getPrevious();
        }
        return new GraphSpec(nodes, edges);
    }

    public Pattern<?, ?> toPattern(final ClassLoader classLoader, final Configuration globalConfiguration) throws Exception {
        // Construct cache of nodes and edges for later use
        final Map<String, NodeSpec> nodeCache = new HashMap<>();
        for (NodeSpec node : nodes) {
            nodeCache.put(node.getName(), node);
        }
        final Map<String, EdgeSpec> edgeCache = new HashMap<>();
        for (EdgeSpec edgeSpec : edges) {
            edgeCache.put(edgeSpec.getSource(), edgeSpec);
        }

        // Build pattern sequence
        String currentNodeName = findBeginPatternName();
        Pattern<?, ?> prevPattern = null;
        String prevNodeName = null;
        while (currentNodeName != null) {
            NodeSpec currentNodeSpec = nodeCache.get(currentNodeName);
            EdgeSpec edgeToCurrentNode = edgeCache.get(prevNodeName);
            // Build the atomic pattern
            // Note, the afterMatchStrategy of neighboring patterns should adopt the GraphSpec's
            // afterMatchStrategy
            prevPattern =
                    currentNodeSpec.toPattern(
                            prevPattern,
                            prevNodeName == null
                                    ? Quantifier.ConsumingStrategy.STRICT
                                    : edgeToCurrentNode.getType(),
                            classLoader,
                            globalConfiguration
                    );
            prevNodeName = currentNodeName;
            currentNodeName =
                    edgeCache.get(currentNodeName) == null
                            ? null
                            : edgeCache.get(currentNodeName).getTarget();
        }

        return prevPattern;
    }


    public String findBeginPatternName() {
        final Set<String> nodeSpecSet = new HashSet<>();
        for (NodeSpec node : nodes) {
            nodeSpecSet.add(node.getName());
        }
        for (EdgeSpec edgeSpec : edges) {
            nodeSpecSet.remove(edgeSpec.getTarget());
        }
        if (nodeSpecSet.size() != 1) {
            throw new IllegalStateException(
                    "There must be exactly one begin node, but there are "
                            + nodeSpecSet.size()
                            + " nodes that are not pointed by any other nodes.");
        }
        Iterator<String> iterator = nodeSpecSet.iterator();

        if (!iterator.hasNext()) {
            throw new RuntimeException("Could not find the begin node.");
        }

        return iterator.next();
    }

    public List<NodeSpec> getNodes() {
        return nodes;
    }

    public List<EdgeSpec> getEdges() {
        return edges;
    }
}
