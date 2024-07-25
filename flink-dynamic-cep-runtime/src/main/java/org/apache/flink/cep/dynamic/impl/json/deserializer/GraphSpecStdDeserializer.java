/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.dynamic.impl.json.deserializer;


import org.apache.flink.cep.dynamic.impl.json.spec.EdgeSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The customized StdDeserializer for GraphSpec.
 */
public class GraphSpecStdDeserializer extends StdDeserializer<GraphSpec> {

    public static final GraphSpecStdDeserializer INSTANCE = new GraphSpecStdDeserializer();
    private static final long serialVersionUID = 1L;

    public GraphSpecStdDeserializer() {
        this(null);
    }

    public GraphSpecStdDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public GraphSpec deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        List<NodeSpec> nodeSpecs = new ArrayList<>();
        Iterator<JsonNode> embeddedElementNames = node.get("nodes").elements();
        while (embeddedElementNames.hasNext()) {
            JsonNode jsonNode = embeddedElementNames.next();
            NodeSpec embedNode = jsonParser.getCodec().treeToValue(jsonNode, NodeSpec.class);
            nodeSpecs.add(embedNode);
        }

        List<EdgeSpec> edgeSpecs = new ArrayList<>();
        Iterator<JsonNode> jsonNodeIterator = node.get("edges").elements();
        while (jsonNodeIterator.hasNext()) {
            JsonNode jsonNode = jsonNodeIterator.next();
            EdgeSpec embedNode = jsonParser.getCodec().treeToValue(jsonNode, EdgeSpec.class);
            edgeSpecs.add(embedNode);
        }

        return new GraphSpec(nodeSpecs, edgeSpecs);

    }
}
