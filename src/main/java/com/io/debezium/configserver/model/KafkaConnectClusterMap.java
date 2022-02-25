package com.io.debezium.configserver.model;///*
// * Copyright Debezium Authors.
// *
// * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
// */
//package io.debezium.configserver.model;
//
//import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
//import org.eclipse.microprofile.openapi.annotations.media.Schema;
//
//import java.net.URI;
//import java.util.HashMap;
//import java.util.Map;
//
//@Schema(implementation = Map.class, type = SchemaType.OBJECT, example = "<1,\"http://localhost:1234\">")
//public class KafkaConnectClusterMap extends HashMap<Integer, URI> {
//
//    public KafkaConnectClusterMap(int initialCapacity) {
//        super(initialCapacity);
//    }
//
//    public KafkaConnectClusterMap() {
//        super();
//    }
//
//    public void put(URI uri) {
//        put(nextId(), uri);
//    }
//
//    private int nextId() {
//        return this.size() + 1;
//    }
//}
