package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

final class ApiJson {

    private ApiJson() { }

    static ObjectNode error(String code, String message) {
        ObjectNode body = JsonNodeFactory.instance.objectNode();
        body.putObject("error").put("code", code).put("message", message);
        return body;
    }

    static ObjectNode accepted(java.util.UUID jobId) {
        return JsonNodeFactory.instance.objectNode()
                .put("jobId", jobId.toString()).put("status", "QUEUED");
    }

    static void require(boolean condition, String code, String message) {
        if (!condition) { throw new ApiException(400, code, message); }
    }

    static boolean onlyFields(JsonNode object, String... allowed) {
        java.util.Set<String> fields = java.util.Set.of(allowed);
        var names = object.fieldNames();
        while (names.hasNext()) {
            if (!fields.contains(names.next())) { return false; }
        }
        return true;
    }
}