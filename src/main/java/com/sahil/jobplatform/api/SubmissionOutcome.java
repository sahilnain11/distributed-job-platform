package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;

public record SubmissionOutcome(int status, JsonNode body) {
    public SubmissionOutcome {
        body = body.deepCopy();
    }

    @Override
    public JsonNode body() {
        return body.deepCopy();
    }
}