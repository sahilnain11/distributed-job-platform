package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

@Component
class SubmissionValidator {

    void validate(JsonNode request) {
        ApiJson.require(request.isObject(), "INVALID_JOB", "Each job must be a JSON object");
        ApiJson.require(ApiJson.onlyFields(request, "type", "payload"),
                "INVALID_JOB", "Only type and payload are allowed on a job request");
        JsonNode type = request.path("type");
        ApiJson.require(type.isTextual() && !type.textValue().isBlank() && type.textValue().length() <= 255,
                "INVALID_TYPE", "type must be a nonblank string of at most 255 characters");
        ApiJson.require(type.textValue().equals("ECHO") || type.textValue().equals("SIMULATED_WORK"),
                "UNSUPPORTED_JOB_TYPE", "Supported job types are ECHO and SIMULATED_WORK");
        JsonNode payload = request.path("payload");
        ApiJson.require(!payload.isMissingNode() && !payload.isNull(), "INVALID_PAYLOAD", "payload is required");
        if (type.textValue().equals("SIMULATED_WORK")) {
            ApiJson.require(payload.isObject() && ApiJson.onlyFields(payload, "durationMillis", "failFirstAttempts"),
                    "INVALID_PAYLOAD", "SIMULATED_WORK requires durationMillis and failFirstAttempts only");
            requireNonnegativeInteger(payload.path("durationMillis"), "durationMillis", true);
            requireNonnegativeInteger(payload.path("failFirstAttempts"), "failFirstAttempts", false);
        }
    }

    private void requireNonnegativeInteger(JsonNode value, String name, boolean duration) {
        var max = java.math.BigDecimal.valueOf(duration ? Long.MAX_VALUE : Integer.MAX_VALUE);
        ApiJson.require(value.isNumber() && value.decimalValue().signum() >= 0
                        && value.decimalValue().stripTrailingZeros().scale() <= 0
                        && value.decimalValue().compareTo(max) <= 0,
                "INVALID_PAYLOAD", name + " must be a nonnegative " + (duration ? "64-bit" : "32-bit") + " integer");
    }
}