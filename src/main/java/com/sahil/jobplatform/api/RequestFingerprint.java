package com.sahil.jobplatform.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.TreeSet;

@Component
public class RequestFingerprint {

    private final ObjectMapper mapper;

    public RequestFingerprint(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String hash(JsonNode request) {
        StringBuilder normalized = new StringBuilder();
        append(request, normalized);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(normalized.toString().getBytes(StandardCharsets.UTF_8));
            return "v1:" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private void append(JsonNode node, StringBuilder output) {
        if (node.isObject()) {
            output.append('{');
            TreeSet<String> names = new TreeSet<>();
            node.fieldNames().forEachRemaining(names::add);
            boolean first = true;
            for (String name : names) {
                if (!first) { output.append(','); }
                first = false;
                output.append(quote(name)).append(':');
                append(node.get(name), output);
            }
            output.append('}');
        } else if (node.isArray()) {
            output.append('[');
            for (int i = 0; i < node.size(); i++) {
                if (i != 0) { output.append(','); }
                append(node.get(i), output);
            }
            output.append(']');
        } else if (node.isNumber()) {
            // Avoid floating-point conversion and unbounded plain-string
            // expansion for large exponents. Equivalent numeric values match.
            output.append(node.decimalValue().stripTrailingZeros().toString());
        } else if (node.isTextual()) {
            output.append(quote(node.textValue()));
        } else if (node.isNull() || node.isBoolean()) {
            output.append(node.toString());
        } else {
            throw new ApiException(400, "INVALID_JSON", "Only JSON values are supported");
        }
    }

    private String quote(String value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot encode JSON text", exception);
        }
    }
}