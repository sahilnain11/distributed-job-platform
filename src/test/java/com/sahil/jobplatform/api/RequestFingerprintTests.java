package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class RequestFingerprintTests {

    private final ObjectMapper mapper = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
    private final RequestFingerprint fingerprints = new RequestFingerprint(mapper);

    @Test
    void recursivelySortsFieldsAndNormalizesEquivalentNumericValuesWithoutLosingPrecision() throws Exception {
        assertThat(hash("{\"b\":{\"z\":1.00,\"a\":2},\"a\":[1,2]}"))
                .isEqualTo(hash("{ \"a\": [1,2], \"b\": {\"a\":2.0,\"z\":1e0} }"));
        assertThat(hash("{\"value\":-0.00}")).isEqualTo(hash("{\"value\":0}"));
        assertThat(hash("{\"value\":1e100000}")).isEqualTo(hash("{\"value\":10e99999}"));
        assertThat(hash("{\"n\":123456789012345678901234567890}"))
                .isNotEqualTo(hash("{\"n\":123456789012345678901234567891}"));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "{}|{\"a\":null}",
            "{\"v\":\"123\"}|{\"v\":123}",
            "{\"v\":\" x\"}|{\"v\":\"x\"}",
            "{\"v\":\"X\"}|{\"v\":\"x\"}",
            "[1,2]|[2,1]",
            "{\"n\":0.123456789012345678901}|{\"n\":0.123456789012345678902}",
            "{\"items\":[{},5]}|{\"items\":[{},6]}"
    })
    void preservesMeaningfulInputDifferences(String left, String right) throws Exception {
        assertThat(hash(left)).isNotEqualTo(hash(right));
    }

    @Test
    void fingerprintHasVersionedSha256Format() throws Exception {
        assertThat(hash("{}")).matches("v1:[0-9a-f]{64}");
    }

    private String hash(String json) throws Exception {
        return fingerprints.hash(mapper.readTree(json));
    }
}