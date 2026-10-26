package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.type.format.jackson.JacksonJsonFormatMapper;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class JsonConfiguration {

    @Bean
    Jackson2ObjectMapperBuilderCustomizer exactRequestJson() {
        return builder -> builder.featuresToEnable(
                DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS,
                DeserializationFeature.USE_BIG_INTEGER_FOR_INTS,
                DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY,
                DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    @Bean
    HibernatePropertiesCustomizer exactStoredJson(ObjectMapper mapper) {
        // Use the same precision-preserving parser for JSONB reloads, not a
        // separate Hibernate ObjectMapper that could round decimal values.
        return properties -> properties.put("hibernate.type.json_format_mapper",
                new JacksonJsonFormatMapper(mapper));
    }
}