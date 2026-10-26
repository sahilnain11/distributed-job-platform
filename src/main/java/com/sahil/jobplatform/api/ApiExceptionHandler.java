package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.TransactionException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

@RestControllerAdvice
class ApiExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<JsonNode> api(ApiException exception) {
        return ResponseEntity.status(exception.getStatus())
                .body(ApiJson.error(exception.getCode(), exception.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<JsonNode> malformed() {
        return ResponseEntity.badRequest().body(ApiJson.error("INVALID_JSON",
                "Body must contain one valid JSON value without duplicate fields"));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<JsonNode> badParameter() {
        return ResponseEntity.badRequest().body(ApiJson.error("INVALID_PARAMETER",
                "Invalid job UUID, status, or limit"));
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    ResponseEntity<JsonNode> mediaType() {
        return ResponseEntity.status(415).body(ApiJson.error("UNSUPPORTED_MEDIA_TYPE", "Use application/json"));
    }

    @ExceptionHandler(HttpMediaTypeNotAcceptableException.class)
    ResponseEntity<JsonNode> acceptType() {
        return ResponseEntity.status(406).contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .body(ApiJson.error("NOT_ACCEPTABLE", "Responses use application/json"));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<JsonNode> missingRoute() {
        return ResponseEntity.status(404).body(ApiJson.error("NOT_FOUND", "Endpoint does not exist"));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    ResponseEntity<JsonNode> method(HttpRequestMethodNotSupportedException exception) {
        var response = ResponseEntity.status(405);
        var supported = exception.getSupportedHttpMethods();
        if (supported != null) {
            response.allow(supported.toArray(org.springframework.http.HttpMethod[]::new));
        }
        return response.body(ApiJson.error("METHOD_NOT_ALLOWED", "Method is not supported on this endpoint"));
    }

    @ExceptionHandler({DataAccessException.class, TransactionException.class, PersistenceException.class})
    ResponseEntity<JsonNode> database(RuntimeException exception) {
        // Never expose SQL, connection strings, or request content to clients/logs.
        LOG.warn("Job API database operation failed: {}", exception.getClass().getSimpleName());
        return ResponseEntity.status(503).body(ApiJson.error("STORAGE_UNAVAILABLE",
                "Cannot confirm acceptance; retry the same request with the same key"));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<JsonNode> unexpected(Exception exception) {
        LOG.error("Unexpected job API error: {}", exception.getClass().getSimpleName());
        return ResponseEntity.internalServerError().body(ApiJson.error("INTERNAL_ERROR", "Request could not be processed"));
    }
}