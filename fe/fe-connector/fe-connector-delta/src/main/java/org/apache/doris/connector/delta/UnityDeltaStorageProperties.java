// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.delta;

import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Converts UC Delta credentials into the properties consumed by Doris BE readers. */
final class UnityDeltaStorageProperties {
    private static final String S3_ENDPOINT = "AWS_ENDPOINT";
    private static final String S3_REGION = "AWS_REGION";
    private static final String S3_ACCESS_KEY = "AWS_ACCESS_KEY";
    private static final String S3_SECRET_KEY = "AWS_SECRET_KEY";
    private static final String S3_TOKEN = "AWS_TOKEN";
    private static final String STORAGE_PROVIDER = "provider";
    private static final String AZURE_PROVIDER = "AZURE";

    private UnityDeltaStorageProperties() {
    }

    static Map<String, String> toBackendProperties(String location,
            DeltaCredentialsResponse response, Map<String, String> catalogProperties) {
        return toBackendProperties(location, response, catalogProperties, null);
    }

    static Map<String, String> toBackendProperties(String location,
            DeltaCredentialsResponse response, Map<String, String> catalogProperties,
            DeltaCredentialOperation expectedOperation) {
        URI locationUri = URI.create(location);
        if ("file".equalsIgnoreCase(locationUri.getScheme())) {
            return Map.of();
        }

        DeltaStorageCredential credential = DeltaStorageCredentialUtil.selectForLocation(
                location, response.getStorageCredentials());
        if (expectedOperation != null && credential.getOperation() != expectedOperation) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a " + credential.getOperation()
                            + " credential for a " + expectedOperation + " operation");
        }
        if (credential.getExpirationTimeMs() != null
                && credential.getExpirationTimeMs() <= System.currentTimeMillis()) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned an expired storage credential for "
                            + credential.getPrefix());
        }
        TemporaryCredentials temporary =
                DeltaStorageCredentialUtil.toTemporaryCredentials(credential);
        if (temporary.getAwsTempCredentials() != null) {
            return awsProperties(temporary.getAwsTempCredentials(), catalogProperties);
        }
        if (temporary.getAzureUserDelegationSas() != null) {
            String accountHost = locationUri.getHost();
            if (accountHost == null || accountHost.isEmpty()) {
                throw new IllegalArgumentException(
                        "Azure Delta location does not contain a storage account host");
            }
            String container = locationUri.getUserInfo();
            if (container == null || container.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Azure Delta location does not contain a storage container");
            }
            String sasToken = temporary.getAzureUserDelegationSas().getSasToken();
            if (sasToken == null || sasToken.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Unity Catalog returned an empty Azure SAS token for " + accountHost);
            }
            Map<String, String> properties = new LinkedHashMap<>();
            // FE's Kernel engine receives the Hadoop SAS configuration from the official
            // Unity Hadoop helper. BE native Parquet scans use the existing object-storage
            // client, so pass the same vended SAS through its provider/token contract.
            properties.put(STORAGE_PROVIDER, AZURE_PROVIDER);
            properties.put(S3_ENDPOINT, blobEndpoint(accountHost));
            properties.put(S3_REGION, "azure");
            properties.put(S3_TOKEN, sasToken);
            return properties;
        }
        throw new UnsupportedOperationException(
                "Unity Catalog returned a GCS OAuth credential, but Doris BE does not yet "
                        + "support OAuth-authenticated native GCS scans");
    }

    /** The BE Azure object client uses Blob APIs; map an ABFS DFS host to its Blob endpoint. */
    private static String blobEndpoint(String accountHost) {
        String lowerHost = accountHost.toLowerCase(Locale.ROOT);
        int dfsMarker = lowerHost.indexOf(".dfs.");
        if (dfsMarker < 0) {
            return accountHost;
        }
        return accountHost.substring(0, dfsMarker) + ".blob"
                + accountHost.substring(dfsMarker + 4);
    }

    static void configureS3HadoopProperties(
            org.apache.hadoop.conf.Configuration configuration,
            Map<String, String> catalogProperties) {
        String region = firstNonBlank(catalogProperties,
                S3_REGION, "s3.region", "client.region", "aws.region");
        if (region == null) {
            return;
        }
        String endpoint = firstNonBlank(catalogProperties,
                S3_ENDPOINT, "s3.endpoint", "aws.endpoint");
        if (endpoint == null) {
            endpoint = "s3." + region + ".amazonaws.com";
        }
        configuration.set("fs.s3a.endpoint", endpoint);
        configuration.set("fs.s3a.endpoint.region", region);
    }

    private static Map<String, String> awsProperties(
            AwsCredentials credentials, Map<String, String> catalogProperties) {
        String region = firstNonBlank(catalogProperties,
                S3_REGION, "s3.region", "client.region", "aws.region");
        if (region == null) {
            throw new IllegalArgumentException(
                    "Unity Delta tables on S3 require 's3.region' so Doris BE can create "
                            + "a native S3 client");
        }
        String endpoint = firstNonBlank(catalogProperties,
                S3_ENDPOINT, "s3.endpoint", "aws.endpoint");
        if (endpoint == null) {
            endpoint = "s3." + region + ".amazonaws.com";
        }

        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(S3_ENDPOINT, endpoint);
        properties.put(S3_REGION, region);
        properties.put(S3_ACCESS_KEY, requireCredentialValue(
                credentials.getAccessKeyId(), "access key"));
        properties.put(S3_SECRET_KEY, requireCredentialValue(
                credentials.getSecretAccessKey(), "secret key"));
        properties.put(S3_TOKEN, requireCredentialValue(
                credentials.getSessionToken(), "session token"));
        return properties;
    }

    private static String requireCredentialValue(String value, String credentialName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned an empty AWS " + credentialName);
        }
        return value;
    }

    private static String firstNonBlank(Map<String, String> properties, String... keys) {
        for (String key : keys) {
            String value = properties.get(key);
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }
}
