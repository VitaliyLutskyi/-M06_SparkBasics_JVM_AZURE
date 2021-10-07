package com.epam.data.engineering;

import org.apache.spark.sql.SparkSession;

public class SparkHandler {
//    put credentials here
    private static final String oAuthClientId = "";
    private static final String oAuthClientSecret = "";
    private static final String oAuthClientEndpoint = "";
    private static final String storageAccountKey = "";

    public static SparkSession getSparkSession() {
        return SparkSession.builder().appName("SparkBasics")
                .config("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
                .config("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                .config("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", oAuthClientId)
                .config("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", oAuthClientSecret)
                .config("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", oAuthClientEndpoint)
                .config("fs.azure.account.key.sadevwesteuropevl.dfs.core.windows.net", storageAccountKey)
                .getOrCreate();
    }
}
