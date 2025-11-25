package org.apache.fluss.fs.abfs.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AzureDelegationTokenProviderTest {

    private static final String CONFIG_PREFIX = "fs.azure.account.oauth2.client";
    private static final String CLIENT_ID = "testClientId";
    private static final String CLIENT_SECRET = "testClientSecret";

    private static final String ENDPOINT_KEY = "http://localhost:8080";

    private static MockAuthServer mockGSServer;

    @BeforeAll
    static void setup() {
        mockGSServer = MockAuthServer.create();
    }

    @Test
    void obtainSecurityTokenShouldReturnSecurityToken() {
        Configuration configuration = new Configuration();
        configuration.set(CONFIG_PREFIX + ".id", CLIENT_ID);
        configuration.set(CONFIG_PREFIX + ".secret", CLIENT_SECRET);
        configuration.set(CONFIG_PREFIX + ".endpoint", ENDPOINT_KEY);
        AzureDelegationTokenProvider azureDelegationTokenProvider =
                new AzureDelegationTokenProvider("abfs", configuration);
        ObtainedSecurityToken obtainedSecurityToken =
                azureDelegationTokenProvider.obtainSecurityToken();
        byte[] token = obtainedSecurityToken.getToken();
        Credentials credentials = CredentialsJsonSerde.fromJson(token);
        assertEquals(credentials.getSecurityToken(), "token");
    }

    @AfterAll
    static void tearDown() {
        mockGSServer.close();
    }
}
