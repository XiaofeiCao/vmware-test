package com.azure.test;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.policy.ExponentialBackoff;
import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.RetryPolicy;
import com.azure.core.http.policy.RetryStrategy;
import com.azure.core.http.rest.Response;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.Region;
import com.azure.core.management.profile.AzureProfile;
import com.azure.core.test.TestBase;
import com.azure.core.util.logging.ClientLogger;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.fluent.models.DeploymentExtendedInner;
import com.azure.resourcemanager.resources.fluentcore.utils.ResourceManagerUtils;
import com.azure.resourcemanager.resources.models.Deployment;
import com.azure.resourcemanager.resources.models.DeploymentMode;
import com.azure.resourcemanager.resources.models.ProvisioningState;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.resolver.DefaultAddressResolverGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.netty.resources.ConnectionProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class ThreadStuckTests extends TestBase {
    private AzureResourceManager azureResourceManager;
    private static final int PARALLELISM = 4;
    private final Random random = new Random();

    private static final ClientLogger LOGGER = new ClientLogger(ThreadStuckTests.class);
    // tag for resource group for easier cleanup
    private static final String RG_TAG = "vmware";
    private static final String RG_TAG_VALUE = UUID.randomUUID().toString();
    @Test
    public void reproduceTest() throws IOException {
        String templateJson = new String(readAllBytes(AzureResourceManager.class.getResourceAsStream("/vm-template.json")), Charset.forName("utf-8"));
        Assertions.assertNotNull(templateJson);
        AtomicInteger round = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        // run forever
        while (true) {
            LOGGER.info("round: {}, request count: {}, failures: {}", round.get(), counter.get(), failures.get());
            try {
                CountDownLatch ctl = new CountDownLatch(PARALLELISM);

                for (int i = 0; i < PARALLELISM; i++) {
                    counter.incrementAndGet();
                    Deployment beginDeploy;
                    String resourceGroupName = generateRandomResourceName("vmware", 15);
                    try {
                        String deploymentName = generateRandomResourceName("dp", 15);
                        beginDeploy = azureResourceManager
                                .deployments()
                                .define(deploymentName)
                                .withNewResourceGroup(azureResourceManager.resourceGroups().define(resourceGroupName).withRegion(Region.US_EAST).withTag(RG_TAG, RG_TAG_VALUE))
                                .withTemplate(templateJson)
                                .withParameters(new HashMap<String, Object>() {{
                                    setParameter(this, "adminUsername", "azureUser");
                                    setParameter(this, "adminPassword", generateRandomResourceName("Pa5$", 15));
                                }}).withMode(DeploymentMode.INCREMENTAL)
                                .beginCreateAsync()
                                .doOnSuccess(deployment -> LOGGER.info("Create VM: {}", deployment))
                                .block();
                    } catch (Exception e) {
                        LOGGER.logThrowableAsError(e);
                        failures.incrementAndGet();
                        ctl.countDown();
                        continue;
                    }

                    // async wait
                    new Thread(() -> {
                        try {
                            waitForCompletion(resourceGroupName, beginDeploy);
                        } catch (Exception e) {
                            LOGGER.logThrowableAsError(e);
                            failures.incrementAndGet();
                        } finally {
                            ctl.countDown();
                            azureResourceManager.resourceGroups().beginDeleteByName(resourceGroupName);
                        }
                    }, "Thread-" + counter.get()).start();
                }
                try {
                    ctl.await();
                } catch (InterruptedException e) {
                    LOGGER.logThrowableAsError(e);
                }
                ResourceManagerUtils.sleep(Duration.ofSeconds(10));
            } catch (Exception e) {
                LOGGER.logThrowableAsError(e);
            } finally {
                round.incrementAndGet();
            }
        }
    }

    private void waitForCompletion(String resourceGroupName, Deployment deployment) {
        Response<DeploymentExtendedInner> response = Flux.interval(Duration.ofSeconds(30))
                .flatMap(index -> azureResourceManager
                        .genericResources()
                        .manager()
                        .serviceClient()
                        .getDeployments()
                        .getByResourceGroupWithResponseAsync(resourceGroupName, deployment.name()))
                .takeUntil(new Predicate<>() {
                    @Override
                    public boolean test(Response<DeploymentExtendedInner> response) {
                        // stop at server error
                        if (response.getStatusCode() >= 400) {
                            return true;
                        }
                        // stop at final state
                        DeploymentExtendedInner inner = response.getValue();
                        ProvisioningState provisioningState = inner.properties().provisioningState();
                        return isFinalState(provisioningState);
                    }
                }).blockLast();
        if (response.getStatusCode() >= 400) {
            throw new IllegalStateException("response status: " + response.getStatusCode());
        }
        if (!ProvisioningState.SUCCEEDED.equals(response.getValue().properties().provisioningState())) {
            throw new IllegalStateException("provision state: " + response.getValue().properties().provisioningState());
        }
    }

    private boolean isFinalState(ProvisioningState provisioningState) {
        return ProvisioningState.CANCELED.equals(provisioningState)
                || ProvisioningState.SUCCEEDED.equals(provisioningState)
                || ProvisioningState.FAILED.equals(provisioningState);
    }

    private String generateRandomResourceName(String prefix, int length) {
        if (prefix == null) {
            return generateRandomResourceName("df", length);
        }
        StringBuilder result = new StringBuilder(prefix);
        while (result.length() < length) {
            result.append(random.nextInt(10));
        }
        return result.toString();
    }

    protected byte[] readAllBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int readValue;
        byte[] data = new byte[0xFFFF];
        while ((readValue = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, readValue);
        }
        return buffer.toByteArray();
    }
    private static void setParameter(Map<String, Object> parameters, final String name, final Object value) {
        parameters.put(name, new HashMap<String, Object>(){{this.put("value", value);}});
    }

    // test setup
    @Override
    protected void beforeTest() {
        super.beforeTest();
        String tenantId = System.getenv("AZURE_TENANT_ID");
        String clientId = System.getenv("AZURE_CLIENT_ID");
        String clientSecret = System.getenv("AZURE_CLIENT_SECRET");
        String subscriptionId = System.getenv("AZURE_SUBSCRIPTION_ID");
        ConnectionProvider connectionProvider = ConnectionProvider.builder("clouddriverAzureV2Connection")
                .maxConnections(500)
                .pendingAcquireMaxCount(-1)
                .maxIdleTime(Duration.ofSeconds(20))
                .pendingAcquireTimeout(Duration.ofSeconds(90))
                .evictInBackground(Duration.ofSeconds(60))
                .build();
        reactor.netty.http.client.HttpClient nettyHttpClient = reactor.netty.http.client.HttpClient.create(connectionProvider)
                .resolver(DefaultAddressResolverGroup.INSTANCE)
                .option(ChannelOption.SO_KEEPALIVE, true)
                // The options below are available only when Epoll transport is used
                .option(EpollChannelOption.TCP_KEEPIDLE, 300)
                .option(EpollChannelOption.TCP_KEEPINTVL, 60)
                .option(EpollChannelOption.TCP_KEEPCNT, 8);
        HttpClient httpClient = new NettyAsyncHttpClientBuilder(nettyHttpClient)
                .responseTimeout(Duration.of(90, ChronoUnit.SECONDS))
                .readTimeout(Duration.of(60, ChronoUnit.SECONDS))
                .connectTimeout(Duration.of(60, ChronoUnit.SECONDS))
                .connectionProvider(connectionProvider)
                .wiretap(true)
                .build();
        RetryStrategy retryStrategy = buildRetryStrategy();

        RetryPolicy retryPolicy = new RetryPolicy(retryStrategy);

        azureResourceManager = AzureResourceManager.configure()
                .withHttpClient(httpClient)
                .withLogLevel(HttpLogDetailLevel.BODY_AND_HEADERS)
//                .withPolicy(new DynamicThrottlePolicy(clientId))
                .withRetryPolicy(retryPolicy)
                .authenticate(new DefaultAzureCredentialBuilder().build(), new AzureProfile(tenantId, subscriptionId, AzureEnvironment.AZURE))
                .withDefaultSubscription();
    }

    private static RetryStrategy buildRetryStrategy() {
        ExponentialBackoffOptions options = new ExponentialBackoffOptions();
        options.setMaxRetries(60); // This is we want to have more retries against 429

        RetryStrategy retryStrategy =  new ExponentialBackoff(options) {
            @Override
            public boolean shouldRetry(HttpResponse httpResponse) {
                boolean isExpiredToken = httpResponse.getStatusCode() == HttpURLConnection.HTTP_UNAUTHORIZED
                        && httpResponse.getHeaderValue("WWW-Authenticate") == null
                        && httpResponse.getBodyAsBinaryData() != null
                        && httpResponse.getBodyAsBinaryData().toString().contains("ExpiredAuthenticationToken");
                return super.shouldRetry(httpResponse)
                        || isExpiredToken;
            }
        };
        return retryStrategy;
    }
}
