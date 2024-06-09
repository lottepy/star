package com.magnumresearch.aqumon.riskengine.adapter.boci.config;

import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import feign.Client;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.InputStream;
import java.security.KeyStore;

@Slf4j
public class BOCIFeignConfig {
    @Value("${adapter.boci.ssl.key-store}")
    private String sslKeyStore;

    @Value("${adapter.boci.ssl.key-store-password}")
    private String sslKeyStorePassword;

    @Value("${adapter.boci.ssl.key-store-type}")
    private String sslKeyStoreType;

    @Bean
    public Client feignClient() throws TradingException {
        log.info("[feignClient] start init BOCIFeignConfig...");
        return new Client.Default(getSSLSocketFactory(), new NoopHostnameVerifier());
    }

    private SSLSocketFactory getSSLSocketFactory() throws TradingException {
        try {
            // 客户端证书
            KeyStore keyStore = KeyStore.getInstance(sslKeyStoreType);
            ClassPathResource classPathResource = new ClassPathResource(sslKeyStore);
            InputStream inputStream = classPathResource.getInputStream();
            keyStore.load(inputStream, sslKeyStorePassword.toCharArray());

            SSLContext sslContext = SSLContextBuilder
                    .create()
                    .loadKeyMaterial(keyStore, sslKeyStorePassword.toCharArray())
                    .loadTrustMaterial((x, y) -> true)
                    .build();
            return sslContext.getSocketFactory();
        } catch (Exception exception) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_BOCI_ERROR,
                    "[getSSLSocketFactory] Can not init https jks for boci feign client!");
        }
    }
}
