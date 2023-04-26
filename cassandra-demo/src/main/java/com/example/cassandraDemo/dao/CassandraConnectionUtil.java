package com.example.cassandraDemo.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.springframework.stereotype.Component;

;

@Component
public class CassandraConnectionUtil {
    private Cluster cluster;


    public Session getSession() {

       /* try {
            //Load cassandra endpoint details from config.properties

            final KeyStore keyStore = KeyStore.getInstance("JKS");
            try (final InputStream is = new FileInputStream("C:\\Users\\swapnil_dhage\\Downloads\\truststore")) {
                keyStore.load(is, "cassa@2@2!".toCharArray());
            }

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            kmf.init(keyStore, "cassa@2@2!".toCharArray());
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                    .getDefaultAlgorithm());
            tmf.init(keyStore);

            // Creates a socket factory for HttpsURLConnection using JKS contents.
            final SSLContext sc = SSLContext.getInstance("TLSv1.2");
            sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new java.security.SecureRandom());

            JdkSSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sc)
                    .build();*/
            cluster = Cluster.builder()
                    .addContactPoint("localhost")
                    .withPort(9042)
                    .withCredentials("cassandra", "cassandra")
                    .build();
            return cluster.connect("versionindexer");
    }


    public Cluster getCluster() {
        return cluster;
    }

    /**
     * Closes the cluster and Cassandra session
     */
   /* public void close() {
        cluster.close();
    }*/

}
