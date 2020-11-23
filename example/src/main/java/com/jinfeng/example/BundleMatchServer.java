package com.jinfeng.example;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.*;
import com.jinfeng.common.utils.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * @package: mobvista.dmp.datasource.bundle.service
 * @author: wangjf
 * @date: 2020-11-02
 * @time: 11:22
 * @email: jinfeng.wang@mobvista.com
 */
public class BundleMatchServer {
    static Logger logger;

    static ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(100, 200, 500, TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<>(200), new CustomizableThreadFactory("BundleMatchMain"), new ThreadPoolExecutor.CallerRunsPolicy());

    public static void main(String[] args) throws JoranException, IOException {

        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter("/Users/wangjf/Workspace/data/pkg_mapping.txt"));
        } catch (IOException e) {
            logger.info("IOException -->> " + e.getMessage());
        }

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        context.reset();
        configurator.doConfigure(BundleMatchServer.class.getClassLoader().getResourceAsStream("logback-syslog.xml"));
        logger = context.getLogger("BundleMatchMain");
        long start = System.currentTimeMillis();
        Set<String> pkgs = readFile("/Users/wangjf/Workspace/data/pkg.txt");
        logger.info("Correct PKS size -->> " + pkgs.size());

        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(poolExecutor);
        MoreExecutors.addDelayedShutdownHook(listeningExecutor, 2, TimeUnit.SECONDS);

        List<ListenableFuture<BundleClass>> futures = new CopyOnWriteArrayList<>();

        List<BundleClass> resultList = new CopyOnWriteArrayList<>();

        for (String pkg : pkgs) {
            ListenableFuture listenableFuture = listeningExecutor.submit(() -> {
                String packageName = request(pkg);
                BundleClass bundleClass = new BundleClass();
                bundleClass.setBundleId(pkg);
                bundleClass.setPackageName(packageName);
                return bundleClass;
            });
            Futures.addCallback(listenableFuture, new FutureCallback<BundleClass>() {
                @Override
                public void onSuccess(BundleClass bundleClass) {
                    if (StringUtils.isNotBlank(bundleClass.getPackageName())) {
                        logger.info("PKG -->> " + bundleClass.getBundleId() + ", ID -->> " + bundleClass.getPackageName());
                        resultList.add(bundleClass);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.info("NO DO!Throwable -->> " + t.getMessage());
                    //  NO DO!
                }
            });
            futures.add(listenableFuture);
        }
        for (ListenableFuture<BundleClass> future : futures) {
            try {
                BundleClass bundleClass = future.get();
                if (StringUtils.isNotBlank(bundleClass.getPackageName())) {
                    assert out != null;
                    out.write(bundleClass.getBundleId() + "," + bundleClass.getPackageName());
                    out.newLine();
                }
            } catch (InterruptedException | ExecutionException | IOException e) {
                logger.info(e.getMessage());
            }
        }
        assert out != null;
        out.flush();
        out.close();
        long end = System.currentTimeMillis();
        logger.info("Runtime -->> " + (end - start));
        poolExecutor.shutdown();
        if (poolExecutor.isShutdown()) {
            System.exit(0);
        }
    }

    public static Set<String> readFile(String filePath) {
        Set<String> pkgSet = new HashSet<>();
        try {
            File file = new File(filePath);
            if (file.isFile() && file.exists()) {
                // 考虑到编码格式
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(read);

                String lineTxt;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    if (Constants.adrPkgPtn.matcher(lineTxt).matches()) {
                        pkgSet.add(lineTxt);
                    }
                }
                bufferedReader.close();
                read.close();
            } else {
                logger.info("NOT FOUND !");
            }
        } catch (Exception e) {
            logger.info("Read File Error !Exception -->> " + e.getMessage());
        }
        return pkgSet;
    }

    public static String request(String packageName) {
        CloseableHttpClient client = HttpClients.createDefault();
        List<BasicNameValuePair> formparams = new ArrayList<>();

        final String serverUrl = "http://itunes.apple.com/lookup";
        URIBuilder uri = new URIBuilder();
        try {
            uri = new URIBuilder(serverUrl).addParameter("bundleId", packageName)
                    .addParameter("country", "US");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(2000).setConnectionRequestTimeout(2000)
                .setSocketTimeout(2000).build();

        HttpGet httpGet = new HttpGet();

        String trackId = "";
        CloseableHttpResponse response;
        try {
            httpGet = new HttpGet(uri.build());
            httpGet.setConfig(requestConfig);
            response = client.execute(httpGet);

            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            JSONObject jsonObject = Constants.String2JSONObject(result.toString());
            if (jsonObject.getInteger("resultCount") > 0 && jsonObject.getJSONArray("results") != null && jsonObject.getJSONArray("results").size() > 0) {
                trackId = jsonObject.getJSONArray("results").getJSONObject(0).getString("trackId");
            }
        } catch (URISyntaxException | IOException e) {
            logger.info("URISyntaxException | IOException -->> " + e.getMessage());
        } finally {
            httpGet.abort();
        }
        return trackId;
    }
}
