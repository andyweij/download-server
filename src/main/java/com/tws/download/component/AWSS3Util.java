package com.tws.download.component;

import com.tws.download.config.S3Config;
import com.tws.download.dto.DownloadInfoDTO;
import com.tws.download.dto.DownloadResponse;
import com.tws.download.dto.S3ObjectInfo;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.*;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AWSS3Util {

    private S3Config awsS3Config;

    private S3AsyncClient s3AsyncClient;

    private S3Client s3Client;

    private S3Presigner s3Presigner;

    @Autowired
    public AWSS3Util(S3Config awsS3Config, S3Presigner s3Presigner) {
        this.awsS3Config = awsS3Config;
        this.s3Client = initS3Client();
        this.s3AsyncClient = initializeS3AsyncClient();
        this.s3Presigner = s3Presigner;
    }

    // 初始化 S3AsyncClient
    private S3AsyncClient initializeS3AsyncClient() {
//        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(awsS3Config.getAccessKey(), awsS3Config.getSecretKey());

        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(50)
                .connectionTimeout(Duration.ofSeconds(120))
                .readTimeout(Duration.ofSeconds(300))
                .writeTimeout(Duration.ofSeconds(300))
                .build();

//        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
//                .apiCallTimeout(Duration.ofMinutes(5))
//                .apiCallAttemptTimeout(Duration.ofSeconds(300))
//                .retryStrategy(RetryMode.STANDARD)
//                .build();

        return S3AsyncClient.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .region(Region.US_WEST_2)
                .serviceConfiguration(builder -> builder.pathStyleAccessEnabled(true))
                .endpointOverride(URI.create(awsS3Config.getEndpoint()))
                .build();
    }

    private S3Client initS3Client() {
//        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(awsS3Config.getAccessKey(), awsS3Config.getSecretKey());
        return S3Client.builder()
                .endpointOverride(URI.create(awsS3Config.getEndpoint()))
                .region(Region.US_WEST_2)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .serviceConfiguration(builder -> builder.pathStyleAccessEnabled(true))
                .build();
    }

    private List<S3Object> listS3Objects(String model) throws S3Exception {
        String bucketName = awsS3Config.getBucketName();
        log.debug("Processing file: {}", model);
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(model)
                .build();

        ListObjectsV2Response result = s3Client.listObjectsV2(request);
        if (result.contents().size() == 0) {
            return Collections.emptyList();
        }

        return result.contents().stream()
                .filter(obj -> !obj.key().endsWith("/")) // 過濾掉類似目錄的物件
                .collect(Collectors.toList());

    }

//    private CompletableFuture<List<S3Object>> listS3ObjectsAsync(String model) {
//        String prefix = awsS3Config.getModelPrefix().length() != 0 ? awsS3Config.getModelPrefix() + "/" : "";
//        ListObjectsV2Request request = ListObjectsV2Request.builder()
//                .bucket(awsS3Config.getBucketName())
//                .prefix(prefix + model + "/")
//                .build();
//
//        return s3AsyncClient.listObjectsV2(request)
//                .thenApply(result -> {
//                    if (result.contents().size() == 0) {
//                        List<S3Object> emptyList = Collections.emptyList();
//                        return emptyList;
//                    }
//                    return result.contents().stream()
//                            .filter(obj -> !obj.key().endsWith("/")) // 過濾掉類似目錄的物件
//                            .collect(Collectors.toList());
//                })
//                .exceptionally(throwable -> {
//                    log.error("Failed to list S3 objects: {}", throwable.getMessage());
//                    throw new RuntimeException(throwable);
//                });
//    }

//    public Optional<URL> getS3ObjectPublicUrls(String model) {
//
//        // 1. 取得 S3Object 列表
//        List<S3Object> s3Objects = listS3Objects(model);
//
//        if (s3Objects.isEmpty()) {
//            return Optional.empty();
//        }
//
//        // 2. 將每個 S3Object 轉換成它的公開 URL
//        return s3Objects.stream()
//                .map(s3Object -> {
//                    // 建立 GetUrlRequest 來取得 URL
//                    GetUrlRequest getUrlRequest = GetUrlRequest.builder()
//                            .bucket(awsS3Config.getBucketName())
//                            .key(s3Object.key())
//                            .build();
//                    // s3Client.utilities().getUrl() 會回傳一個 URL 物件
//                    return s3Client.utilities().getUrl(getUrlRequest);
//                }).findFirst();
//    }

    private void downloadFile(String objectKey, String localFilePath) {
        String bucketName = awsS3Config.getBucketName();
        Path targetPath = Paths.get(localFilePath);
        long existingSize = 0;
        try {
            existingSize = Files.exists(targetPath) ? Files.size(targetPath) : 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long totalSize = getFileSize(bucketName, objectKey);
        if (existingSize >= totalSize) {
            log.info("Download API file :{} already downloaded.", objectKey);
            return;
        }
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .range("bytes=" + existingSize + "-")
                .build();
        log.info("Download API target file: {} ； 已下載: {} bytes", objectKey, existingSize);
        try (ResponseInputStream<GetObjectResponse> responseStream = s3Client.getObject(request);
             FileOutputStream fos = new FileOutputStream(new File(localFilePath), true)) {
            // Transfer the data from the input stream to the file output stream in chunks
            byte[] buffer = new byte[8192]; // 8KB buffer size, adjustable
            int bytesRead;
            while ((bytesRead = responseStream.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
            log.info("Downloaded: {} -> {}", objectKey, localFilePath);
        } catch (IOException e) {
            log.error("Failed to download or write file: {}", e.getMessage());
        }
    }

//    public CompletableFuture<String> downloadModelAsync(String model) {
//        return listS3ObjectsAsync(model)
//                .thenCompose(objects -> {
//                    if (objects.isEmpty()) {
//                        return CompletableFuture.failedFuture(new S3ExceptionHandler.ModelNotFoundException());
//                    }
//                    String prefix = awsS3Config.getModelPrefix();
//                    String localDir = localStorageConfig.getAbsoluteLocalDir();
//                    long modelTotalSize = objects.stream().map(s3Object -> {
//                        String objectKey = s3Object.key();
//                        return getFileSize(awsS3Config.getBucketName(), objectKey);
//                    }).mapToLong(Long::longValue).sum();
//                    log.info("modelTotalSize : {}", modelTotalSize);
//                    // 並行下載所有檔案
//                    List<CompletableFuture<?>> downloadFutures = objects.stream()
//                            .map(s3Object -> {
//                                String objectKey = s3Object.key();
//                                String relativePath = objectKey.substring(prefix.length()); // 取得相對於 prefix 的部分
//                                Path localFilePath = Path.of(localDir, relativePath);
//                                log.info("本地存放位置 : {}", localFilePath);
//                                // 確保目錄存在（同步執行，因為這是輕量操作）
//                                try {
//                                    Files.createDirectories(localFilePath.getParent());
//                                } catch (IOException e) {
//                                    log.error("無法建立目錄：{} {}", localFilePath.getParent(), e.getMessage());
//                                    return CompletableFuture.failedFuture(new IOException("無法建立目錄: " + e.getMessage()));
//                                }
//
//                                // 非同步下載單個檔案
//                                return downloadFileAsync(objectKey, localFilePath);
//                            })
//                            .collect(Collectors.toList());
//
//                    // 等待所有下載完成
//                    String finalPath = Paths.get(localDir, model).toString();
//                    return CompletableFuture.allOf(downloadFutures.toArray(new CompletableFuture[0]))
//                            .thenApply(voidResult -> {
//                                log.info("All files downloaded for model: {}, final path: {}", model, finalPath);
//                                return finalPath; // 返回下載路徑/model
//                            });
//                })
//                .exceptionally(throwable -> {
//                    if (throwable instanceof S3ExceptionHandler.ModelNotFoundException) {
//                        log.error("Model not found in S3: {}", model);
//                    } else if (throwable.getCause() instanceof S3Exception) {
//                        log.error("S3 exception occurred: {}", throwable.getMessage());
//                    } else {
//                        log.error("Unexpected error: {}", throwable.getMessage());
//                    }
//                    throw new RuntimeException(throwable);
//                });
//    }

    public String downloadFiles(String downloadTargetDir, String file, String bucketName, String s3Prefix) {
        String s3filePathStr = s3Prefix + file;
        List<S3Object> objects = listS3Objects(s3filePathStr);
        long modelTotalSize = objects.stream().map(s3Object -> {
            String objectKey = s3Object.key();
            return getFileSize(bucketName, objectKey);
        }).mapToLong(Long::longValue).sum();

        long totalBytes = objects.stream().mapToLong(S3Object::size).sum();
        log.debug("S3 model total size - API : , {}", totalBytes);
        AtomicLong downloadedBytes = new AtomicLong(0);
        log.info("開始下載模型: {}, 總檔案數: {}, 總大小: {} bytes", file, objects.size(), totalBytes);
        List<CompletableFuture<?>> downloadFutures = objects.stream()
                .map(s3Object -> {
                    String objectKey = s3Object.key();
                    long fileSize = s3Object.size(); // 單檔大小

                    String relativePath = objectKey.substring(s3Prefix.length());
                    Path localFilePath = Path.of(downloadTargetDir, relativePath);

                    // 建立目錄 (保留原本邏輯)
                    try {
                        Files.createDirectories(localFilePath.getParent());
                    } catch (IOException e) {
                        return CompletableFuture.failedFuture(new IOException("無法建立目錄: " + e.getMessage()));
                    }

                    // 4.【關鍵】在單檔下載完成後，更新進度
                    return downloadFileAsync(objectKey, localFilePath)
                            .thenApply(path -> {
                                // 累加下載進度
                                long current = downloadedBytes.addAndGet(fileSize);

                                // 計算百分比
                                int percent = (totalBytes > 0) ? (int) ((current * 100) / totalBytes) : 100;

                                // 印出進度 (可依需求調整 Log 等級或頻率)
                                log.info("[Step 1] 下載進度: {}% ({}/{} bytes) - 完成: {}", percent, current, totalBytes, path);

                                return path;
                            });
                })
                .collect(Collectors.toList());

        String finalPath = Paths.get(downloadTargetDir, file).toString();

        // 5. 等待所有任務完成 (支援取消機制)
        CompletableFuture<Void> allDownloads = CompletableFuture.allOf(
                downloadFutures.toArray(new CompletableFuture[0]));

        try {
            // 使用 .get() 阻塞，這樣才能響應 InterruptedException
            allDownloads.get();
            log.info("模型 {} 下載完畢，路徑: {}", file, finalPath);
            return finalPath;

        } catch (InterruptedException e) {
            log.warn("模型下載被中斷！正在取消所有子下載任務...");

            // 【資源清理】確保背景正在跑的下載也被取消
            downloadFutures.forEach(f -> f.cancel(true));

            Thread.currentThread().interrupt();
            throw new RuntimeException("Model download cancelled", e);

        } catch (ExecutionException e) {
            log.error("下載過程中發生錯誤", e);
            // 如果有任何一個檔案下載失敗，這裡會拋出異常
            throw new RuntimeException("Model download failed", e.getCause());
        }
    }

    private CompletableFuture<String> downloadFileAsync(String objectKey, Path localFilePath) {
        long existingSize = 0;
        String bucketName = awsS3Config.getBucketName();
        try {
            existingSize = Files.exists(localFilePath) ? Files.size(localFilePath) : 0;

            long totalSize = getFileSize(bucketName, objectKey);
            if (existingSize >= totalSize) {
                log.info("File :{} already downloaded.", objectKey);
                return CompletableFuture.completedFuture(localFilePath.toString());
            }
            log.info("File :{} , {} bytes remaining to download.", objectKey, totalSize - existingSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .range(existingSize > 0 ? String.format("bytes=%d-", existingSize) : null) // 支援斷點續傳
                .build();
        log.info("Target file: {} ； 已下載: {} bytes", objectKey, existingSize);
        return s3AsyncClient
                .getObject(request, AsyncResponseTransformer.toFile(localFilePath,
                        FileTransformerConfiguration.defaultCreateOrAppend()))
                .thenApply(response -> {
                    log.info("S3 Downloaded: {}", localFilePath);

                    return localFilePath.toString();
                })
                .exceptionally(throwable -> {
                    log.error("Failed to download file {}: {}", objectKey, throwable.getMessage());
                    throw new RuntimeException(throwable);
                });
    }

    private CompletableFuture<String> downloadFileWithResumeAsync(String objectKey, String localFilePath) {
        String bucketName = awsS3Config.getBucketName();
        // 假設已知文件總大小（可通過 HeadObjectRequest 獲取）
        long fileSize = getFileSize(bucketName, objectKey); // 需實作此方法
        long chunkSize = 1024 * 1024 * 5; // 每個範圍 5MB
        File file = new File(localFilePath);
        long downloadedBytes = file.exists() ? file.length() : 0;

        // 如果文件已部分下載，從斷點繼續
        if (downloadedBytes >= fileSize) {
            log.info("File already fully downloaded: {}", localFilePath);
            return CompletableFuture.completedFuture(localFilePath);
        }

        // 構建範圍下載請求
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .range(String.format("bytes=%d-%d", downloadedBytes,
                        Math.min(downloadedBytes + chunkSize - 1, fileSize - 1)))
                .build();

        return s3AsyncClient
                .getObject(request,
                        AsyncResponseTransformer.toFile(Paths.get(localFilePath),
                                FileTransformerConfiguration.defaultCreateOrAppend()))
                .thenApply(response -> {
                    log.info("S3 Downloaded range: {} (bytes {}-{}) -> {}", objectKey, downloadedBytes,
                            Math.min(downloadedBytes + chunkSize - 1, fileSize - 1), localFilePath);
                    // 遞迴或迴圈下載剩餘部分
                    if (downloadedBytes + chunkSize < fileSize) {
                        return downloadFileWithResumeAsync(objectKey, localFilePath).join(); // 遞迴繼續下載
                    }
                    return localFilePath;
                })
                .exceptionally(throwable -> {
                    log.error("Failed to download range {} (bytes {}-{}): {}", objectKey, downloadedBytes,
                            Math.min(downloadedBytes + chunkSize - 1, fileSize - 1), throwable.getMessage());
                    throw new RuntimeException(throwable);
                });
    }

    /**
     * 取得s3上物件檔案大小
     *
     * @param bucketName
     * @param objectKey
     * @return
     */
    private long getFileSize(String bucketName, String objectKey) {
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
        return s3AsyncClient.headObject(headRequest)
                .thenApply(HeadObjectResponse::contentLength)
                .join();
    }
    // public Map<String, ModelInfoListFormatDTO> s3JsonReader(String bucket, String
    // key) throws IOException {
    // GetObjectRequest request = GetObjectRequest.builder()
    // .bucket(bucket)
    // .key(key)
    // .build();
    //
    // try (InputStream is = s3Client.getObject(request);
    // InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8))
    // {
    // Gson gson = new Gson();
    // Type mapType = new TypeToken<Map<String, ModelInfoListFormatDTO>>() {
    // }.getType();
    // Map<String, ModelInfoListFormatDTO> modelInfoMap = gson.fromJson(reader,
    // mapType);
    // log.info("Successfully parsed model info map Result: {}", modelInfoMap);
    // return modelInfoMap;
    // } catch (JsonSyntaxException e) {
    // log.error("Failed to parse JSON : Error: {}", e.getMessage());
    // throw new IOException("Invalid JSON format", e);
    // } catch (IOException e) {
    // throw new IOException();
    // }
    // }

    /**
     * 從指定的 S3 儲存桶和鍵獲取物件的輸入串流。
     *
     * @param bucket S3 儲存桶名稱
     * @param key    S3 物件鍵
     * @return 物件的 InputStream
     * @throws IOException              如果獲取 S3 物件失敗（例如，物件不存在或網路錯誤）
     * @throws IllegalArgumentException 如果 bucket 或 key 為空或無效
     */
    public InputStream getS3ObjectStream(String bucket, String key) throws IOException {
        if (bucket == null || bucket.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 儲存桶名稱不能為空");
        }
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 物件鍵不能為空");
        }

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        try {

            InputStream is = s3Client.getObject(request);
            log.info("Successfully retrieved S3 object: bucket={}, key={}", bucket, key);
            return is;
        } catch (NoSuchKeyException e) {
            log.error("S3 object not found: bucket=" + bucket + ", key=" + key);
            throw new FileNotFoundException("S3 物件不存在: " + key);
        } catch (SdkClientException e) {
            log.error("Failed to retrieve S3 object: bucket=" + bucket + ", key=" + key + ", error=" + e.getMessage());
            throw new IOException("無法從 S3 獲取物件: " + key, e);
        }
    }


    /**
     * 下載s3檔案；取代本地檔案
     *
     * @param bucket
     * @param key
     * @param localPath
     * @throws IOException
     */
    private void downloadAndReplace(String bucket, String key, Path localPath) throws IOException {
        Path tempPath = Files.createTempFile("s3_download_", ".tmp");
        try (InputStream s3Stream = getS3ObjectStream(bucket, key)) {
            Files.copy(s3Stream, tempPath, StandardCopyOption.REPLACE_EXISTING);
        }
        if (Files.exists(localPath) && !Files.isWritable(localPath)) {
            throw new IOException("目標文件不可寫: " + localPath);
        }
        Files.move(tempPath, localPath, StandardCopyOption.REPLACE_EXISTING);
    }

    // 比較 S3 物件和本地文件的修改時間
    public boolean isS3Newer(Path localPath, Instant s3LastModified) throws IOException {
        FileTime localLastModified = Files.getLastModifiedTime(localPath);
        // FileTime.toInstant() 已基於 UTC
        return s3LastModified.isAfter(localLastModified.toInstant());
    }

    @PreDestroy
    public void destroy() throws IOException {
        s3Client.close();
        s3AsyncClient.close();
        log.info("s3Client & s3AsyncClient closed");
    }

//    public String getS3FullPath(String key) {
//        if (awsS3Config.getModelPrefix() != null) {
//            String newDir = awsS3Config.getModelPrefix() + "/" + key;
//            return newDir;
//        }
//        return key;
//    }

    /**
     * Calculates the total size of all objects under a given prefix in an S3 bucket.
     *
     * @param
     * @param bucketName The name of the bucket.
     * @param prefix     The prefix (folder path) to calculate.
     * @return The total size in bytes as a BigInteger.
     */
    public BigInteger calculatePrefixTotalSize(String bucketName, String prefix) {
        // 建立 ListObjectsV2 請求
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        // SDK 的 Paginator 會自動處理分頁請求，非常方便
        ListObjectsV2Iterable responsePaginator = s3Client.listObjectsV2Paginator(request);

        // 使用 Stream API 來處理遍歷和加總
        // 使用 BigInteger 以避免 Long 型別在檔案非常大或非常多時溢位
        return responsePaginator.stream()
                .flatMap(response -> response.contents().stream()) // 將每個 response 的 contents (List<S3Object>) 攤平
                .map(S3Object::size) // 取出每個物件的 size (Long)
                .map(BigInteger::valueOf) // 將 Long 轉換為 BigInteger
                .reduce(BigInteger.ZERO, BigInteger::add); // 將所有 BigInteger 加總
    }

    /**
     * 取得指定目錄下所有檔案的預簽名資訊
     */
    public List<S3ObjectInfo> generateObjectDownloadLinks(String objectName, Duration expiration) {
        String fullPrefix = awsS3Config.getModelPrefix() + "/" + objectName;

        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(awsS3Config.getBucketName())
                .prefix(fullPrefix + "/") // 確保加上結尾斜線，避免查到相似前綴
                .build();

        ListObjectsV2Response listRes = s3Client.listObjectsV2(listReq);

        return listRes.contents().stream()
                .filter(s3Obj -> !s3Obj.key().endsWith("/"))
                .map(s3Obj -> {
                    // 這裡的 s3Obj.key() 已經包含了 prefix，產生 URL 時直接傳入完整 key
                    String url = generateSinglePresignedUrl(s3Obj.key(), expiration);

                    // 擷取相對路徑 (去除 fullPrefix 和後面的斜線)
                    String relativePath = s3Obj.key().substring(fullPrefix.length() + 1);

                    return S3ObjectInfo.builder()
                            .relativePath(relativePath)
                            .size(s3Obj.size())
                            .downloadUrl(url)
                            .build();
                })
                .collect(Collectors.toList());
    }

    /**
     * 產生單一檔案的預簽名 URL
     * 注意：這裡接收的 key 必須是完整的 S3 Key
     */
    public String generateSinglePresignedUrl(String fullKey, Duration expiration) {
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(awsS3Config.getBucketName())
                .key(fullKey)
                .build();

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(expiration)
                .getObjectRequest(objectRequest)
                .build();

        PresignedGetObjectRequest presignedRequest = s3Presigner.presignGetObject(presignRequest);
        return presignedRequest.url().toString();
    }
}
