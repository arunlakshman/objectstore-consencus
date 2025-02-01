## Object Store based consensus

### Prerequisites

- Java Development Kit (JDK) 11
- Maven

### Java Version Requirement

This project requires Java 11. Please ensure you have Java 11 installed on your system before proceeding with the installation.

To check your Java version, run the following command in your terminal:
```
java -version
```


### Installation Steps

1. Clone the repository:
```
git clone https://github.com/yourusername/your-repo-name.git
```

2. Navigate to the project directory:
```
cd your-repo-name
```

3. Build the project:
```
mvn clean install
```

## Usage

Instructions on how to use your project...


## References

1. https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html
2. https://github.com/hashicorp/vault/blob/cba7abc64e4d1cb20129b534e3b1a255fbc18977/physical/gcs/gcs_ha.go
3. https://cloud.google.com/blog/topics/developers-practitioners/implementing-leader-election-google-cloud-storage

Rename to org.westmam


```angular2html
16:08:09.833 [pool-2-thread-1] WARN  o.c.o.consensus.s3.lock.S3Lock - No leader election record found
software.amazon.awssdk.services.s3.model.NoSuchKeyException: (Service: S3, Status Code: 404, Request ID: 74038VW6HCJAQM6T, Extended Request ID: UV6QxFDTtfy2Gk3E9mJ5DLh3BJgXFBN9d0fM4aqQpvAJ8gy/Xdhl2YfBhnbYSEbs8maG33xdZQM=) (Service: S3, Status Code: 404, Request ID: 74038VW6HCJAQM6T, Extended Request ID: UV6QxFDTtfy2Gk3E9mJ5DLh3BJgXFBN9d0fM4aqQpvAJ8gy/Xdhl2YfBhnbYSEbs8maG33xdZQM=)
	at software.amazon.awssdk.services.s3.model.NoSuchKeyException$BuilderImpl.build(NoSuchKeyException.java:137)
	at software.amazon.awssdk.services.s3.model.NoSuchKeyException$BuilderImpl.build(NoSuchKeyException.java:91)
	at software.amazon.awssdk.services.s3.internal.handlers.ExceptionTranslationInterceptor.modifyException(ExceptionTranslationInterceptor.java:65)
	at software.amazon.awssdk.core.interceptor.ExecutionInterceptorChain.modifyException(ExecutionInterceptorChain.java:181)
	at software.amazon.awssdk.core.internal.http.pipeline.stages.utils.ExceptionReportingUtils.runModifyException(ExceptionReportingUtils.java:54)
	at software.amazon.awssdk.core.internal.http.pipeline.stages.utils.ExceptionReportingUtils.reportFailureToInterceptors(ExceptionReportingUtils.java:38)
	at software.amazon.awssdk.core.internal.http.pipeline.stages.ExecutionFailureExceptionReportingStage.execute(ExecutionFailureExceptionReportingStage.java:39)
	at software.amazon.awssdk.core.internal.http.pipeline.stages.ExecutionFailureExceptionReportingStage.execute(ExecutionFailureExceptionReportingStage.java:26)
	at software.amazon.awssdk.core.internal.http.AmazonSyncHttpClient$RequestExecutionBuilderImpl.execute(AmazonSyncHttpClient.java:210)
	at software.amazon.awssdk.core.internal.handler.BaseSyncClientHandler.invoke(BaseSyncClientHandler.java:103)
	at software.amazon.awssdk.core.internal.handler.BaseSyncClientHandler.doExecute(BaseSyncClientHandler.java:173)
	at software.amazon.awssdk.core.internal.handler.BaseSyncClientHandler.lambda$execute$1(BaseSyncClientHandler.java:80)
	at software.amazon.awssdk.core.internal.handler.BaseSyncClientHandler.measureApiCallSuccess(BaseSyncClientHandler.java:182)
	at software.amazon.awssdk.core.internal.handler.BaseSyncClientHandler.execute(BaseSyncClientHandler.java:74)
	at software.amazon.awssdk.core.client.handler.SdkSyncClientHandler.execute(SdkSyncClientHandler.java:45)
	at software.amazon.awssdk.awscore.client.handler.AwsSyncClientHandler.execute(AwsSyncClientHandler.java:53)
	at software.amazon.awssdk.services.s3.DefaultS3Client.headObject(DefaultS3Client.java:7036)
	at software.amazon.awssdk.services.s3.S3Client.headObject(S3Client.java:13336)
	at org.cloud.objectstore.consensus.s3.lock.S3Lock.get(S3Lock.java:35)
	at org.cloud.objectstore.consensus.common.leaderelection.DefaultLeaderElector.tryAcquireOrRenew(DefaultLeaderElector.java:240)
	at org.cloud.objectstore.consensus.common.leaderelection.DefaultLeaderElector.lambda$acquire$5(DefaultLeaderElector.java:146)
	at org.cloud.objectstore.consensus.common.leaderelection.DefaultLeaderElector.lambda$loop$0(DefaultLeaderElector.java:69)
	at java.base/java.util.concurrent.CompletableFuture$AsyncRun.run(CompletableFuture.java:1736)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:304)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at java.base/java.lang.Thread.run(Thread.java:829)
16:08:09.834 [pool-2-thread-1] INFO  o.c.o.c.c.l.DefaultLeaderElector - No current le
```
