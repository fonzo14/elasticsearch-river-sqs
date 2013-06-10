package org.elasticsearch.river.sqs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class SqsRiver extends AbstractRiverComponent implements River {
	
	private final Client client;
	
	private final String sqsRegion;
	private final String sqsAccessKey;
	private final String sqsSecretKey;
	private final String sqsQueueUrl;
	private final int sqsMaxNumberOfMessages;
	private final int sqsWaitTimeSeconds;
	private final int sqsVisibilityTimeout;
	private final boolean sqsThrottling;
	private final long sqsSleepTime;
	
	private final int bulkSize;
    private final long bulkTimeout;
    private final boolean ordered;
    
    private volatile boolean closed = false;
    
    private volatile Thread thread;
    
    private final AmazonSQSAsyncClient sqs;

    @SuppressWarnings("unchecked")
    @Inject
    public SqsRiver(RiverName riverName, RiverSettings settings, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.client = client;
        
        if (settings.settings().containsKey("sqs")) {
        	Map<String, Object> sqsSettings = (Map<String, Object>) settings.settings().get("sqs");
        	sqsRegion = XContentMapValues.nodeStringValue(sqsSettings.get("region"), "us-east-1");
        	sqsAccessKey = XContentMapValues.nodeStringValue(sqsSettings.get("access_key"), null);
        	sqsSecretKey = XContentMapValues.nodeStringValue(sqsSettings.get("secret_key"), null);
        	sqsQueueUrl = XContentMapValues.nodeStringValue(sqsSettings.get("queue_url"), null);
        	sqsMaxNumberOfMessages = XContentMapValues.nodeIntegerValue(sqsSettings.get("max_number_of_messages"), 10);
        	sqsWaitTimeSeconds = XContentMapValues.nodeIntegerValue(sqsSettings.get("wait_time_seconds"), 20);
        	sqsVisibilityTimeout = XContentMapValues.nodeIntegerValue(sqsSettings.get("visibility_timeout"), 1800);
        	sqsThrottling = XContentMapValues.nodeBooleanValue(sqsSettings.get("throttling"), true);
        	sqsSleepTime = XContentMapValues.nodeLongValue(sqsSettings.get("sleep_time"), 50);
        } else {
        	sqsRegion = settings.globalSettings().get("cloud.aws.region");
        	sqsAccessKey = settings.globalSettings().get("cloud.aws.access_key");
        	sqsSecretKey = settings.globalSettings().get("cloud.aws.secret_key");
        	sqsQueueUrl = settings.globalSettings().get("cloud.aws.sqs.queue_url");
        	sqsMaxNumberOfMessages = 10;
        	sqsWaitTimeSeconds = 20;
        	sqsVisibilityTimeout = 1800;
        	sqsThrottling = true;
        	sqsSleepTime = 50;
        }
        
    	if (settings.settings().containsKey("index")) {
    		Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
    		bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = XContentMapValues.nodeLongValue(indexSettings.get("bulk_timeout"), 10000);
            } else {
                bulkTimeout = 10000;
            }
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
    	} else {
            bulkSize = 100;
            bulkTimeout = 10000;
            ordered = false;
        }
    	
    	String endpoint = "https://sqs.".concat(sqsRegion).concat(".amazonaws.com");
		sqs = new AmazonSQSAsyncClient(new BasicAWSCredentials(sqsAccessKey, sqsSecretKey));
		sqs.setEndpoint(endpoint);
	}

	@Override
	public void start() {
		logger.info("creating sqs river queue {} bulk_size {} bulk_timeout {} ordered {}", sqsQueueUrl, bulkSize, bulkTimeout, ordered);
		
		thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "sqs_river").newThread(new Consumer());
        thread.start();
	}
	
	@Override
	public void close() {
		if (closed) {
            return;
        }
        logger.info("closing sqs river");
        closed = true;
        thread.interrupt();
	}
	
	private class Consumer implements Runnable {

		@Override
		public void run() {
			while(true) {
				if (closed) {
					break;
				}
				
				List<String> messages = getBulkMessages();
				
				if (messages != null && !messages.isEmpty()) {
					BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
					
					for (String message : messages) {
						try {
							byte[] data = message.getBytes();
							bulkRequestBuilder.add(data, 0, data.length, false);
						} catch (Exception e) {
							logger.warn("failed to add message {}", message);
						}
					}
					
					long startBulkTimeMillis = System.currentTimeMillis();
					int  noMessagesCount = 0;
					
					while ((bulkRequestBuilder.numberOfActions() < bulkSize) && ((System.currentTimeMillis() - startBulkTimeMillis) < bulkTimeout )) {
						messages = getBulkMessages();
						
						if (messages != null && !messages.isEmpty()) {
							for (String message : messages) {
								try {
									byte[] data = message.getBytes();
									bulkRequestBuilder.add(data, 0, data.length, false);
								} catch (Exception e) {
									logger.warn("failed to add message {}", message);
								}
							}
						}
						
						long sleepTime = sqsSleepTime;
						
						if (sqsThrottling) {
							if (messages.size() < sqsMaxNumberOfMessages) {
								noMessagesCount++;
								sleepTime = (long)(1000 + (Math.log(noMessagesCount) * 1000));
							} 
						}
						
						try {
							if (logger.isDebugEnabled()) {
								logger.debug("Sleeping {}ms", sleepTime);
							}
							Thread.sleep(sleepTime);
						} catch (InterruptedException e) {
							logger.warn("Interrupt thread while sleeping", e);
						}						
					}
					
					if (logger.isDebugEnabled()) {
                        logger.debug("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                    }
					
					if (ordered) {
                        try {
                            if (bulkRequestBuilder.numberOfActions() > 0) {
                              BulkResponse response = bulkRequestBuilder.execute().actionGet();
                              if (response.hasFailures()) {
                                logger.warn("failed to execute" + response.buildFailureMessage());
                              }
                            }
                        } catch (Exception e) {
                            logger.warn("failed to execute bulk", e);
                        }
                    } else {
                        if (bulkRequestBuilder.numberOfActions() > 0) {
                            bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
								@Override
								public void onResponse(BulkResponse response) {
                                    if (response.hasFailures()) {
                                      logger.warn("failed to execute" + response.buildFailureMessage());
                                    }
                                }
								
								@Override
								public void onFailure(Throwable e) {
                                    logger.warn("failed to execute bulk for messages", e);
                                }
							});
                        }
                    }
					
				}

			}
		}
		
		private List<String> getBulkMessages() {
			List<String> messages = new ArrayList<String>();

			try {
				ReceiveMessageRequest request = new ReceiveMessageRequest(sqsQueueUrl);
				request.setMaxNumberOfMessages(sqsMaxNumberOfMessages);
				request.setWaitTimeSeconds(sqsWaitTimeSeconds);
				request.setVisibilityTimeout(sqsVisibilityTimeout);
				
				List<Message> sqsMessages = sqs.receiveMessage(request).getMessages();
				
				if (logger.isDebugEnabled()) {
					logger.debug(("Received {} messages from queue"), sqsMessages.size());
				}

				if (sqsMessages != null && !sqsMessages.isEmpty()) {
					List<DeleteMessageBatchRequestEntry> entries = new ArrayList<DeleteMessageBatchRequestEntry>();
					
					for (Message message : sqsMessages) {
						if (message.getBody() != null) {
							messages.add(message.getBody());
						}
						
						entries.add(new DeleteMessageBatchRequestEntry(message.getMessageId(), message.getReceiptHandle()));
					}
					
					if (!entries.isEmpty()) {
						if (logger.isDebugEnabled()) {
							logger.debug(("Deleting {} messages from queue"), entries.size());
						}
						DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(sqsQueueUrl, entries);
						sqs.deleteMessageBatchAsync(deleteMessageBatchRequest);
					}
				}
			} catch (AmazonServiceException ase) {
				logger.error(ase.getMessage());
			} catch (AmazonClientException ace) {
				logger.error("Could not reach SQS. {}", ace.getMessage());
			}

			return messages;
		}
		
	}
	
}
