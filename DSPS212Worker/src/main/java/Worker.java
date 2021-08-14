import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;


public class Worker {
    private final int TIMEOUT = 60;
    private final String NAMED_ENTITIES = "namedEntities";
    private final String SENTIMENT = "sentiment";
    private final String TEXT = "text";
    private final SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    private final NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

    public final int FIRST = 0;
    private final String INCOMING_MANAGER_WORKER_QUEUE = "managerWorkerQueue";
    private final String OUTGOING_WORKER_QUEUE_NAME = "workerManagerQueue";
    public final String OUTPUT = "output";
    public final String REVIEW = "review";
    public final String ANALYSIS = "analysis";
    public final Region REGION = Region.US_EAST_1;
    private final String incomingManagerQueueUrl;
    private final String outgoingManagerQueueUrl;
    private final SqsClient sqs;
    private final JSONParser parser;

    public Worker() {
        sqs = SqsClient.builder().region(REGION).build();
        parser = new JSONParser();

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(INCOMING_MANAGER_WORKER_QUEUE)
                .build();
        incomingManagerQueueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(OUTGOING_WORKER_QUEUE_NAME)
                .build();
        outgoingManagerQueueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

    }

    void run() {
//      A worker process resides on an EC2 node. His life cycle is Repeatedly:
        while (true){
            handleManagerMessage();
        }
    }

    private void handleManagerMessage() {
        // Get a message from an SQS queue.
        Message managerMessage = getSingleMessage();
        if (managerMessage == null){
            return;
        }
        sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(incomingManagerQueueUrl)
                .receiptHandle(managerMessage.receiptHandle())
                .visibilityTimeout(TIMEOUT)
                .build());

        JSONObject body;
        try {
            body = (JSONObject) parser.parse(managerMessage.body());
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        JSONObject review = (JSONObject) body.get(REVIEW);
        String review_string = (String) review.get(TEXT);
        String output_file = (String) body.get(OUTPUT);

//      Perform the requested job, and return the result.

//      does his job and put in in analysis
        int sentiment = sentimentAnalysisHandler.findSentiment(review_string); // between 0 to 4.
        String named_entities = namedEntityRecognitionHandler.getEntities(review_string);

        JSONObject analysis = new JSONObject();
        analysis.put(NAMED_ENTITIES, named_entities);
        analysis.put(SENTIMENT, sentiment);

//      Send report to manager
        JSONObject report = new JSONObject();
        report.put(ANALYSIS, analysis);
        report.put(REVIEW, review);
        report.put(OUTPUT, output_file);
        sendJsonMessage(report);

//      Remove the processed message from the SQS queue.
        deleteMessage(managerMessage);
    }

    private void sendJsonMessage(JSONObject message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(outgoingManagerQueueUrl)
                .messageBody(message.toJSONString())
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private Message getSingleMessage() {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(incomingManagerQueueUrl)
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return (messages.isEmpty()) ? null : messages.get(FIRST);
    }

    private void deleteMessage(Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(incomingManagerQueueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

}

//  json format of input files:

//  each line is a jsonObject with these fields: title, reviews.
//  reviews is an array of jsonObjects, each with these fields: id, link, tile, text, rating, author, date.

// line --> {"title":"t","reviews":[{"id":"i","link":"l","title":"t","text":"t","rating":"r","author":"a","date":"d"},...,{}]}