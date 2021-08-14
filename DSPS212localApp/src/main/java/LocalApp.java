import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

public class LocalApp {

    public final String TMP_ASS_1 = "/tmp/Ass1/";
    public final String JAR_NAME = "DSP_Ass1_Manager-1.0-SNAPSHOT.jar";
    public final String MANAGER_BOOTSTRAP_SCRIPT =
            "#!/bin/bash\n" +
                    "mkdir " + TMP_ASS_1 + "\n" +
                    "aws s3 cp s3://dsps212-artifacts/jars/" + JAR_NAME + " " + TMP_ASS_1 + "\n" +
                    "java -jar " + TMP_ASS_1 + JAR_NAME + "\n";
    private final String ANALYSIS = "analysis";
    private final String REVIEW = "review";
    private final String NAMED_ENTITIES = "namedEntities";
    private final String SENTIMENT = "sentiment";
    private final String RATING = "rating";
    private final String NOT_SARCASTIC = "NOT_SARCASTIC";
    private final String SARCASTIC = "SARCASTIC";
    private final String LINK = "link";
    public final int FIRST = 0;
    public final String amiId = "ami-081475026498ccd01"; // Linux, Java 1.8 and Git
    private final String OUTGOING_QUEUE_NAME = "localsManagerQueue";
    private final String INCOMING_QUEUE_NAME;
    public final String BUCKET_NAME = "dsps212";
    public final String BUCKET = "bucket";
    public final String INPUT = "input";
    public final String OUTPUT = "output";
    public final String N = "n";
    private final String TERMINATE = "terminate";
    public final String MANAGER = "Manager";
    public final Region REGION = Region.US_EAST_1;
    private final String ROLE = "role";
    private final String TAG_ROLE = "tag:" + ROLE;
    private final String MANAGER_ARN = "arn:aws:iam::695603436761:instance-profile/DSP_AWS_S3";
    private final String QNAME = "localQueueName";
    private String LOCAL_TMP = "";

    private final SqsClient sqs;
    private final Ec2Client ec2;
    private final S3Client s3;
    private String outgoingQueueUrl = null;
    private String incomingQueueUrl = null;
    private final JSONParser parser;

    public LocalApp() {
        INCOMING_QUEUE_NAME = "managerLocalQueue" + System.currentTimeMillis();
        ec2 = Ec2Client.builder().region(REGION).build();
        s3  =  S3Client.builder().region(REGION).build();
        sqs = SqsClient.builder().region(REGION).build();
        parser = new JSONParser();
        try {
            LOCAL_TMP = Files.createTempDirectory("local").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void init(String[] inputs, String[] outputs, int num_reviews_per_worker) {
        initMessageQueue();

        initManager();

        initInputs(inputs);

        sendTasks(inputs, outputs, num_reviews_per_worker);
    }

    private void sendTasks(String[] inputs, String[] outputs, int num_reviews_per_worker) {
        // Send message to manager queue
        for (int i = FIRST; i < inputs.length; i++) {
            JSONObject message = new JSONObject();
            message.put(QNAME, INCOMING_QUEUE_NAME);   //  send him the queue name(Q identifier) as well.
            message.put(BUCKET, BUCKET_NAME);
            message.put(INPUT, inputs[i]);
            message.put(OUTPUT, outputs[i]);
            message.put(N, num_reviews_per_worker);
            sendJsonMessage(message);
        }
    }

    private void initMessageQueue() {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(INCOMING_QUEUE_NAME)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
            incomingQueueUrl = create_result.queueUrl();
        } catch (QueueNameExistsException e) {
            System.out.println("Exception thrown: "+ e);
            System.exit(1);
        }
    }

    private void initInputs(String[] inputs) {
        // Upload input files to storage
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        uploadFiles(inputs);
    }

    private void initManager() {
        // check manager online - start if not
        if (shouldCreateManager()) { // TODO: test
            outgoingQueueUrl = createManagerAndGetQueueUrl();
        }
        else{
//          if manager already exists, then the OUTGOING_QUEUE from locals to manager also exists
//          so we get its url.
            outgoingQueueUrl = getManagerQueueUrl();
        }
    }

    private String getManagerQueueUrl() {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(OUTGOING_QUEUE_NAME)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private void uploadFiles(String[] inputs) {
        for (String inputFile : inputs) {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(inputFile)
                    .build();
            s3.putObject(putObjectRequest, RequestBody.fromFile(new File(inputFile)));
        }
    }

    private String createManagerAndGetQueueUrl() {
//      first create outgoing queue from locals to manager,
//      then create the manager.
        String queueUrl = createManagerQueue();
        runAndTagManager();

        return queueUrl;
    }

    private void tagManager(String instanceId) {
        Tag managerTag = Tag.builder()
                .key(ROLE)
                .value(MANAGER)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(managerTag)
                .build();

        ec2.createTags(tagRequest);
    }

    private void runAndTagManager() {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(MANAGER_ARN).build())
                .instanceType(InstanceType.T2_MEDIUM)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .keyName("DSP_key")
                .userData(Base64.getEncoder().encodeToString(MANAGER_BOOTSTRAP_SCRIPT.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();
        if (instances.size() != 1){
            System.out.println("Error creating manager");
            System.exit(1);
        }

        String instanceId = instances.get(FIRST).instanceId();
        System.out.println("Created manager " + instanceId);
        tagManager(instanceId);
    }

    private String createManagerQueue() {
        String queueUrl;
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(OUTGOING_QUEUE_NAME)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
            queueUrl = create_result.queueUrl();
        } catch (QueueNameExistsException e) {
            queueUrl = getManagerQueueUrl();
        }
        return queueUrl;
    }

    private boolean shouldCreateManager() {
        String nextToken = null;
        boolean shouldCreateManager = true;
        do {
            DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                    .filters(Filter.builder().name(TAG_ROLE).values(MANAGER).build())
                    .nextToken(nextToken)
                    .build();

            DescribeInstancesResponse describeInstances = ec2.describeInstances(describeInstancesRequest);
            List<Reservation> reservations = describeInstances.reservations();
            for (Reservation reservation : reservations) {
                for (Instance instance : reservation.instances()) {
                    InstanceState instanceState = instance.state();
                    while(instanceState.code() == 0) { //Loop until the instance no longer in "pending" state.
                        try {
                            Thread.sleep(5000);
                        } catch(InterruptedException ignored) {}
                    }
                    if (instanceState.nameAsString().equalsIgnoreCase("running")) {
                        shouldCreateManager = false;
                        System.out.println("Found running manager: " + instance.instanceId());
                        break;
                    }
                }
            }
            nextToken = describeInstances.nextToken();
        } while (nextToken != null);
        return shouldCreateManager;
    }

    void run(int remaining, boolean terminate) {
        // Check queue for "Done" messages
        while (remaining > 0) {

            // Receive messages from the queue
            Message message = getSingleMessage();
            if (message == null){
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ignored) {}
                continue;   // go back to beginning of while
            }

            JSONObject body;
            try {
                body = (JSONObject) parser.parse(message.body());
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
            String bucket = (String) body.get(BUCKET);
            String file_name = (String) body.get(OUTPUT);
            System.out.println("Got finished message: "+ file_name);

            // delete message from the queue
            deleteMessage(message, incomingQueueUrl);

            // Download summary from storage
            final Path path = Paths.get(LOCAL_TMP, file_name);
            s3.getObject(GetObjectRequest.builder().bucket(bucket).key(file_name).build(),
                    ResponseTransformer.toFile(path));
            System.out.println("Downloaded summary to: "+ path.toString());

            createHTML(file_name);

            remaining--;
            System.out.println(remaining + " files remaining");
        }

        System.out.println("All files are done!");
        System.out.println("Deleting queue");
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(incomingQueueUrl).build());
        // Terminate if necessary, after all files received
        if (terminate){
            JSONObject terminateMessage = new JSONObject();
            terminateMessage.put(TERMINATE, true);
            System.out.println("Sending terminate message to manager");
            sendJsonMessage(terminateMessage);
        }
        System.exit(0);
    }

    private void createHTML(String file_name) {
//       Parse file into html

        try {
            Path path = Paths.get(LOCAL_TMP, file_name);
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
            String line = reader.readLine();
            FileWriter fileWriter = new FileWriter(file_name);

//          write beginning
            fileWriter.append("<!DOCTYPE html>\n" + "<html lang=\"en\">\n" + "<head>\n" + "\t<title>Reviews Analysis for ")
                    .append(file_name)
                    .append("</title>\n")
                    .append("</head>\n")
                    .append("<body>\n");

//          write line for each review
//            for review, analysis will be one line JSONstring containing:
//            • A link to the original review, coloured according to its sentiment:
//                o dark red: very negative,   sentiment = 0.
//                o red: negative
//                o black: neutral
//                o light green: positive
//                o dark green: very positive, sentiment = 4.
//            • A list of the named entities found in the review (comma separated items with under [])
//            • Sarcasm Detection
//              We apply a simple detection algorithm, based on the number of stars given by the user are suitable for
//              the review sentiment analysis. If it is so, then there is no sarcasm,
//              otherwise it appears to be sarcasm.
            while (line != null) {
                JSONObject result = (JSONObject) parser.parse(line);
                JSONObject analysis = (JSONObject) result.get(ANALYSIS);
                JSONObject review = (JSONObject) result.get(REVIEW);
                String review_link = (String) review.get(LINK);
                int sentiment = Math.toIntExact((long) analysis.get(SENTIMENT));  // between 0 to 4.
                String named_entities = (String) analysis.get(NAMED_ENTITIES);
                String linkColor = getLinkColor(sentiment);
                int stars = Math.toIntExact((long) review.get(RATING));     // between 1 to 5.
                boolean sarcasm = Math.abs(stars - sentiment) > 2;
                String sarcasm_string = NOT_SARCASTIC;
                if(sarcasm) {
                    sarcasm_string = SARCASTIC;
                }
                fileWriter.append("<p><a href= ")
                        .append(review_link)
                        .append("\" style=\"color: ")
                        .append(linkColor)
                        .append(";\">")
                        .append(review_link)
                        .append("</a>, Named Entities: [")
                        .append(named_entities)
                        .append("] , ")
                        .append(sarcasm_string)
                        .append(".</p>\n");

                line = reader.readLine();
            }

//          write ending
            fileWriter.append(
                    "</body>\n" +
                    "</html>\n");
            fileWriter.flush();
            System.out.println("Created HTML File in: "+ path.toString());


        } catch (IOException | ParseException e) {
            System.out.println("Exception thrown: " + e);
        }
    }

    private String getLinkColor(int sentiment) {
        String linkColor;
        switch (sentiment) {
            case 0:
                linkColor = "DarkRed";
                break;
            case 1:
                linkColor = "red";
                break;
            case 2:
                linkColor = "black";
                break;
            case 3:
                linkColor = "LightGreen";
                break;
            case 4:
                linkColor = "DarkGreen";
                break;
            default:
                linkColor = "";
                break;
        }
        return linkColor;
    }

    private void sendJsonMessage(JSONObject message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(outgoingQueueUrl)
                .messageBody(message.toJSONString())
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private Message getSingleMessage() {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(incomingQueueUrl)
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return (messages.isEmpty()) ? null : messages.get(FIRST);
    }

    private void deleteMessage(Message message, String queueUrl) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

}
