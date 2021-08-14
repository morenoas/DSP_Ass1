import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Manager {
    public static final String TMP_ASS_1 = "/tmp/Ass1/";
    public static final String JAR_NAME = "DSP_Ass1_Worker-1.0-SNAPSHOT.jar";
    private static final String ID = "id";
    public final String MANAGER = "Manager";
    private final String WORKER_ARN = "arn:aws:iam::695603436761:instance-profile/DSP_AWS_S3";
    private String MANAGER_TEMP_DIR;
    public final int FIRST = 0;
    public final String BUCKET_NAME = "dsps212";
    private final String TERMINATE = "terminate";
    public final String WORKER = "Worker";
    private final String QNAME = "localQueueName";
    public final int MAX_INSTANCES = 15;
    public final String WORKER_BOOTSTRAP_SCRIPT =
            "#!/bin/bash\n" +
                    "mkdir " + TMP_ASS_1 + "\n" +
                    "aws s3 cp s3://dsps212-artifacts/jars/" + JAR_NAME + " " + TMP_ASS_1 + "\n" +
                    "java -jar " + TMP_ASS_1 + JAR_NAME + "\n";

    private final ConcurrentHashMap<String, String> localQName_to_QUrl;
    private final ConcurrentHashMap<String, String> outputFileName_to_localQName;
    private final ConcurrentHashMap<String, HashSet<String>> fileReviews;

    private final String INCOMING_LOCAL_QUEUE_NAME = "localsManagerQueue";
    private final String INCOMING_WORKER_QUEUE_NAME = "workerManagerQueue";
    private final String OUTGOING_WORKER_QUEUE_NAME = "managerWorkerQueue";
//    private final String TAG_KEY = "tag-key";
    public final String BUCKET = "bucket";
    public final String INPUT = "input";
    public final String OUTPUT = "output";
    public final String N = "n";
    public final String REVIEWS = "reviews";
    public final String REVIEW = "review";
    public final String ANALYSIS = "analysis";
    public final Region REGION = Region.US_EAST_1;
    private final String ROLE = "role";
    private final String TAG_ROLE = "tag:" + ROLE;
    private final String workerAmiId = "ami-081475026498ccd01"; // Linux, Java 1.8 and Git
    private final String incomingLocalQueueUrl;
    private String incomingWorkerQueueUrl;
    private String outgoingWorkerQueueUrl;
    private boolean isTerminate;

    private final Ec2Client ec2;
    private final S3Client s3;
    private final SqsClient sqs;
    private final ThreadPoolExecutor executor;

    public Manager() {
        localQName_to_QUrl = new ConcurrentHashMap<>();
        outputFileName_to_localQName = new ConcurrentHashMap<>();
        fileReviews = new ConcurrentHashMap<>();

        ec2 = Ec2Client.builder().region(REGION).build();
        s3  =  S3Client.builder().region(REGION).build();
        sqs = SqsClient.builder().region(REGION).build();

        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(INCOMING_LOCAL_QUEUE_NAME)
                .build();
        incomingLocalQueueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(INCOMING_WORKER_QUEUE_NAME)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
            incomingWorkerQueueUrl = create_result.queueUrl();
            request = CreateQueueRequest.builder()
                    .queueName(OUTGOING_WORKER_QUEUE_NAME)
                    .build();
            create_result = sqs.createQueue(request);
            outgoingWorkerQueueUrl = create_result.queueUrl();
        } catch (Exception e) {
            System.out.println("Exception thrown: "+ e);
            System.exit(1);
        }
        try {
            MANAGER_TEMP_DIR = Files.createTempDirectory("manager").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static SendMessageBatchRequestEntry createSendMessageBatchRequestEntry(int id, JSONObject message) {
        return SendMessageBatchRequestEntry.builder()
                .id("msg_" + id)
                .messageBody(message.toJSONString())
                .build();
    }

    void run() {
        while (true) {
            if(!isTerminate) {
                fetchLocalMessage();
            }
            fetchWorkerMessage();
            if (isTerminate & fileReviews.isEmpty()) {   //  only last message
                terminateManager();
                System.exit(0);
            }
        }
    }

    private void terminateManager() {
        // TODO: terminate workers
        List<String> instanceIds = describeWorkers();
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceIds)
                .build();
        System.out.println("Terminating " + instanceIds.size() + " workers");
        ec2.terminateInstances(terminateRequest);
        System.out.println("Deleting queues");
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(incomingLocalQueueUrl).build());
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(incomingWorkerQueueUrl).build());
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(outgoingWorkerQueueUrl).build());

        terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(getSelfID())
                .build();
        System.out.println("Terminating self");
        ec2.terminateInstances(terminateRequest);
    }

    private void fetchWorkerMessage() {

//      o In case the manager receives response messages from the workers (regarding input file), it:
//          ▪ Creates a summary output file accordingly,
//          ▪ Uploads the output file to S3,
//          ▪ Sends a message to the application with the location of the file.
        Message workerMessage = getSingleMessage(incomingWorkerQueueUrl);
        if (workerMessage == null){
            return;
        }
        executor.submit(()->handleWorkerMessage(workerMessage));
    }

    private void handleWorkerMessage(Message workerMessage) {
        JSONParser parser = new JSONParser();
        JSONObject body;
        try {
            body = (JSONObject) parser.parse(workerMessage.body());
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        JSONObject review = (JSONObject) body.get(REVIEW);
        String reviewId = (String) review.get(ID);
        String output_file = (String) body.get(OUTPUT);
        deleteMessage(workerMessage, incomingWorkerQueueUrl);

        // Atomic Variables to be used in concurrent compute method.
        AtomicInteger reviewsLeft = new AtomicInteger();
        AtomicBoolean hasfile = new AtomicBoolean();
        AtomicBoolean hasReview = new AtomicBoolean();

//      make sure we don't receive a message more than once.
        fileReviews.compute(output_file, (key, value) -> {
            final boolean hasKey = value != null;
            hasfile.set(hasKey);
            if (!hasKey) {
                return null;
            }
            hasReview.set(value.remove(reviewId));
            reviewsLeft.set(value.size());
            return value.isEmpty() ? null : value;
        });

        if (!hasfile.get()) {
            System.out.println("Error: missing review file path in map: " + output_file);
            return;
        }
        if (!hasReview.get()) {
            System.out.println("Error: review: " + reviewId + " for file: " + output_file + " already processed");
            return;
        }

        output_file = handleReport(body);
        if (reviewsLeft.get() > 0) {
            System.out.println(reviewsLeft.get() + " reviews left for " + output_file);
        }
        else { // last review
            System.out.println(output_file + " is ready");
            // Put file in storage
            putObjectInStorage(output_file);
            // Send response to local
            JSONObject message = new JSONObject();
            message.put(BUCKET, BUCKET_NAME);
            message.put(OUTPUT, output_file);
            String queueName = outputFileName_to_localQName.remove(output_file);
            String localQueueUrl = localQName_to_QUrl.get(queueName);
            System.out.println("Informing local");
            sendJsonMessage(message, localQueueUrl);
            String finalOutput_file = output_file;
        }
    }

    private Message getSingleMessage(String queueUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return (messages.isEmpty()) ? null : messages.get(FIRST);
    }

    private void putObjectInStorage(String output_file) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(output_file)
                .build();
        Path path = Paths.get(MANAGER_TEMP_DIR, output_file);
        s3.putObject(putObjectRequest, RequestBody.fromFile(path.toFile()));
    }

    private void sendWorkerMessages(List<JSONObject> messages) {
        SendMessageBatchRequestEntry[] entries = IntStream.range(0, messages.size())
                .mapToObj(i -> createSendMessageBatchRequestEntry(i, messages.get(i)))
                .toArray(SendMessageBatchRequestEntry[]::new);
        SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
                .queueUrl(outgoingWorkerQueueUrl)
                .entries(entries)
                .build();
        System.out.println("Sending " + messages.size() + " worker messages");
        try {
            sqs.sendMessageBatch(send_batch_request);
        } catch (AwsServiceException | SdkClientException e) {
            e.printStackTrace();
        }
    }
    private void sendJsonMessage(JSONObject message, String queueUrl) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message.toJSONString())
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private void deleteMessage(Message message, String queueUrl) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    private synchronized String handleReport(JSONObject body) {
        JSONObject analysis = (JSONObject) body.get(ANALYSIS);
        JSONObject review = (JSONObject) body.get(REVIEW);
        String output_file = (String) body.get(OUTPUT);

        JSONObject report = new JSONObject();
        report.put(ANALYSIS, analysis);
        report.put(REVIEW, review);

        try {
            System.out.println("appending report to file " + output_file);
            Path path = Paths.get(MANAGER_TEMP_DIR, output_file);
            String reviewString = report.toJSONString() + System.lineSeparator();
            Files.write(path, reviewString.getBytes(UTF_8),
                    StandardOpenOption.CREATE,StandardOpenOption.APPEND);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output_file;
    }

    private void fetchLocalMessage() {
        Message localMessage = getSingleMessage(incomingLocalQueueUrl);
        if (localMessage == null){
            return;
        }
        System.out.println("Local message Received");
        executor.submit(()->handleLocalMessage(localMessage));
    }

    private void handleLocalMessage(Message localMessage) {
        JSONParser parser = new JSONParser();
        JSONObject messageBody;
        try {
            messageBody = (JSONObject) parser.parse(localMessage.body());
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        // Receiving a message, find out which message (task/termination).
        isTerminate = messageBody.containsKey(TERMINATE);
        if (isTerminate){
            System.out.println("Terminate message Received");
            return;
        }

        // upon receiving a task message:

        String bucket = (String) messageBody.get(BUCKET);
        String input_file_name = (String) messageBody.get(INPUT);
        String output_file_name = (String) messageBody.get(OUTPUT);
        int num_reviews_per_worker = Math.toIntExact((long) messageBody.get(N));
        String localQName = (String) messageBody.get(QNAME);

        //get Qname's url
        localQName_to_QUrl.computeIfAbsent(localQName, key -> {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(localQName)
                    .build();
            return sqs.getQueueUrl(getQueueRequest).queueUrl();
        });

//      add it to maps
        outputFileName_to_localQName.put(output_file_name, localQName);
//      finish adding to maps

        deleteMessage(localMessage, incomingLocalQueueUrl);

//      o Download the input file from S3.
        Path tmpFilePath = Paths.get(MANAGER_TEMP_DIR, input_file_name);
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(input_file_name).build(),
                ResponseTransformer.toFile(tmpFilePath));

        System.out.println("File downloaded to: " + tmpFilePath);
        int total_reviews = 0;
        HashSet idsSet;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(tmpFilePath.toString()));
            System.out.println("Start reading file");
            String line = reader.readLine();
            while (line != null) {
                List<JSONObject> messages = new LinkedList<>();
                try {
                    JSONObject product = (JSONObject) parser.parse(line);
                    JSONArray reviewsArr = (JSONArray) product.get(REVIEWS);

                    // distribute reviews to workers through common queue
                    for (Object o : reviewsArr) {
                        JSONObject review = (JSONObject) o;
                        String reviewId = (String) review.get(ID);
                        if( (idsSet = fileReviews.get(output_file_name)) == null){
                            idsSet = new HashSet();
                        }
                        idsSet.add(reviewId);
                        fileReviews.put(output_file_name, idsSet);  // "put" replaces the current entry
                                                                    // of output_file_name if exists right?
                        JSONObject message = new JSONObject();
                        message.put(REVIEW, review);
                        message.put(OUTPUT, output_file_name); // added, so the worker will be able to send it back in the report
                                                               // and the manager will be able to match the report to the output_file.
                        messages.add(message);
                    }
                    sendWorkerMessages(messages);
                    total_reviews += messages.size();
                } catch (ParseException | NullPointerException e) {
                    e.printStackTrace();
                    System.out.println("Line read failed, continuing");
                }
                line = reader.readLine();
            }
            System.out.println("Done reading file");
        } catch (Exception e) {
            e.printStackTrace();
        }

//      o Check the SQS message count and starts Worker processes (nodes) accordingly.
//          ▪ The manager should create a worker for every n messages (as defined by the command-line argument),
//            if there are no running workers.
        // check for available worker count
        System.out.println("Number of reviews: " + total_reviews);
//        fileReviews.put(output_file_name, total_reviews); // no longer needed.
        addWorkersIfNeeded(num_reviews_per_worker, total_reviews);
    }

    private synchronized void addWorkersIfNeeded(int num_reviews_per_worker, int total_reviews) {
        int num_instances = describeWorkers().size();
        int required_instances = (int) Math.ceil(total_reviews / (double)num_reviews_per_worker);
        int missingInstances = required_instances - num_instances;
        missingInstances = Math.min(missingInstances, MAX_INSTANCES - num_instances); // Prevent overflow
        System.out.println("Missing instances: " + missingInstances);
        if (missingInstances > 0) { // TODO: test
            addWorkers(missingInstances);
        }
    }

    private List<String> describeWorkers() {
        DescribeInstancesRequest describeWorkersRequest = DescribeInstancesRequest.builder()
                .filters(Filter.builder().name(TAG_ROLE).values(WORKER).build())
                .build();

        DescribeInstancesResponse describeWorkers = ec2.describeInstances(describeWorkersRequest);
        List<Reservation> reservations = describeWorkers.reservations();
        List<String> instanceIds = new ArrayList<>();
        for (Reservation reservation :reservations) {
            for (Instance instance : reservation.instances()) {
                InstanceState instanceState = instance.state();
                if (instanceState.nameAsString().equalsIgnoreCase("running") |
                    instanceState.nameAsString().equalsIgnoreCase("pending")) {
                    instanceIds.add(instance.instanceId());
                }
            }
        }
        System.out.println("Active Workers: " + instanceIds.size());
        return instanceIds;
    }

    private String getSelfID() {
        DescribeInstancesRequest describeWorkersRequest = DescribeInstancesRequest.builder()
                .filters(Filter.builder().name(TAG_ROLE).values(MANAGER).build())
                .build();

        DescribeInstancesResponse describeWorkers = ec2.describeInstances(describeWorkersRequest);
        List<Reservation> reservations = describeWorkers.reservations();
        for (Reservation reservation :reservations) {
            for (Instance instance : reservation.instances()) {
                InstanceState instanceState = instance.state();
                if (instanceState.nameAsString().equalsIgnoreCase("running")) {
                    System.out.println("Id: " + instance.instanceId());
                    return instance.instanceId();
                }
            }
        }
        return null;
    }

    private void addWorkers(int missingInstances) {
//      ▪ If there are k active workers, and the new job requires m workers, then the manager should create
//        m-k new workers, if possible.
//      ▪ Note that while the manager creates a node for every n messages, it does not delegate messages to
//        specific nodes. All of the worker nodes take their messages from the same SQS queue; so it might be
//        the case that with 2n messages, hence two worker nodes, one node processed n+(n/2) messages, while
//        the other processed only n/2.

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(WORKER_ARN).build())
                .instanceType(InstanceType.T2_MEDIUM)
                .imageId(workerAmiId)
                .maxCount(missingInstances)
                .minCount(missingInstances)
                .keyName("DSP_key")
                .userData(Base64.getEncoder().encodeToString(WORKER_BOOTSTRAP_SCRIPT.getBytes()))
                .build();

        System.out.println("Requesting " + missingInstances + " instances");
        try {
            RunInstancesResponse response = ec2.runInstances(runRequest);
            List<Instance> instances = response.instances();
            System.out.println("Created " + instances.size() + " instances");
            // Tag worker. another field that saves the current number of reviews per worker,
            //             so we know if we need more workers.
            String[] instanceIds = instances.stream().map(Instance::instanceId).toArray(String[]::new);
            tagWorkers(instanceIds);
        } catch (AwsServiceException | SdkClientException e) {
            e.printStackTrace();
        }
    }

    private void tagWorkers(String[] instanceIds) {
        System.out.println("Tagging Workers");
        Tag tag = Tag.builder()
                .key(ROLE)
                .value(WORKER)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceIds)
                .tags(tag)
                .build();
        ec2.createTags(tagRequest);
    }
}
