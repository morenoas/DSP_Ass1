import java.util.Arrays;

public class LocalMain {
    public static final int FIRST = 0;
    private static final String TERMINATE = "terminate";

    public static void main(String[] args) {
        final String USAGE = // TODO
                "To run this example, supply a list of input and output files, a number of reviews per worker and a termination command\n" +
                        "Ex: LocalMain <input_file_1 [, input_file_2, ...]> <output_file_1 [, output_file_2, ...]> <num_reviews_per_worker> [<terminate>]\n";


        // Argument parsing
        if (args.length < 2) {
            System.out.println(USAGE);
            System.exit(1);
        }

        boolean terminate = false;
        String last = args[args.length-1];
        if (last.equals(TERMINATE)){
            terminate = true;
            last = args[args.length-2];
        }
        int num_reviews_per_worker = Integer.parseInt(last);
        int numFiles = (args.length - 1) / 2; // Handles even or odd num of args (with or without terminate)
        String[] inputs = Arrays.copyOfRange(args, FIRST, numFiles);
        String[] outputs = Arrays.copyOfRange(args, numFiles, numFiles*2);
        LocalApp localApp = new LocalApp();
        localApp.init(inputs, outputs, num_reviews_per_worker);
        localApp.run(numFiles, terminate);
    }
}
