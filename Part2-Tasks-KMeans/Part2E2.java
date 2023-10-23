import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// For K-Means, we'll use one mapper to read the list of data points we generate
// Before the mapper runs, we will generate the initial list of K centroids
// The mapper should output a (key,value) pair of (centroid coordinates, data point coordinates)
// The reducer will group the data point coordinates by using the (key,value) pair received from the mapper
// The reducer will then calculate the new centroid that should be at the center of the data points that were grouped with it

public class Part2E2 {

    private static int[][] KCentroids;
    private static boolean finished = false;
    private static int KValue = 3;
    private static int R = 4;
    private static double threshold = 100;
    private static boolean withinThreshold = false;

    private static void generateKCentroids(int K, int rangeX, int rangeY) {
        Random rand = new Random();
        for(int n = 0;n < K;n++) {
            KCentroids[n][0] = rand.nextInt(rangeX + 1);
            KCentroids[n][1] = rand.nextInt(rangeY + 1);
        }
    }

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text>{

        private Text centroid = new Text();
        private Text dataPoint = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] csvLine = value.toString().split(",");
            // Determine which centroid this data point is closest to
            centroid.set(determineClosestCentroid(csvLine));
            dataPoint.set(value);
            context.write(centroid,dataPoint);
        }
    }

    private static Text determineClosestCentroid(String[] dataPoint) {
        // Use Euclidean Distance to determine which centroid is the closest to this data point
        int closestCentroid = 0;
        int[] intDataPoint = {Integer.parseInt(dataPoint[0]),Integer.parseInt(dataPoint[1])};
        double closestDistance = distanceFunction(KCentroids[0],intDataPoint);
        double currentDistance;
        if (KCentroids.length > 1) {
            for (int n = 1; n < KCentroids.length; n++) {
                currentDistance = distanceFunction(KCentroids[n],intDataPoint);
                if (currentDistance < closestDistance) {
                    closestDistance = currentDistance;
                    closestCentroid = n;
                }
            }
        }
        return new Text(KCentroids[closestCentroid][0] + "," + KCentroids[closestCentroid][1]);
    }

    private static double distanceFunction(int[] dataPoint1, int[] dataPoint2) {
        return Math.sqrt(Math.pow(dataPoint1[0] - dataPoint2[0],2) + Math.pow(dataPoint1[1] - dataPoint2[1],2));
    }
    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int Xsum = 0;
            int Ysum = 0;
            int numDataPoint=0;
            int oldX=0;
            int oldY=0;
            String localdataPoints="";

            // Aggregate data points for the same centroid
            for (Text val : values) {
                String[] csvLine = val.toString().split(",");
                String[] keyLine = key.toString().split(",");
                Xsum += Integer.parseInt(csvLine[0]);
                Ysum += Integer.parseInt(csvLine[1]);
                numDataPoint ++;
                localdataPoints += "(" + val.toString() + ")\t"; //fetch the datapoints for the centroid
                oldX = Integer.parseInt(keyLine[0]); // keep mapper centroid x, we will use it on the reducer, to compare it with the a new centroid x
                oldY = Integer.parseInt(keyLine[1]); // keep mapper centroid y, we will use it on the reducer, to compare it with the a new centroid y
            }
            Text localCentroid = new Text(Xsum + "," + Ysum + ","+numDataPoint+","+oldX+","+oldY); // partial sum of X, Y, number of data points, and non-aggregated mapper centroid values as an output
            context.write(localCentroid, new Text(localdataPoints));

        }
    }
    public static class KMeansReducer extends Reducer<Text,Text,Text,Text> {

        private Text newCentroid = new Text();
        //private String dataPoints =null;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numDataPoints = 0;
            int oldX = Integer.parseInt(key.toString().split(",")[3]); //  centroid x from combiner but without aggregation which is same key value from mapper
            int oldY = Integer.parseInt(key.toString().split(",")[4]); //  centroid y from combiner but without aggregation which is same key value from mapper
            int newX = 0;
            int newY = 0;
            String dataPoints="Data Points: ";

            for (Text val : values) {
                String[] csvLine = key.toString().split(",");
                int x = Integer.parseInt(csvLine[0]); // partial sum of x from combiner
                int y = Integer.parseInt(csvLine[1]); // partial sum of y from combiner
                int count = Integer.parseInt(csvLine[2]); // number of data points from combiner
                dataPoints += "(" + val.toString() + ")\t";
                // Update the local sum for the centroid
                newX += x;
                newY += y;
                numDataPoints +=count;
            }

            if (numDataPoints > 0) {
                newX /= numDataPoints;
                newY /= numDataPoints;
            }

            newCentroid.set("New Centroid: (" + newX + "," + newY + ")"); //set new centroid
            // Check to see if the new centroid has moved a distance greater than the threshold from the old centroid
            if(distanceFunction(new int[]{oldX, oldY}, new int[]{newX,newY}) < threshold) {
                withinThreshold = true;
            } else {
                withinThreshold = false;
            }

            if (finished) {
                context.write(newCentroid, new Text(dataPoints));
            }
            else {
                for (int i =0; i<KCentroids.length; i++) {
                    if (oldX == KCentroids[i][0] && oldY == KCentroids[i][1]) {
                        KCentroids[i][0] = newX;
                        KCentroids[i][1] = newY;
                    }
                }
                context.write(newCentroid, new Text(dataPoints));
            }
        }
    }

    public void debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int count = 0;
        KCentroids = new int[KValue][2];
        generateKCentroids(KValue, 10000, 10000);
        //Here is where we perform a Haloop
        while (count < R)
        {
            if (count == (R - 1)) // last iteration set boolean to true
            {
                finished = true;
            }
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Part2E2");
            job.setJarByClass(Part2E2.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/KMeansDataset.csv"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project2/Part2E2test.txt" + "_" + count));
            int i = job.waitForCompletion(true) ? 0 : 1;
            if(withinThreshold) {
                System.out.println("Algorithm terminated due to convergence after iteration " + count + ".");
                break;
            }
            count++;
            System.out.println("RValue:" + R);
            System.out.println("Count:" + count);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (endTime - startTime) + "ms");
        //   System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int count = 0;
        KCentroids = new int[KValue][2];
        generateKCentroids(KValue, 10000, 10000);
        //Here is where we perform a Haloop
        while (count < R)
        {
            if (count == (R - 1)) // last iteration set boolean to true
            {
                finished = true;
            }
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Part2E2");
            job.setJarByClass(Part2E2.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/KMeansDataset.csv"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project2/Part2E2test.txt" + "_" + count));
            int i = job.waitForCompletion(true) ? 0 : 1;
            if(withinThreshold) {
                System.out.println("Algorithm terminated due to convergence after iteration " + count + ".");
                break;
            }
            count++;
            System.out.println("RValue:" + R);
            System.out.println("Count:" + count);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (endTime - startTime) + "ms");
    }
}
