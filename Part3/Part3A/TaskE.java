package Part3A;

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

public class TaskE {

    private static double[][] KCentroids;
    private static boolean finished = false;
    private static int KValue = 3;
    private static int R = 4;
    private static double threshold = 100;
    private static boolean withinThreshold = false;

    private static void generateKCentroids(int K, double upperX, double lowerX, double upperY, double lowerY) {
        for(int n = 0;n < K;n++) {
            KCentroids[n][0] = Math.random() * (upperX - lowerX) + lowerX;
            KCentroids[n][1] = Math.random() * (upperY - lowerY) + lowerY;
            KCentroids[n][2] = 0;
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
        double[] doubleDataPoint = {Double.parseDouble(dataPoint[0]),Double.parseDouble(dataPoint[1])};
        double closestDistance = distanceFunction(KCentroids[0],doubleDataPoint);
        double currentDistance;
        if (KCentroids.length > 1) {
            for (int n = 1; n < KCentroids.length; n++) {
                currentDistance = distanceFunction(KCentroids[n],doubleDataPoint);
                if (currentDistance < closestDistance) {
                    closestDistance = currentDistance;
                    closestCentroid = n;
                }
            }
        }
        KCentroids[closestCentroid][2] = 1;
        return new Text(KCentroids[closestCentroid][0] + "," + KCentroids[closestCentroid][1]);
    }

    private static double distanceFunction(double[] dataPoint1, double[] dataPoint2) {
        return Math.sqrt(Math.pow(dataPoint1[0] - dataPoint2[0],2) + Math.pow(dataPoint1[1] - dataPoint2[1],2));
    }
    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double Xsum = 0;
            double Ysum = 0;
            int numDataPoint=0;
            double oldX=0;
            double oldY=0;
            String localdataPoints="";

            // Aggregate data points for the same centroid
            for (Text val : values) {
                String[] csvLine = val.toString().split(",");
                String[] keyLine = key.toString().split(",");
                Xsum += Double.parseDouble(csvLine[0]);
                Ysum += Double.parseDouble(csvLine[1]);
                numDataPoint ++;
                localdataPoints += "(" + val.toString() + ")\t"; //fetch the datapoints for the centroid
                oldX = Double.parseDouble(keyLine[0]); // keep mapper centroid x, we will use it on the reducer, to compare it with the a new centroid x
                oldY = Double.parseDouble(keyLine[1]); // keep mapper centroid y, we will use it on the reducer, to compare it with the a new centroid y
            }
            Text localCentroid = new Text(Xsum + "," + Ysum + ","+numDataPoint+","+oldX+","+oldY); // partial sum of X, Y, number of data points, and non-aggregated mapper centroid values as an output
            context.write(localCentroid, new Text(localdataPoints));

        }
    }
    public static class KMeansReducer extends Reducer<Text,Text,Text,Text> {

        private Text newCentroid = new Text();
        private boolean firstPass = true;
        private String centroidsWithNoPoints = "";
        //private String dataPoints =null;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numDataPoints = 0;
            double oldX = Double.parseDouble(key.toString().split(",")[3]); //  centroid x from combiner but without aggregation which is same key value from mapper
            double oldY = Double.parseDouble(key.toString().split(",")[4]); //  centroid y from combiner but without aggregation which is same key value from mapper
            double newX = 0;
            double newY = 0;
            String dataPoints="Data Points: ";

            for (Text val : values) {
                String[] csvLine = key.toString().split(",");
                double x = Double.parseDouble(csvLine[0]); // partial sum of x from combiner
                double y = Double.parseDouble(csvLine[1]); // partial sum of y from combiner
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

            // Print all centroids with no associated data points in the first line of the output
            if(firstPass) {
                for(int n = 0;n < KCentroids.length;n++) {
                    if(KCentroids[n][2] == 0) {
                        centroidsWithNoPoints += "Centroid with no data points: (" + KCentroids[n][0] + "," + KCentroids[n][1] + ")\n";
                    }
                }
                newCentroid.set(centroidsWithNoPoints + "New Centroid: (" + newX + "," + newY + ")");
                firstPass = false;
            } else {
                newCentroid.set("New Centroid: (" + newX + "," + newY + ")");
            }

            // Check to see if the new centroid has moved a distance greater than the threshold from the old centroid
            if(distanceFunction(new double[]{oldX, oldY}, new double[]{newX,newY}) < threshold) {
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
        KCentroids = new double[KValue][3];
        generateKCentroids(KValue, 71.2727, 17.9559, 174.1110, -176.6295);
        //Here is where we perform a Haloop
        while (count < R)
        {
            if (count == (R - 1)) // last iteration set boolean to true
            {
                finished = true;
            }
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TaskE");
            job.setJarByClass(TaskE.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("uscities_LatAndLong.csv");
            FileOutputFormat.setOutputPath(job, new Path("output" + "_" + count));
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
        KCentroids = new double[KValue][3];
        generateKCentroids(KValue, 71.2727, 17.9559, 174.1110, -176.6295);
        //Here is where we perform a Haloop
        while (count < R)
        {
            if (count == (R - 1)) // last iteration set boolean to true
            {
                finished = true;
            }
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TaskE");
            job.setJarByClass(TaskE.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("uscities_LatAndLong.csv");
            FileOutputFormat.setOutputPath(job, new Path("output" + "_" + count));
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