import java.io.IOException;
import java.util.Random;
import java.util.regex.*;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Part2A {

    private static int[][] KCentroids;
    private static int globalCnt = 0;
    private static String[] allDataPoints = new String[3]; //3 centroids so 3 datasets, change this if K changes

    //Silhouette Code
    private static double calculateEuclideanDistance(String pointA_x, String pointA_y, String pointB_x, String pointB_y)
    {
        int xDiff = Integer.parseInt(pointA_x) - Integer.parseInt(pointB_x);
        int yDiff = Integer.parseInt(pointA_y) - Integer.parseInt(pointB_y);
        double distance = Math.sqrt((xDiff * xDiff) + (yDiff * yDiff));

        return distance;
    }

    private static double calculateMeanDistance(double totalDistance, int matchCount)
    {
        double meanDistance = (1 / (matchCount - 1)) * totalDistance;
        return meanDistance;
    }

    private static void calculateNeighborCentroid(String pointA_x, String pointY_x, int idx)
    {

    }

    private static void calculateSilhouetteValue(Text centroid, Text dataPoints)
    {
        boolean firstIteration = true;
        Pattern pattern = Pattern.compile("\\(\\d+,\\d+\\)"); //match every data point (123, 321)
        Matcher m = pattern.matcher(dataPoints.toString());
        String dataPointX = String.Empty;
        String dataPointY = String.Empty;
        double totalDistance = 0.0;
        int matchCount = 0;

        while (m.find())
        {
            String pair = m.group(); //will return (123, 321)
            String x = pair.split(",")[0].replace("(", "");
            String y = pair.split(",")[1].replace(")", "");
            if (firstIteration)
            {
                dataPointX = x; //fetches 123
                dataPointY = y; //fetches 321

                firstIteration = false;
            }

            else
            {
                double distance = calculateEuclideanDistance(x, y, dataPointX, dataPointY);
                totalDistance += distance;
            }
            matchCount++;
        }

        double meanDistance = calculateMeanDistance(totalDistance, matchCount);
        calculateNeighborCentroid(dataPointX, dataPointY, 0);
    }

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

    public static class KMeansReducer extends Reducer<Text,Text,Text,Text> {

        private Text newCentroid = new Text();
        private String dataPoints = "Data Points: ";

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int Xsum = 0;
            int Ysum = 0;
            int count = 0;
            for (Text val : values) {
                dataPoints += "(" + val.toString() + ")\t";
                String[] csvLine = val.toString().split(",");
                Xsum += Integer.parseInt(csvLine[0]);
                Ysum += Integer.parseInt(csvLine[1]);
                count++;
            }

            allDataPoints[globalCnt] = dataPoints;
            globalCnt++;

            newCentroid.set("New Centroid: (" + Xsum/count + "," + Ysum/count + ")");
            context.write(newCentroid, new Text(dataPoints));
        }
    }

    public void debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int KValue = 3; //test with 1, 10 and 100
        KCentroids = new int[KValue][2];
        generateKCentroids(KValue, 10000, 10000);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Part2A");
        job.setJarByClass(Part2A.class);
        job.setMapperClass(KMeansMapper.class);
//        job.setCombinerClass(KMeansReducer.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        long endTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (endTime - startTime) + "ms");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int KValue = 3;
        KCentroids = new int[KValue][2];
        generateKCentroids(KValue, 10000, 10000);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Part2A");
        job.setJarByClass(Part2A.class);
        job.setMapperClass(KMeansMapper.class);
//        job.setCombinerClass(KMeansReducer.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}