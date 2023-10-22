package Part3A;

import java.io.IOException;

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

public class TaskA {

    private static double[][] KCentroids;

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

    public static class KMeansReducer extends Reducer<Text,Text,Text,Text> {

        private Text newCentroid = new Text();
        private boolean firstPass = true;
        private String centroidsWithNoPoints = "";

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String dataPoints = "Data Points: ";
            double Xsum = 0;
            double Ysum = 0;
            int count = 0;
            for (Text val : values) {
                dataPoints += "(" + val.toString() + ")\t";
                String[] csvLine = val.toString().split(",");
                Xsum += Double.parseDouble(csvLine[0]);
                Ysum += Double.parseDouble(csvLine[1]);
                count++;
            }
            // Print all centroids with no associated data points in the first line of the output
            if(firstPass) {
                for(int n = 0;n < KCentroids.length;n++) {
                    if(KCentroids[n][2] == 0) {
                        centroidsWithNoPoints += "Centroid with no data points: (" + KCentroids[n][0] + "," + KCentroids[n][1] + ")\n";
                    }
                }
                newCentroid.set(centroidsWithNoPoints + "New Centroid: (" + Xsum / count + "," + Ysum / count + ")");
                firstPass = false;
            } else {
                newCentroid.set("New Centroid: (" + Xsum / count + "," + Ysum / count + ")");
            }
            context.write(newCentroid, new Text(dataPoints));
        }
    }

    public void debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int KValue = 3; //test with 1, 10 and 100
        KCentroids = new double[KValue][3];
        // Maximum Lat and Long
        // lat     71.2727
        // lng    174.1110
        // Minimum Lat and Long
        // lat     17.9559
        // lng   -176.6295
        generateKCentroids(KValue, 71.2727, 17.9559, 174.1110, -176.6295);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskA");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(KMeansMapper.class);
//        job.setCombinerClass(KMeansReducer.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int i = job.waitForCompletion(true) ? 0 : 1;
        long endTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (endTime - startTime) + "ms");
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int KValue = 3; //test with 1, 10 and 100
        KCentroids = new double[KValue][3];
        // Maximum Lat and Long
        // lat     71.2727
        // lng    174.1110
        // Minimum Lat and Long
        // lat     17.9559
        // lng   -176.6295
        generateKCentroids(KValue, 71.2727, 17.9559, 174.1110, -176.6295);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskA");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(KMeansMapper.class);
//        job.setCombinerClass(KMeansReducer.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int i = job.waitForCompletion(true) ? 0 : 1;
        long endTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (endTime - startTime) + "ms");
    }
}