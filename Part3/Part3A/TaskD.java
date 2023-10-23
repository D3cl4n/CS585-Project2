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

public class TaskD {

    private static double[][] KCentroids;

    private static void generateKCentroids(int K, double upperX, double lowerX, double upperY, double lowerY) {
        for(int n = 0;n < K;n++) {
            KCentroids[n][0] = Math.random() * (upperX - lowerX) + lowerX;
            KCentroids[n][1] = Math.random() * (upperY - lowerY) + lowerY;
            KCentroids[n][2] = 0;
        }
    }

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

        private Text centroid = new Text();
        private Text dataPoint = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] csvLine = value.toString().split(",");
            // Determine which centroid this data point is closest to
            centroid.set(determineClosestCentroid(csvLine));
            dataPoint.set(value);
            context.write(centroid, dataPoint);
            // System.out.println("centroid:"+centroid);
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
            String localdataPoints="";

            // Aggregate data points for the same centroid
            for (Text val : values) {
                String[] csvLine = val.toString().split(",");
                Xsum += Double.parseDouble(csvLine[0]);
                Ysum += Double.parseDouble(csvLine[1]);
                numDataPoint ++;
                localdataPoints += "(" + val.toString() + ")\t"; //fetch the datapoints for the centroid
            }
            // System.out.println("Combiner #Dp: "+ numDataPoint);
            Text localCentroid = new Text(Xsum + "," + Ysum + ","+numDataPoint);
            context.write(localCentroid, new Text(localdataPoints));
            //  System.out.println("localCentroid "+ localCentroid+"Combiner dataPoints: "+ localdataPoints);

        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        private Text newCentroid = new Text();
        private boolean firstPass = true;
        private String centroidsWithNoPoints = "";
        private String dataPoints =null;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numDataPoints = 0;
            double newX = 0;
            double newY = 0;
            String localDP="";

            for (Text val : values) {
                String[] csvLine = key.toString().split(",");
                double x = Double.parseDouble(csvLine[0]);
                double y = Double.parseDouble(csvLine[1]);
                int count = Integer.parseInt(csvLine[2]);
                localDP += "(" + val.toString() + ")\t";
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

            dataPoints = new String( "Data Points: "+localDP);
            context.write(newCentroid,new Text(dataPoints));
            //  System.out.println(" #Dp: "+ numDataPoints);
            // Emit data points without aggregation
//            for (Text val : values) {
//                context.write(key, val);
//            }

        }
    }

    public void debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int KValue = 10;
        KCentroids = new double[KValue][3];
        generateKCentroids(KValue, 71.2727, 17.9559, 174.1110, -176.6295);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class); // Use the combiner for centroids
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("uscities_LatAndLong.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        long endTime = System.currentTimeMillis(); // Record the end time
        long executionTime = endTime - startTime;
        System.out.println("Job execution time: " + executionTime + " ms");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int KValue = 10;
        KCentroids = new double[KValue][3];
        generateKCentroids(KValue, 71.2727, 17.9559, 174.1110, -176.6295);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class); // Use the combiner for centroids
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("uscities_LatAndLong.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        long endTime = System.currentTimeMillis(); // Record the end time
        long executionTime = endTime - startTime;
        System.out.println("Job execution time: " + executionTime + " ms");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}