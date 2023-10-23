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

public class Part2D {

    private static int[][] KCentroids;

    private static void generateKCentroids(int K, int rangeX, int rangeY) {
        Random rand = new Random();
        for (int n = 0; n < K; n++) {
            KCentroids[n][0] = rand.nextInt(rangeX + 1);
            KCentroids[n][1] = rand.nextInt(rangeY + 1);
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
        int[] intDataPoint = { Integer.parseInt(dataPoint[0]), Integer.parseInt(dataPoint[1]) };
        double closestDistance = distanceFunction(KCentroids[0], intDataPoint);
        double currentDistance;
        if (KCentroids.length > 1) {
            for (int n = 1; n < KCentroids.length; n++) {
                currentDistance = distanceFunction(KCentroids[n], intDataPoint);
                if (currentDistance < closestDistance) {
                    closestDistance = currentDistance;
                    closestCentroid = n;
                }
            }
        }
        KCentroids[closestCentroid][2] = 1;
        return new Text(KCentroids[closestCentroid][0] + "," + KCentroids[closestCentroid][1]);
    }

    private static double distanceFunction(int[] dataPoint1, int[] dataPoint2) {
        return Math.sqrt(Math.pow(dataPoint1[0] - dataPoint2[0], 2) + Math.pow(dataPoint1[1] - dataPoint2[1], 2));
    }

    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int Xsum = 0;
            int Ysum = 0;
            int numDataPoint=0;
            String localdataPoints="";

            // Aggregate data points for the same centroid
            for (Text val : values) {
                String[] csvLine = val.toString().split(",");
                Xsum += Integer.parseInt(csvLine[0]);
                Ysum += Integer.parseInt(csvLine[1]);
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
        private String dataPoints = null;
        private boolean firstPass = true;
        private String centroidsWithNoPoints = "";
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numDataPoints = 0;
            int newX = 0;
            int newY = 0;
            String localDP="";

            for (Text val : values) {
                String[] csvLine = key.toString().split(",");
                int x = Integer.parseInt(csvLine[0]);
                int y = Integer.parseInt(csvLine[1]);
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
            context.write(newCentroid,new Text(dataPoints ));
            //  System.out.println(" #Dp: "+ numDataPoints);
            // Emit data points without aggregation
            for (Text val : values) {
                context.write(key, val);
            }

        }
    }

    public void debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int KValue = 10;
        KCentroids = new int[KValue][2];
        generateKCentroids(KValue, 10000, 10000);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Part2D");
        job.setJarByClass(Part2D.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class); // Use the combiner for centroids
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/KMeansDataset.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project2/Part2D.txt"));

        int i = job.waitForCompletion(true) ? 0 : 1;
        long endTime = System.currentTimeMillis(); // Record the end time
        long executionTime = endTime - startTime;
        System.out.println("Job execution time: " + executionTime + " ms");
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int KValue = 10;
        KCentroids = new int[KValue][2];
        generateKCentroids(KValue, 10000, 10000);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Part2D");
        job.setJarByClass(Part2D.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class); // Use the combiner for centroids
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/KMeansDataset.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project2/Part2D.txt"));

        int i = job.waitForCompletion(true) ? 0 : 1;
        long endTime = System.currentTimeMillis(); // Record the end time
        long executionTime = endTime - startTime;
        System.out.println("Job execution time: " + executionTime + " ms");
    }
}
