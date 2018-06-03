import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;


public class AverageRating {

    public static JavaSparkContext jsc;

    public AverageRating(JavaSparkContext jsc){
        this.jsc = jsc;
    }

    /**
     * Function to Filter the first line from CSV file.
     * @param rddWhichNeedsToBeFiltered
     * @return
     */
    public static JavaRDD<String> filterHeaderFromRDD(JavaRDD<String> rddWhichNeedsToBeFiltered ){
        String header = rddWhichNeedsToBeFiltered.first();
        return rddWhichNeedsToBeFiltered.filter(row -> !row.equalsIgnoreCase(header));

    }


    public static JavaRDD<Tuple3<String, Double, Integer>> run(String movies, String reviews){


        //Read the CSV files into RDD's
        JavaRDD<String> moviesRdd = jsc.textFile(movies);
        JavaRDD<String> reviewsRdd = jsc.textFile(reviews);

        //Remove the header from the RDD's before any transformation
        moviesRdd = filterHeaderFromRDD(moviesRdd);
        reviewsRdd = filterHeaderFromRDD(reviewsRdd);

        /*Split the lines based on comma operator and create a JavaPair rdd to store the movieid and movie name content
        as (1 ToyStory)*/
        JavaPairRDD<Integer, String> moviesPairRdd = moviesRdd.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String str) throws Exception {
                String[] data = str.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
                return new Tuple2<Integer, String>(Integer.parseInt(data[0]), data[1]);
            }
        });

        /*Split the lines based on comma operator and create a JavaPair rdd to store the movieid and review content
        as (31 2.5)*/
        JavaPairRDD<Integer, Double> reviewPairRdd = reviewsRdd.mapToPair(new PairFunction<String, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(String str) throws Exception {
                String[] data = str.split(",", -1);
                return new Tuple2<Integer, Double>(Integer.parseInt(data[1]), Double.parseDouble(data[2]));
            }
        });

        //Functions to Use CombineKey to find the average Movie Rating.

        // function 1:
        Function<Double, AvgCount> createAcc = (Double x) -> new AvgCount(x, 1);

        // function 2
        Function2<AvgCount, Double, AvgCount> addAndCount = (AvgCount a, Double x) -> {
            a.totalAddedReviews += x;
            a.popularity += 1;
            return a;
        };

        // function 3
        Function2<AvgCount, AvgCount, AvgCount> combine = (AvgCount a, AvgCount b) -> {
            a.totalAddedReviews += b.totalAddedReviews;
            a.popularity += b.popularity;
            return a;
        };

        // now that we have defined 3 functions, we can use combineByKey() to calculate the average rating
        JavaPairRDD<Integer, AvgCount> avgCounts = reviewPairRdd.combineByKey(
                createAcc,
                addAndCount,
                combine).filter(integerAvgCountTuple2 -> integerAvgCountTuple2._2().avg() > 4.0).
                filter(integerAvgCountTuple2 -> integerAvgCountTuple2._2().popularity > 10);

        //Join the avgCount RDD which looks like (movieId AvgCount(Object)) with moviesPairRDD which looks like (movieId movieName)
        JavaPairRDD<Integer, Tuple2<String,AvgCount>> joinedRDD = moviesPairRdd.join(avgCounts);

        //You don't need the MovieId, so modify the schema to remove the movieId and get that in a new pairRDD
        JavaPairRDD<String, AvgCount> modifiedSchema = modifyRDDSchema(joinedRDD);

        //Now since the  output requires three rows, get that into a RDD with Tuple3 dataType
        JavaRDD<Tuple3<String, Double, Integer>> rddSchema = modifiedSchema.map(stringAvgCountTuple3 -> new Tuple3<>(stringAvgCountTuple3._1,stringAvgCountTuple3._2().avg(),stringAvgCountTuple3._2().popularity));

        //Now sort by Average rating
        JavaRDD<Tuple3<String, Double, Integer>> outputSchema = rddSchema.sortBy(stringIntegerDoubleTuple3 -> stringIntegerDoubleTuple3._2(),true,1);

        return outputSchema;
    }


    /**
     * Function to modify the schema of the JavaPairRDD.
     * @param innerJoinRDD
     * @return
     */
    public static JavaPairRDD<String, AvgCount> modifyRDDSchema(JavaPairRDD<Integer, Tuple2<String, AvgCount>> innerJoinRDD){

        JavaPairRDD<String, AvgCount> modifiedRDD = innerJoinRDD.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<String, AvgCount>>, String, AvgCount>() {
            @Override
            public Tuple2<String, AvgCount> call(Tuple2<Integer, Tuple2<String, AvgCount>> integerTuple2Tuple2) throws Exception {
                return new Tuple2(integerTuple2Tuple2._2._1, integerTuple2Tuple2._2._2);
            }
        });

        return modifiedRDD;

    }


    /**
     * This is the entry point of the spark job, Spark context is initialized and closed here.
     * @param args
     * args[0], args[1] are the input csv files for the job
     * args[2] refers to the directory where the output will be stored.
     */

    public static void main(String args[]) {

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        AverageRating ar = new AverageRating(jsc);
        JavaRDD<Tuple3<String, Double, Integer>> output = ar.run(args[0],args[1]);
        output.saveAsTextFile(args[2]);
        jsc.close();

    }

}
