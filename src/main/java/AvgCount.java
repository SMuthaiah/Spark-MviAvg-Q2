import scala.Int;

import java.io.Serializable;


public class AvgCount implements Serializable {

    int popularity;
    double totalAddedReviews;

    public AvgCount(double totalAddedReviews, int popularity){
        this.popularity = popularity;
        this.totalAddedReviews = totalAddedReviews;
    }

    public double avg(){
        return totalAddedReviews/(double)popularity;
    }

   /* @Override
    public int compareTo(AvgCount avgCount){
        if(this.avg()<avgCount.avg())
            return -1;
        else if(avgCount.avg()<this.avg())
            return 1;
        return 0;
    }*/


}
