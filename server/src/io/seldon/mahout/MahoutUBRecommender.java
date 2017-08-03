package io.seldon.mahout;

import com.google.common.collect.Ordering;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.similarity.*;

import java.io.*;
import java.util.*;


/**
 * Created by smrutiranjans on 26/9/16.
 */
@Component
public class MahoutUBRecommender implements ItemRecommendationAlgorithm {
    private static Logger logger = Logger.getLogger(io.seldon.mahout.MahoutUBRecommender.class.getName());
    private static final String name = io.seldon.mahout.MahoutUBRecommender.class.getSimpleName();
    private final MahoutFeaturesManager store;

    @Autowired
    public MahoutUBRecommender(MahoutFeaturesManager store){
        this.store = store;
    }

    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount,
                                                 RecommendationContext ctxt, List<Long> recentitemInteractions) {

        Set<ItemRecommendationResultSet.ItemRecommendationResult> recs = new HashSet<>();
        try {
            //DataModel model = new FileDataModel(new File("/seldon-data/seldon-models/ahalife/mahout/model.csv"));
            DataModel model = this.store.getClientStore(client, ctxt.getOptsHolder());

            if(model==null) {
                logger.debug("Couldn't find a user based store for this client");
                return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
            }

            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
            UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

            if(ctxt.getMode()== RecommendationContext.MODE.INCLUSION){
                // special case for INCLUSION as it's easier on the cpu.
                for (Long item : ctxt.getContextItems()){
                    if (!recentitemInteractions.contains(item))
                    {
                        recs.add(new ItemRecommendationResultSet.ItemRecommendationResult(item, recommender.estimatePreference(user, item)));
                    }
                }

            } else {
                List<RecommendedItem> recommendations = recommender.recommend(user, maxRecsCount);
                for (RecommendedItem recommendation : recommendations) {
                    recs.add(new ItemRecommendationResultSet.ItemRecommendationResult(recommendation.getItemID(),recommendation.getValue()));
                }
            }

        } catch (TasteException e) {
            logger.error(e.getMessage(), e);
        }

        List<ItemRecommendationResultSet.ItemRecommendationResult> recsList = Ordering.natural().greatestOf(recs, maxRecsCount);
        if (logger.isDebugEnabled())
            logger.debug("Created "+recsList.size() + " recs");
        return new ItemRecommendationResultSet(recsList, name);
    }


    @Override
    public String name() {
        return name;
    }
}
