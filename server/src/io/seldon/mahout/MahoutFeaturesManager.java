package io.seldon.mahout;

import io.seldon.recommendation.Recommendation;
import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.springframework.beans.factory.annotation.Autowired;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by smrutiranjans on 26/9/16.
 */
@Component
public class MahoutFeaturesManager extends ModelManager<DataModel> {
    private static Logger logger = Logger.getLogger(MahoutFeaturesManager.class.getName());
    private final ExternalResourceStreamer featuresFileHandler;
    public static final String LOC_PATTERN = "mahout";

    @Autowired
    public MahoutFeaturesManager(ExternalResourceStreamer featuresFileHandler,
                                      NewResourceNotifier notifier){
        super(notifier, Collections.singleton(LOC_PATTERN));
        this.featuresFileHandler = featuresFileHandler;
    }


    @Override
    protected DataModel loadModel(String location, String client) {
        logger.info("Reloading most popular items by dimension for client: "+ client);
        try {
            DataModel model = new FileDataModel(new File(location+"/model.csv"));
            return model;
        } catch (IOException e) {
            logger.error("Couldn't reloadFeatures for client "+ client, e);
        }
        return null;
    }
}
