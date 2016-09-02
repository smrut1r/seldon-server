/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.recommendation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class LastRecommendationBean implements Serializable {

	String algorithm;
	Map<Long, Double> recs;
	public LastRecommendationBean(String algorithm, Map<Long, Double> recs) {
		super();
		this.algorithm = algorithm;
		this.recs = recs;
	}
	public String getAlgorithm() {
		return algorithm;
	}
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	public Map<Long, Double> getRecs() {
		return recs;
	}
	public void setRecs(Map<Long, Double> recs) {
		this.recs = recs;
	}
	
	

}
