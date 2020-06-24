/*
 * beymani: Outlier and anamoly detection
 * Author: Pranab Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package application.model.predictor;

import application.constants.FraudDetectionConstants;
import application.util.Configuration;
import application.util.OsUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static application.constants.FraudDetectionConstants.DEFAULT_MODEL;

/**
 * Predictor based on markov model
 *
 * @author pranab
 */
public class MarkovModelPredictor extends ModelBasedPredictor {
	private static final Logger LOG = LoggerFactory.getLogger(MarkovModelPredictor.class);
	private MarkovModel markovModel;

	private Map<Integer, List<String>> records = new HashMap<>();
	private boolean localPredictor;
	private int stateSeqWindowSize;
	private int stateOrdinal;
	private DetectionAlgorithm detectionAlgorithm;
	private Map<Integer, Pair<Double, Double>> globalParams;
	private double metricThreshold;
	private int[] maxStateProbIndex;
	private double[] entropy;

	public MarkovModelPredictor(Configuration conf) {
		String OS_prefix = null;
		if (OsUtils.isWindows()) {
			OS_prefix = "win.";
		} else {
			OS_prefix = "unix.";
		}

		String mmKey = conf.getString(OS_prefix.concat(FraudDetectionConstants.Conf.MARKOV_MODEL_KEY), null);
		String model;

		if (StringUtils.isBlank(mmKey)) {
			model = new MarkovModelResourceSource().getModel(DEFAULT_MODEL);
		} else {
			model = new MarkovModelFileSource().getModel(mmKey);
		}

		markovModel = new MarkovModel(model);
		localPredictor = conf.getBoolean(FraudDetectionConstants.Conf.LOCAL_PREDICTOR);

		if (localPredictor) {
			stateSeqWindowSize = conf.getInt(FraudDetectionConstants.Conf.STATE_SEQ_WIN_SIZE);
			LOG.info("local predictor window fieldSize:" + stateSeqWindowSize);
		} else {
			stateSeqWindowSize = 5;
			globalParams = new HashMap<>();
		}

		//state value ordinal within record
		stateOrdinal = conf.getInt(FraudDetectionConstants.Conf.STATE_ORDINAL);

		//detection algoritm
		String algorithm = conf.getString(FraudDetectionConstants.Conf.DETECTION_ALGO);
		LOG.info("detection algorithm:" + algorithm);

		if (algorithm.equals("missProbability")) {
			detectionAlgorithm = DetectionAlgorithm.MissProbability;
		} else if (algorithm.equals("missRate")) {
			detectionAlgorithm = DetectionAlgorithm.MissRate;

			//max probability state index
			maxStateProbIndex = new int[markovModel.getNumStates()];
			for (int i = 0; i < markovModel.getNumStates(); ++i) {
				int maxProbIndex = -1;
				double maxProb = -1;
				for (int j = 0; j < markovModel.getNumStates(); ++j) {
					if (markovModel.getStateTransitionProb()[i][j] > maxProb) {
						maxProb = markovModel.getStateTransitionProb()[i][j];
						maxProbIndex = j;
					}
				}
				maxStateProbIndex[i] = maxProbIndex;
			}
		} else if (algorithm.equals("entropyReduction")) {
			detectionAlgorithm = DetectionAlgorithm.EntropyReduction;

			//entropy per source state
			entropy = new double[markovModel.getNumStates()];
			for (int i = 0; i < markovModel.getNumStates(); ++i) {
				double ent = 0;
				for (int j = 0; j < markovModel.getNumStates(); ++j) {
					ent += -markovModel.getStateTransitionProb()[i][j] * Math.log(markovModel.getStateTransitionProb()[i][j]);
				}
				entropy[i] = ent;
			}
		} else {
			//error
			String msg = "The detection algorithm '" + algorithm + "' does not exist";
			LOG.error(msg);
			throw new RuntimeException(msg);
		}

		//metric threshold
		metricThreshold = conf.getDouble(FraudDetectionConstants.Conf.METRIC_THRESHOLD);
	}

	@Override
	public Prediction execute(char[] _entityID, char[] _record) {
		double score = 0;

		int entityID = Arrays.hashCode(_entityID);
		String record = new String(_record);

		List<String> recordSeq = records.get(entityID);
		if (null == recordSeq) {
			recordSeq = new ArrayList<>();
			records.put(entityID, recordSeq);
		}

		//add and maintain size
		recordSeq.add(record);
		if (recordSeq.size() > stateSeqWindowSize) {
			recordSeq.remove(0);
		}

		String[] stateSeq = null;
		if (localPredictor) {
			//local metric
			//LOG.DEBUG("local metric,  seq size " + recordSeq.size());

			if (recordSeq.size() == stateSeqWindowSize) {
				stateSeq = new String[stateSeqWindowSize];
				for (int i = 0; i < stateSeqWindowSize; ++i) {
					stateSeq[i] = recordSeq.get(i).split(",")[stateOrdinal];
				}
				score = getLocalMetric(stateSeq);
			}
		} else {
			//global metric
			//LOG.DEBUG("global metric");

			if (recordSeq.size() >= 2) {
				stateSeq = new String[2];

				for (int i = stateSeqWindowSize - 2, j = 0; i < stateSeqWindowSize; ++i) {
					stateSeq[j++] = recordSeq.get(i).split("\t")[stateOrdinal];
				}

				Pair<Double, Double> params = globalParams.get(entityID);

				if (null == params) {
					params = new Pair<>(0.0, 0.0);
					globalParams.put(entityID, params);
				}

				score = getGlobalMetric(stateSeq, params);
			}
		}

		//outlier
		//if(score!=0)
		//LOG.DEBUG("metric  " + entityID + ":" + score);

		Prediction prediction = new Prediction(_entityID, score, stateSeq, (score > metricThreshold));//(..,..,..,isoutlier)

		if (score > metricThreshold) {
            /*
            StringBuilder stBld = new StringBuilder(entityID);
            stBld.append(" : ");
            for (String st : stateSeq) {
                stBld.append(st).append(" ");
            }
            stBld.append(": ");
            stBld.append(score);
            jedis.lpush(outputQueue,  stBld.toString());
            */
			// should return the score and state sequence
			// should say if is an outlier or not
		}

		return prediction;
	}

	@Override
	public Prediction execute(String entityID, String record) {
		double score = 0;


		List<String> recordSeq = records.get(entityID.hashCode());
		if (null == recordSeq) {
			recordSeq = new ArrayList<>();
			records.put(entityID.hashCode(), recordSeq);
		}

		//add and maintain size
		recordSeq.add(record);
		if (recordSeq.size() > stateSeqWindowSize) {
			recordSeq.remove(0);
		}

		String[] stateSeq = null;
		if (localPredictor) {
			//local metric
			//LOG.DEBUG("local metric,  seq size " + recordSeq.size());

			if (recordSeq.size() == stateSeqWindowSize) {
				stateSeq = new String[stateSeqWindowSize];
				for (int i = 0; i < stateSeqWindowSize; ++i) {
					stateSeq[i] = recordSeq.get(i).split(",")[stateOrdinal];
				}
				score = getLocalMetric(stateSeq);
			}
		} else {
			//global metric
			//LOG.DEBUG("global metric");

			if (recordSeq.size() >= 2) {
				stateSeq = new String[2];

				for (int i = stateSeqWindowSize - 2, j = 0; i < stateSeqWindowSize; ++i) {
					stateSeq[j++] = recordSeq.get(i).split("\t")[stateOrdinal];
				}

				Pair<Double, Double> params = globalParams.get(entityID.hashCode());

				if (null == params) {
					params = new Pair<>(0.0, 0.0);
					globalParams.put(entityID.hashCode(), params);
				}

				score = getGlobalMetric(stateSeq, params);
			}
		}

		//outlier
		//if(score!=0)
		//LOG.DEBUG("metric  " + entityID + ":" + score);

		Prediction prediction = new Prediction(entityID.toCharArray(), score, stateSeq, (score > metricThreshold));//(..,..,..,isoutlier)

		if (score > metricThreshold) {
            /*
            StringBuilder stBld = new StringBuilder(entityID);
            stBld.append(" : ");
            for (String st : stateSeq) {
                stBld.append(st).append(" ");
            }
            stBld.append(": ");
            stBld.append(score);
            jedis.lpush(outputQueue,  stBld.toString());
            */
			// should return the score and state sequence
			// should say if is an outlier or not
		}

		return prediction;
	}

	/**
	 * @param stateSeq
	 * @return
	 */
	private double getLocalMetric(String[] stateSeq) {
		double metric = 0;
		double[] params = new double[2];
		params[0] = params[1] = 0;

		if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
			missProbability(stateSeq, params);
		} else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
			missRate(stateSeq, params);
		} else {
			entropyReduction(stateSeq, params);
		}

		metric = params[0] / params[1];
		return metric;
	}

	/**
	 * @param stateSeq
	 * @return
	 */
	private double getGlobalMetric(String[] stateSeq, Pair<Double, Double> globParams) {
		double metric = 0;
		double[] params = new double[2];
		params[0] = params[1] = 0;

		if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
			missProbability(stateSeq, params);
		} else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
			missRate(stateSeq, params);
		} else {
			entropyReduction(stateSeq, params);
		}

		globParams.setLeft(globParams.getLeft() + params[0]);
		globParams.setRight(globParams.getRight() + params[1]);
		metric = globParams.getLeft() / globParams.getRight();
		return metric;
	}

	/**
	 * @param stateSeq
	 * @return
	 */
	private void missProbability(String[] stateSeq, double[] params) {
		int length = stateSeq.length;
		int start = localPredictor ? 1 : length - 1;
		for (int i = start; i < length; ++i) {
			int prState = markovModel.getStates().indexOf(stateSeq[i - 1]);
			int cuState = markovModel.getStates().indexOf(stateSeq[i]);

			//LOG.DEBUG("state prob index:" + prState + " " + cuState);

			//add all probability except target state
			for (int j = 0; j < markovModel.getStates().size(); ++j) {
				if (j != cuState) {
					params[0] += markovModel.getStateTransitionProb()[prState][j];
				}
			}
			params[1] += 1;
		}

		//LOG.DEBUG("params:" + params[0] + ":" + params[1]);
	}

	/**
	 * @param stateSeq
	 * @return
	 */
	private void missRate(String[] stateSeq, double[] params) {
		int start = localPredictor ? 1 : stateSeq.length - 1;
		for (int i = start; i < stateSeq.length; ++i) {
			int prState = markovModel.getStates().indexOf(stateSeq[i - 1]);
			int cuState = markovModel.getStates().indexOf(stateSeq[i]);
			params[0] += (cuState == maxStateProbIndex[prState] ? 0 : 1);
			params[1] += 1;
		}
	}

	/**
	 * @param stateSeq
	 * @return
	 */
	private void entropyReduction(String[] stateSeq, double[] params) {
		int start = localPredictor ? 1 : stateSeq.length - 1;
		for (int i = start; i < stateSeq.length; ++i) {
			int prState = markovModel.getStates().indexOf(stateSeq[i - 1]);
			int cuState = markovModel.getStates().indexOf(stateSeq[i]);
			params[0] += (cuState == maxStateProbIndex[prState] ? 0 : 1);
			params[1] += 1;
		}
	}

	private enum DetectionAlgorithm {
		MissProbability,
		MissRate,
		EntropyReduction
	}
}
