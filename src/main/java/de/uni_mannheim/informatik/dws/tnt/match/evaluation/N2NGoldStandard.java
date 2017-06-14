/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.BigPerformance;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.P;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class N2NGoldStandard {

	// maps a cluster of correspondences (Set<String>) to its name (String)
	private Map<Set<String>, String> correspondenceClusters = new HashMap<>();
	
	/**
	 * @return the correspondenceClusters
	 */
	public Map<Set<String>, String> getCorrespondenceClusters() {
		return correspondenceClusters;
	}
	
	/**
	 * 
	 * Evaluates a clustering against the gold standard according to Vilain, Marc, et al. "A model-theoretic coreference scoring scheme." Proceedings of the 6th conference on Message understanding. Association for Computational Linguistics, 1995.
	 * 
	 * @param clustersToEvaluate
	 * @param verbose
	 */
	public void evaluateModelTheoretic(Map<Set<String>, String> clustersToEvaluate, ClusteringPerformance perf, boolean verbose) {
		
		// let S be an equivalence set (the gold standard)
		// let R1 ... Rm be equivalent classes generated by the response (the clustering)
		// p(S) is a partition of S relative to the response. Each subset of S in p(S) is formed by intersecting S and those response sets Ri that overlap S
		
		// map Si to recall
		Map<String, Double> recallPerClass = new HashMap<>();
		// map Si' to precision
		Map<String, Double> precisionPerClass = new HashMap<>();
		
		int numerator = 0; // SUM |Si| - |p(Si)|
		int denominator = 0; // SUM |Si| - 1
		
		// create partition for recall
		for(Set<String> gsCluster : correspondenceClusters.keySet()) {
			
			// Si = gsCluster
			Set<Set<String>> partition = new HashSet<>(); //p(Si) 
			HashSet<String> seenElements = new HashSet<>();
			
			for(Set<String> clu : clustersToEvaluate.keySet()) {
				
				Set<String> intersection = Q.intersection(gsCluster, clu);
				if(intersection.size()>0) {
					partition.add(intersection);
					seenElements.addAll(intersection);
				}
				
			}
			
			// create singleton sets in partition for all unmapped elements
			for(String elem : Q.without(gsCluster, seenElements)) {
				partition.add(Q.toSet(elem));
			}
		
			int gsSize = gsCluster.size(); // |Si|
			int partitionSize = partition.size(); // |p(Si)|
			
			recallPerClass.put(correspondenceClusters.get(gsCluster), (gsSize-partitionSize)/(double)(gsSize-1));
			
			numerator += gsSize - partitionSize;
			denominator += gsSize - 1;
		}
		
		double recall = numerator / (double)denominator;
		
		numerator = 0;
		denominator = 0;
		
		// create partition for precision
		for(Set<String> clu : clustersToEvaluate.keySet()) {
			
			// Si' = clu
			Set<Set<String>> partition = new HashSet<>(); // p'(Si')
			Set<String> seenElements = new HashSet<>();
			
			for(Set<String> gsCluster : correspondenceClusters.keySet()) {
				
				Set<String> intersection = Q.intersection(clu, gsCluster);
				if(intersection.size()>0) {
					
					partition.add(intersection);
					seenElements.addAll(intersection);
					
					if(verbose && intersection.size()<clu.size()) {
						System.err.println(String.format("[error] %s<->%s: %s", correspondenceClusters.get(gsCluster), clustersToEvaluate.get(clu), Q.without(clu, intersection)));
						System.err.println(String.format("\t\t%s", clu));
					}
				}
				
			}
			
			// create singleton sets in partition for all unmapped elements
			for(String elem : Q.without(clu, seenElements)) {
				partition.add(Q.toSet(elem));
			}
			
			int cluSize = clu.size(); // |Si'|
			int partitionSize = partition.size(); // |p'(Si')|
			
			precisionPerClass.put(clustersToEvaluate.get(clu), (cluSize-partitionSize)/(double)(cluSize-1));
			
			numerator += cluSize - partitionSize;
			denominator += cluSize - 1;
		}
		
		double precision = 0.0;
		if(denominator!=0) {
			precision = numerator / (double)denominator;
		} else if(numerator==0) {
			precision = 0.0;
		}
		
		perf.setClassRecall(recallPerClass);
		perf.setClusterPrecision(precisionPerClass);
		perf.setModelTheoreticPerformance(new IRPerformance(precision, recall));
	}
	
	public <T extends Matchable, U extends Matchable> Performance evaluateCorrespondencePerformance(Collection<Correspondence<T, U>> correspondences, boolean verbose) {
		
		int correct = 0;
		for(Correspondence<T, U> cor : correspondences) {
			
			if(Q.any(correspondenceClusters.keySet(), new P.ContainsAll<>(Q.toSet(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier())))) {
				correct++;
				if(verbose) {
					System.out.println(String.format("[correct] %s<->%s", cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));
				}
			} else {
				if(verbose) {
					System.out.println(String.format("[incorrect] %s<->%s", cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));
				}
			}
			
		}
		
		// determine number of pair-wise correspondences that can be generated from the gold standard
		BigDecimal totalCors = new BigDecimal(0);
		for(Set<String> clu : correspondenceClusters.keySet()) {
			
			// in each class from the gold standard, all possible links are the correspondences that can be generated by a pair-wise method
			// the number of edges in a fully connected graph with N nodes is (N*(N-1))/2
			
			BigDecimal pairs = new BigDecimal(clu.size()).multiply(new BigDecimal(clu.size()-1.0)).divide(new BigDecimal(2.0));
			
			totalCors = totalCors.add(pairs);
		}
		
		return new BigPerformance(new BigDecimal(correct), new BigDecimal(correspondences.size()), totalCors);
	}
	
	public <T extends Matchable, U extends Matchable>  Collection<Correspondence<T, U>> getCorrectCorrespondences(Collection<Correspondence<T, U>> correspondences) {
		ArrayList<Correspondence<T, U>> correct = new ArrayList<>();
		for(Correspondence<T, U> cor : correspondences) {
			if(isCorrectCorrespondence(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier())) {
				correct.add(cor);
			}
		}
		return correct;
	}
	
	public <T extends Matchable, U extends Matchable>  Collection<Correspondence<T, U>> getIncorrectCorrespondences(Collection<Correspondence<T, U>> correspondences) {
		ArrayList<Correspondence<T, U>> incorrect = new ArrayList<>();
		for(Correspondence<T, U> cor : correspondences) {
			if(!isCorrectCorrespondence(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier())) {
				incorrect.add(cor);
			}
		}
		return incorrect;
	}
	
	public boolean isCorrectCorrespondence(String element1, String element2) {
		
		for(Set<String> clu : correspondenceClusters.keySet()) {
			if(clu.contains(element1) && clu.contains(element2)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 
	 * Maps each cluster in the gold standard to the result cluster with the largest overlap to calculate precision and recall.
	 * 
	 * @param clustersToEvaluate
	 * @param verbose
	 * @return
	 */
	public ClusteringPerformance evaluateCorrespondenceClusters(Map<Set<String>, String> clustersToEvaluate, boolean verbose) {
		
		// the result, mapping a gs cluster name to the performance
		HashMap<String, Performance> result = new HashMap<>();
		
		// the clusters to evaluate
		Set<Set<String>> clusters = new HashSet<>(clustersToEvaluate.keySet()); 
		
		List<Set<String>> gsClusters = new ArrayList<>(correspondenceClusters.keySet());
		gsClusters = Q.sort(gsClusters, new Comparator<Set<String>>() {

			@Override
			public int compare(Set<String> o1, Set<String> o2) {
				return -Integer.compare(o1.size(), o2.size());
			}
		});
		
		// for each schema cluster in the gold standard
		for(Set<String> gsCluster : gsClusters) {
			
			int overlapSize = 0;
			Set<String> bestMatch = null;
			
			// find the cluster with the largest overlap
			for(Set<String> cluster : clusters) {
				
				int intersectionSize = Q.intersection(gsCluster, cluster).size();
				
				if(intersectionSize>0 && (bestMatch==null || intersectionSize>overlapSize)) {
					bestMatch = cluster;
					overlapSize = intersectionSize;
				}
				
			}
			
			int matchSize = bestMatch==null ? 0 : bestMatch.size();
			
			Performance perf = new Performance(overlapSize, matchSize, gsCluster.size());
//			String name = correspondenceClusters.get(gsCluster);
			String name = String.format("%s <-> %s", correspondenceClusters.get(gsCluster), clustersToEvaluate.get(bestMatch));
			
			result.put(name, perf);
			
			clusters.remove(bestMatch);
			
			if(verbose) {
				
//				if(bestMatch!=null) {
//					for(String error : Q.without(bestMatch, gsCluster)) {
//						System.err.println(String.format("[error] %s: %s", name, error));
//					}
//				} 
//				for(String error : Q.without(gsCluster, bestMatch)) {
//					System.out.println(String.format("[missing] %s: %s", name, error));
//				}
			}
		}
		
		// all remaining clusters must be wrong
		for(Set<String> cluster : clusters) {
			String name = String.format("[not existing] %s", clustersToEvaluate.get(cluster));
			Performance perf = new Performance(0, cluster.size(), 0); 
			
//			if(verbose) {
//				System.out.println(String.format("[not existing] %s", cluster));
//			}
			
			result.put(name, perf);
		}

		ClusteringPerformance p = new ClusteringPerformance(result);
		evaluateModelTheoretic(clustersToEvaluate, p, verbose);
		return p;
	}
	
	public String formatEvaluationResult(Map<String, Performance> result, boolean summaryOnly) {
		StringBuilder sb = new StringBuilder();
		
    	int correctSum = 0;
    	int predictedSum = 0;
    	int totalSum = 0;
    	
    	double pr = 0.0;
    	double rec = 0.0;

    	double clusteringPrecisionSum = 0.0;
    	int clusteringPrecisionCount = 0;
    	int gsClusters = 0;
    	
    	int totalCors = 0;
    	for(String key : result.keySet()) {
    		Performance p = result.get(key);
    		totalCors += p.getNumberOfCorrectTotal();
    	}
    	
    	for(String key : result.keySet()) {
    		Performance p = result.get(key);
    		
    		if(!summaryOnly) {
    			sb.append(String.format("%s:\tprec: %.4f\trec:%.4f\tf1:%.4f\t\t%d/%d correspondences\n", org.apache.commons.lang3.StringUtils.rightPad(key, 30), p.getPrecision(), p.getRecall(), p.getF1(), p.getNumberOfPredicted(), p.getNumberOfCorrectTotal()));
    		}
    		
    		if(p.getNumberOfPredicted()>1) {
    			correctSum += p.getNumberOfCorrectlyPredicted();
    		}
    		// the sum of all predicted and all correct will be the same in the end ... (as there is a prediction for every one and an entry in the gs for every one)
    		predictedSum += p.getNumberOfPredicted();
    		totalSum += p.getNumberOfCorrectTotal();
    		    		
    		// calculate weighted average
    		double weight = (p.getNumberOfCorrectTotal() / (double)totalCors);
    		rec += p.getRecall() * weight; 
    		pr += p.getPrecision() * weight;
    		
    		// calculate clustering precision (take into account the number of clusters)
    		if(p.getNumberOfPredicted()>1 && p.getNumberOfCorrectTotal()>0) {
	    		double numPairs = (p.getNumberOfCorrectlyPredicted() * (p.getNumberOfCorrectlyPredicted()-1))/2.0;
	    		double ttlPairs = (p.getNumberOfPredicted()*(p.getNumberOfPredicted()-1))/2.0;
	    		clusteringPrecisionSum += numPairs/ttlPairs;
	    		clusteringPrecisionCount++;
    		}
    		
    		if(p.getNumberOfCorrectTotal()>0) {
    			gsClusters++;
    		}
    	}
    	
    	Performance p = new Performance(correctSum, predictedSum, totalSum);
    	// accuracy?
    	sb.append("Overall Performance\n");
    	sb.append(String.format("\tcorrect: %d\n\tcreated: %d\n\ttotal: %d\n", p.getNumberOfCorrectlyPredicted(), p.getNumberOfPredicted(), p.getNumberOfCorrectTotal()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), p.getPrecision()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), p.getRecall()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), p.getF1()));
    	
    	double altF1 = (2 * pr * rec) / (pr + rec);
    	// this way of evaluation does not take incorrect clusters (which were not matched to the gold standard) into account
    	// these clusters will not influence precision, but only recall
    	sb.append("Weighted Average Performance over all ground truth clusters\n");
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), pr));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), rec));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), altF1));
    	
    	double clusteringPrecision = clusteringPrecisionSum / (double)clusteringPrecisionCount;
    	sb.append(String.format("Clustering Precision: %.4f\n", clusteringPrecision));
    	double penaltyFactor = 0.0;
    	if(result.size()<gsClusters) {
    		penaltyFactor = result.size()/(double)gsClusters;
    	} else {
    		penaltyFactor = gsClusters/(double)result.size();
    	}
    	double penalisedClusteringPrecision = penaltyFactor * clusteringPrecision;
    	sb.append(String.format("Penalised Clustering Precision: %.4f\n", penalisedClusteringPrecision));
    	
    	return sb.toString();
	}
	
	public void loadFromTSV(File file) throws IOException {
		BufferedReader r = new BufferedReader(new FileReader(file));
		String line = null;
		
		correspondenceClusters = new HashMap<>();
		
		while((line = r.readLine())!=null) {
			String[] values = line.split("\\t");
			
			if(values.length>1) { 
				Set<String> correspondences = new HashSet<>(Arrays.asList(values[1].split(",")));
				
				correspondenceClusters.put(correspondences, values[0]);
			}
		}
		
		r.close();
	}
	
	public void writeToTSV(File file) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(file));
		
		for(Set<String> clu : Q.sort(correspondenceClusters.keySet(), new Comparator<Set<String>>() {

			@Override
			public int compare(Set<String> o1, Set<String> o2) {
				return correspondenceClusters.get(o1).compareTo(correspondenceClusters.get(o2));
			}
		})) {
			
			w.write(String.format("%s\t%s\n", correspondenceClusters.get(clu), StringUtils.join(clu, ",")));
			
		}
		
		w.close();
	}
	
	public static <T1 extends Matchable, T2 extends Matchable> N2NGoldStandard createFromCorrespondences(Collection<Correspondence<T1, T2>> correspondences) {
		HashSet<String> nodes = new HashSet<>();
		ConnectedComponentClusterer<String> comp = new ConnectedComponentClusterer<>();
		
		for(Correspondence<T1, T2> cor : correspondences) {
			if(cor.getSimilarityScore()>0.0) {
				nodes.add(cor.getFirstRecord().getIdentifier());
				nodes.add(cor.getSecondRecord().getIdentifier());
				
				comp.addEdge(new Triple<String, String, Double>(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier(), cor.getSimilarityScore()));
			}
		}
		
		for(String node : nodes) {
			comp.addEdge(new Triple<String, String, Double>(node, node, 1.0));
		}
		
		Map<Collection<String>, String> clusters = comp.createResult();
		N2NGoldStandard n2n = new N2NGoldStandard();
		
		for(Collection<String> cluster : clusters.keySet()) {
			n2n.getCorrespondenceClusters().put(new HashSet<>(cluster), Q.firstOrDefault(cluster));
		}
		
		return n2n;
	}
	
	public void convertToCorrespondenceBasedGoldStandard(File outputFile) throws IOException {
		CSVWriter w = new CSVWriter(new FileWriter(outputFile));
		
		for(Set<String> clu : correspondenceClusters.keySet()) {
			String mapsTo = correspondenceClusters.get(clu);
			
			for(String element : clu) {
				// convert provenance string to id
				String[] parts = element.split(";");
				String id = String.format("%s~Col%s", parts[0], parts[1]);
				
				w.writeNext(new String[] { id, mapsTo, "true" });
			}
		}
		
		w.close();
	}
}
