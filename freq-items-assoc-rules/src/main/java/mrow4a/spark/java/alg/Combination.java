package mrow4a.spark.java.alg;

import scala.collection.mutable.ArrayBuffer;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public final class Combination {

    public static List<List<String>> getSubArraysOfLength(List<String> input, long combinationLength) {
        List<List<String>> combinations = get(input, input.size() - 1, combinationLength);

        Vector<List<String>> filteredCombinations = new Vector<>();
        int combinationsSize = combinations.size();

        for (int i = 0; i < combinationsSize; i++) {
            List<String> currentCombination = combinations.get(i);
            int currentCombinationSize = currentCombination.size();
            if (currentCombinationSize == combinationLength) {
                filteredCombinations.add(currentCombination);
            }
        }

        return filteredCombinations;
    }

    private static List<List<String>> get(List<String> input, int index, long combinationLength) {
        if (index == -1) {
            return new Vector<>();
        }

        String next = input.get(index);
        List<List<String>> combinations = get(input, index - 1, combinationLength);
        int combinationsSize = combinations.size();

        for (int i = 0; i < combinationsSize; i++) {
            List<String> currentCombination = combinations.get(i);
            int currentCombinationSize = currentCombination.size();

            if (currentCombinationSize < combinationLength) {
                List<String> newCombination = new ArrayList<>(currentCombination);
                newCombination.add(next);

                combinations.add(newCombination);
            }
        }

        List<String> newCombination = new ArrayList<>();
        newCombination.add(next);
        combinations.add(newCombination);

        return combinations;
    }
}
