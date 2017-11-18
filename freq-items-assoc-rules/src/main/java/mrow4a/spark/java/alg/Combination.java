package mrow4a.spark.java.alg;

import scala.collection.mutable.ArrayBuffer;

import java.util.*;

public class Combination {

    private HashSet<String> validSubCombinations;
    private long requiredCombinationLength;

    public Combination(HashSet<String> validSubCombinations, long requiredCombinationLength) {
        this.validSubCombinations = validSubCombinations;
        this.requiredCombinationLength = requiredCombinationLength;
    }

    public List<List<String>> getSubArrays(List<String> input) {
        int lastElementIndex = input.size() - 1;
        List<List<String>> combinations = get(input, lastElementIndex);

        // Filter out only the ones which match the required combination length
        Vector<List<String>> filteredCombinations = new Vector<>();
        for (int i = 0; i < combinations.size(); i++) {
            List<String> currentCombination = combinations.get(i);

            // Accept only the ones with required length
            if (currentCombination.size() == this.requiredCombinationLength) {
                filteredCombinations.add(currentCombination);
            }
        }

        return filteredCombinations;
    }

    private List<List<String>> get(List<String> input, int elementIndex) {
        if (elementIndex == -1) {
            return new Vector<>();
        }

        String element = input.get(elementIndex);

        // Get combination for next element in input array
        List<List<String>> nextElementCombinations = get(input, elementIndex - 1);
        int nextElementCombinationsNumber = nextElementCombinations.size();

        for (int i = 0; i < nextElementCombinationsNumber; i++) {
            List<String> currentCombination = nextElementCombinations.get(i);
            int currentCombinationSize = currentCombination.size();

            // If this subcombination is direct combination for required, check if this one is not excluded
            if (currentCombinationSize == this.requiredCombinationLength - 1
                    && !this.validSubCombinations.contains(currentCombination.toString()) ) {
                continue;
            }

            // Accept only subcombinations (lower then requiredCombinationLength)
            if (currentCombinationSize < this.requiredCombinationLength) {
                // Extend this combination with the element
                List<String> newCombination = new ArrayList<>(currentCombination);
                newCombination.add(element);
                nextElementCombinations.add(newCombination);
            }
        }

        List<String> newCombination = new ArrayList<>();
        newCombination.add(element);
        nextElementCombinations.add(newCombination);

        return nextElementCombinations;
    }
}
