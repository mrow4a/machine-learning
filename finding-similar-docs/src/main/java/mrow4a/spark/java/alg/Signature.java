package mrow4a.spark.java.alg;

import java.io.Serializable;
import java.util.Random;
import java.util.Vector;

class Signature implements Serializable {

    private final int signatureLength;
    private final int[] a;
    private final int[] b;
    private final int prime;

    public Signature(int signatureLength) {
        Random randomGenerator = new Random();
        this.a = new int[signatureLength];
        this.b = new int[signatureLength];
        for (int i = 0; i < signatureLength; i++) {
            this.a[i] = getRandom(randomGenerator);
            this.b[i] = getRandom(randomGenerator);
        }
        this.prime = 433494437;
        this.signatureLength = signatureLength;
    }

    public Vector<Integer> get(int[] shingles) {
        Vector<Integer> signature = new Vector<>();
        for (int i = 0; i < this.signatureLength; i++) {
            signature.add(this.getMinHash(shingles, i));
        }
        return signature;
    }

    private Integer getMinHash(int[] shingles, int hashFunIndex) {
        int min = this.getHash(shingles[0], 0);
        int current;
        for (int i = 0; i < shingles.length; i++) {
            current = this.getHash(shingles[i], hashFunIndex);
            if (current < min) {
                min = current;
            }
        }

        return min;
    }

    private Integer getHash(int x, int hashFunIndex) {
        return (this.a[hashFunIndex] * x + this.b[hashFunIndex]) % this.prime;
    }

    private Integer getRandom(Random randomGenerator) {
        int randomInt = randomGenerator.nextInt();
        while (randomInt == 0) {
            randomInt = randomGenerator.nextInt();
        }

        return randomInt;
    }
}
