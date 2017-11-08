package mrow4a.spark.java.alg;

import java.util.Random;

public class HashFunction {

    private Random randomGenerator = new Random();
    private int a;
    private int b;
    private int prime;

    public HashFunction() {
        this.a = getRandom();
        this.b = getRandom();
        this.prime = 5;
    }

    public Integer getHash(int x) {
        return (a*x+b)%prime;
    }

    private Integer getRandom() {
        int randomInt = randomGenerator.nextInt();
        while (randomInt == 0) {
            randomInt = randomGenerator.nextInt();
        }

        return randomInt;
    }
}
