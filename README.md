# Data Mining, Scalable Machine Learning and Deep Learning projects
- Piotr Mrowczynski
- Jin Hu
- Riccardo Sibani 
- Filippo Boiani

## Contents

- Finding similar items
- Frequent Items assoc rules
- Spark Streaming Triangle Counting and Reservoi Sampling
- Linear and Logistic Regression Spark ML
- Neural Networks Tensorflow on Spark
- Decision Trees

### Finding similar items

Finding textually similar documents based on Jaccard similarity using the shingling,
minhashing, locality-sensitive hashing (LSH) techniques and corresponding algorithms.

Building (Docker required for maven single assembly):
```
chmod +x finding-similar-docs/build.sh 
chmod +x finding-similar-docs/run.sh 
./finding-similar-docs/build.sh 
```

Running basic version:
```
./finding-similar-docs/run.sh FindingSimilarDocsBasicJob 
```

Running MinHash version:
```
./finding-similar-docs/run.sh FindingSimilarDocsMinHashJob 
```

Results
```
Time: 3283 milliseconds
Found similar document pairs [5/4950] with similarity threshold [0.2] and shingle lenght [5] and
signature lenght [10]

Time: 18397 milliseconds
Found similar document pairs [422/1997001] with similarity threshold [0.2] and shingle lenght [5]
and signature lenght [10]
```

### Frequent Items assoc rules

Discovering association rules between itemsets in a sales transaction database (a set of baskets),
done by two steps: Finding frequent itemsets with support at least s, Generating association rules
with confidence at least c from the itemsets found in the first step.

Building (Docker required for maven single assembly)
```
chmod +x freq-items-assoc-rules/build.sh
chmod +x freq-items-assoc-rules/run.sh
freq-items-assoc-rules/build.sh
```

Running freq items detection and association generation
```
freq-items-assoc-rules/run.sh
```

Results
```
Time: 20749 milliseconds
Dataset: 100000 baskets of items
Association rules detected:
[704] -> [39] with confidence 0.617056856187291
[704] -> [825] with confidence 0.6142697881828316
[39, 704] -> [825] with confidence 0.9349593495934959
[704, 825] -> [39] with confidence 0.9392014519056261
[227] -> [390] with confidence 0.577007700770077
[39, 825] -> [704] with confidence 0.8719460825610783
Frequent Itemsets:
[39, 704, 825]: 1035
[227, 390]: 1049
[789, 829]: 1194
[39, 704]: 1107
[368, 682]: 1193
[217, 346]: 1336
[390, 722]: 1042
[39, 825]: 1187
[368, 829]: 1194
[704, 825]: 1102
[72]: 2852
[322]: 1154
....
```

### Spark Streaming Triangle Counting and Reservoi Sampling

Discovering association rules between itemsets in a sales transaction database (a set of baskets),
done by two steps: Finding frequent itemsets with support at least s, Generating association rules
with confidence at least c from the itemsets found in the first step.

Building (Docker required for maven single assembly)
```
chmod +x stream-graph-proc/build.sh
chmod +x stream-graph-proc/run.sh
stream-graph-proc/build.sh
```

Running
```
stream-graph-proc/run.sh
```

Results 
```
Dataset: 16,000,000 edges
Triangle count exact: 7,515,023

Results:

Memory limit: 2,000,000 edges
Current triangle count estimate: 7,594,782 at t=16,518,947

Memory limit: 1,000,000 edges
Current triangle count estimate: 7,626,910 at t=16,518,947

Memory limit: 100,000 edges
Current triangle count estimate: 9,015,504 at t=16,518,947
```

### Linear Regression on Spark ML

Pipeline descriptions and explanations in the code

### Neural Networks Tensorflow

TODO

### Wordcount

Use it as template for other apps

## Decision trees

### Assignment 0
All the 6 attributes have many values. Therefore we can have different configurations and a higher depth. 

We believe the most difficult problem to learn is the second one because we have different possibilities. 
(a1 = a2 = 1) or (a1 = a3 = 1) or (a1 = a4 = 1) etc...
### Assignment 1

Entropy of the datasets: 

Dataset | Entropy 
--- | --- 
**MONK-1** | 1 
**MONK-2** | 0.957117428265
**MONK-3** | 0.999806132805

The entropy is high since the distribution are binomial. 

```
python/entropy.py
```

### Assignment 2
*Explain entropy for a uniform distribution and a non-uniform distribution, present some example distributions with high and low entropy.*

Entropy is the unpredictability of one event.
An uniform distribution will have an high entropy, while an non-uniform distribution will have a lower entropy. 

No distribution has high or low entropy per se. In general, discrete distributions (like binomial, poisson, uniform etc...) tend to have high entropy when the events are equiprobable. 

When the probability of a particular event is far higher then the others, the uncertainty decreases accordingly. As an example, we can the take the overly-cited toss of a coin where the sides are equals. In this case the entropy is 0 because, no matter how we toss the coin, the result will be the same. Therefore, there is no uncertainty. Entropy is not an intrinsic property of a particular distribution, but instead, it depends on the parameters. 

Examples: 
- Die toss (uniform, high entropy)
- Fake die toss (non uniform, lower entropy)

**Reference**s: 
- [Type of distributions](http://people.stern.nyu.edu/adamodar/New_Home_Page/StatFile/statdistns.htm)
- [Entropy](https://en.wikipedia.org/wiki/Entropy_(information_theory)#Introduction)

### Assignment 3

Dataset | a1 | a2 | a3 | a4 | a5 | a6 
--- | --- | --- | --- | --- | --- | --- 
**MONK-1** | 0.0752725556083 | 0.00583842996291 | 0.0047075666173 | 0.0263116965077 | 0.287030749716 | 0.000757855715864
**MONK-2** | 0.00375617737751 | 0.00245849866608 | 0.00105614771589 | 0.0156642472926 | 0.0172771769379 | 0.00624762223688
**MONK-3** | 0.00712086839607 | 0.293736173508 | 0.000831114044534 | 0.00289181728865 | 0.25591172462 | 0.0070770260741

To grow the decision tree we generally choose the alternative that bears more information reducing the uncertainty (and therefore the entropy). Given this, for MONK-1 and MONK-2 we choose a5, even though the latter doesn't have much of a gain. For MONK-3 we go for a2 which is higher than a5. 

Now we can ask ourselves why a5. Well, a5 is the set with the highest cardinality, therefore any piece of information that contributes in making the probability uneven has an higher effect on the entropy. [ASSUMPTION: CHECK]

```
python/inform_gain.py
```

### Assignment 4

*For splitting we choose the attribute that maximizes the information gain, Eq.3. Looking at Eq.3 how does the entropy of the subsets, Sk, look like when the information gain is maximized? How can we motivate using the information gain as a heuristic for picking an attribute for splitting? Think about reduction in entropy after the split and what the entropy implies.*

In every step we select the attribute with the most information gain. Therefore, after having selected this the entropy is minimized. The entropy of Sk is the minimum.

### Assignment 5
Train and test set errors for the three Monk datasets for the full trees:

--- | E_train | E_test 
--- | --- | ---
**MONK-1** | 1.0 | 0.828703703704
**MONK-2** | 1.0 | 0.69212962963
**MONK-3** | 1.0 | 0.944444444444

The errors are computed using the predefined function check (Measure fraction of correctly classified samples) 

If we check the resulted full decision tree against the training dataset we will have no error, since the tree depends on the training set. 
From the result we can deduce the second dataset is the most difficult to train. 

```
python/build_dec_trees.py
```

Function build_dec_trees.py will give errors for training and test sets for different depths of the tree. It occurs that sometimes
depth does not need to be big to significatnly reduce variance, and in one case low depth improved variance in test set.

### Assignment 6  
*Explain pruning from a bias-variance tradeoff perspective.*

A complicated decision tree (e.g. deep) has **low bias and high variance**. The bias-variance tradeoff does depend on the depth of the tree.

If the number of levels is too high i.e a complicated decision tree, the model tends to overfit.

The tree then will have a great deal of condition to be satisfied.

Only if all the conditions are satisfied, a decision is reached. This will work very well for the training set as you are continuously

narrowing down on the data. The tree becomes highly tuned to the data present in the training set.

Decision tree is sensitive to where it splits and how it splits. Therefore, even small changes in input variable values might result in

very different tree structure. For example, when a new data point is fed, even if one of the parameters deviates slightly, the condition

will not be met and it will take the wrong branch.

By pruning we reduce the tree depth and the complexity of the model, which means the model doesn't overfit. The more we prune the higher

the bias becomes and the variance decreases.

(Bias refers to the error that is introduced by approximating a real-life problem, which may be extremely complicated, by a much simpler

model).

### Assignment 7

In this exercise had grown the full tree, and then post-prune (applied Reduced Error Pruning). This means, we split data
into training and validation sets.

We performed the complete pruning by repeatedly pruning the tree candidate into prunes. If the prune is better then candidate,
we perform further pruning on the prune to pick the tree which gives the best classification performance on the validation dataset.

Pruning is stopped when all the pruned trees perform worse than the current candidate.

This produces smallest version of most accurate subtree.

We also evaluated the effect pruning has on the test error for the monk1 and monk3 datasets.

We define the optimal partition into training and validation set.

![PLOT](https://github.com/filippoboiani/machine-learning/blob/master/dectrees/plot.png "Fraction and Errors")

Values | 0.3 | 0.4 | 0.5 | 0.6 | 0.7 | 0.8 
--- | --- | --- | --- | --- | --- | --- 
**MONK-1 Accuracy Mean** | 0.769398 | 0.796851 | 0.825578 | 0.8412037 | 0.849606 | 0.8605324
**MONK-3 Accuracy Mean** | 0.914212 | 0.939513 | 0.950995 | 0.9570833 | 0.955879 | 0.9540740
**MONK-1 Variance** | 0.001768237 | 0.001454166 | 0.002145209 | 0.0020444315 | 0.0021446818 | 0.002203936
**MONK-3 Variance** | 0.002567924 | 0.001636194 | 0.001057961 | 0.0011084769 | 0.0008425304 | 0.000803189

```
python/pruning.py
```
