## Question 1

**Pairs implementation**: For pairs implementation, I have taken 2 MapReduce jobs. First MapReduce is used to find the total number of unique words encountered and it's count. It emits the pair(WORD, ONE) for every unique words encountered. Second MapReduce handles the co-occurrence of pair of words and calculates the PMI and the number of times it has co-occurred.(word1, word2)->(PMI, no. of times pair occurs)

Input records: Each Mapper takes first 40 words of a sentence.

Intermediate key-value pairs: List of (Key, Value) pairs, where the Key is the unique word and the value is the count of that word.

Final Output Records: List of (Key, Value) pairs, where Key is the co-occurring pair (word1, word2) and the value is also a pair (PMI, co-occurrence count)

**Stripes implementation**: For Stripes implementation too, I have taken 2 MapReduce jobs. First MapReduce is used to find the total number of unique words encountered and it's count. It emits the pair(WORD, ONE) for every unique words encountered. Second MapReduce handles the co-occurrence of pair of words and calculates the PMI and the number of times it has co-occurred. (word1, word2)->(PMI, no. of times pair occurs) where word1 is a Key and work2 is a MAP,each key of MAP is the co-occurrence word, word2 and value of that key is (PMI, count) corresponding to that co-occurrence word.

Input records: Each Mapper takes first 40 words of a sentence.

Intermediate key-value pairs: List of (Key, Value) pairs, where the Key is the unique word and the value is the count of that word.

Final Output Records: List of (Key, Value) pairs, where Key is a word, say word1 and the value is a HashMap, where the Key of the HashMap is a co-occurring word, say word2 and the value is (PMI, co-occurrence count).

## Question 2
```
Running time of the complete pairs implementation: 21.392 s
Running time of the complete stripes implementation: 8.446 s

Environment: linux.student.cs.uwaterloo.ca
```
## Question 3
```
Running time of the complete pairs implementation(Without Combiner): 25.399 s
Running time of the complete stripes implementation(Withour Combiner): 8.401 s

Environment: linux.student.cs.uwaterloo.ca
```
## Question 4
```
Distinct PMI pairs (Using -threshold 10):
77198  308792 2327792
```
## Question 5
```
The pair(s) (x, y) with the highest PMI (Using -threshold 10): 
(maine, anjou)  (3.6331422, 12)
(anjou, maine)  (3.6331422, 12)

The pair(s) (x, y) with the lowest (negative) PMI (Using -threshold 10): 
(thy, you)      (-1.5303967, 11)
(you, thy)      (-1.5303967, 11)
```

The probability of seeing the word 'maine' having seen the word 'anjou' in the first 40 words of the same line is high, which gives them the higher PMI. In other words, the co-occurence of 'maine' and 'anjou' is relatively closer than the times they occured individually in the line. 

The reasoning for the lowest PMI of the pair (thy, you) goes the opposite. However, in both the cases of high PMI and low PMI, we can have some knowledge of the other word given one word has been seen.

## Question 6
```
Three words having the highest PMI with "tears" (using threshold 10):
(tears, shed)   (2.1117902, 15)
(tears, salt)   (2.052812, 11)
(tears, eyes)   (1.165167, 23)

Three words having the highest PMI with "death" (using threshold 10):
(death, father's)       (1.120252, 21)
(death, die)            (0.7541594, 18)
(death, life)           (0.7381346, 31)
```
## Question 7
```
Five words that have the highest PMI with "hockey" (using threshold 50):
(hockey, defenceman)    (2.4031334, 147)
(hockey, winger)        (2.3864822, 185)
(hockey, goaltender)    (2.2435493, 198)
(hockey, ice)           (2.1956174, 2002)
(hockey, nhl)           (2.2435493, 937)
```
## Question 8
```
Five words that have the highest PMI with "data" (using threshold 50):
(data, storage)         (1.9805377, 100)
(data, database)        (1.9001268, 97)
(data, disk)            (1.7944009, 67)
(data, stored)          (1.7885251, 65)
(data, processing)      (1.6491873, 57)
```
