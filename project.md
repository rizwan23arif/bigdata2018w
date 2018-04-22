## MovieLens 100K Dataset

Query : Give me the top 10 recommended movies for Star Wars (1977)
Note : Star Wars (1977) has a movieID of 50 in 100k dataset
```
spark-submit --class ca.uwaterloo.cs451.project.MovieRecommendation100k \
  target/assignments-1.0.jar 50
```

Answer : 
```
Top 10 recommended movies for Star Wars (1977)
Empire Strikes Back, The (1980) score: 0.9895522078385338       strength: 345
Return of the Jedi (1983)       score: 0.9857230861253026       strength: 480
Raiders of the Lost Ark (1981)  score: 0.981760098872619        strength: 380
20,000 Leagues Under the Sea (1954)     score: 0.9789385605497993       strength: 68
12 Angry Men (1957)     score: 0.9776576120448436       strength: 109
Close Shave, A (1995)   score: 0.9775948291054827       strength: 92
African Queen, The (1951)       score: 0.9764692222674887       strength: 138
Sting, The (1973)       score: 0.9751512937740359       strength: 204
Wrong Trousers, The (1993)      score: 0.9748681355460885       strength: 103
Wallace & Gromit: The Best of Aardman Animation (1996)  score: 0.9741816128302572       strength: 58
```

## MovieLens Latest Datasets - Small

Query : Give me the top 10 recommended movies for Star Wars (1977)
Note : Star Wars (1977) has a movieID of 260
```
spark-submit --class ca.uwaterloo.cs451.project.MovieRecommendationLatest \
  target/assignments-1.0.jar 260
```

Answer :
```
Top 10 recommended movies for Star Wars: Episode IV - A New Hope (1977)
Star Wars: Episode V - The Empire Strikes Back (1980)   score: 0.9896402714789055       strength: 203
Star Wars: Episode VI - Return of the Jedi (1983)       score: 0.9891531995341264       strength: 187
"Untouchables   score: 0.9844332350959465       strength: 63
Iron Man (2008) score: 0.9830376465559196       strength: 56
Raiders of the Lost Ark (Indiana Jones and the Raiders of the Lost Ark) (1981)  score: 0.9828753537963838       strength: 177
"Dark Knight    score: 0.9816392821664428       strength: 74
"Lord of the Rings: The Fellowship of the Ring  score: 0.9802403864565682       strength: 147
"Departed       score: 0.9787847893840399       strength: 52
Platoon (1986)  score: 0.9774131316382156       strength: 55
"Hunt for Red October   score: 0.9771664798038402       strength: 73
```

  
  
