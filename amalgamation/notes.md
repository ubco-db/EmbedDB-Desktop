I think the spline needs to go first because EmbedDB.h includes Spline first and then uses it to make a struct 

## Order of Dependencies: 


![image](/Blank%20diagram.png "Dependency Diagram")

- Spline -> RadixSpline -> EmbedDB.h 
- schema.h -> advancedQueries <- EmbedDB.h


## Questions: 

What is the point of the #ifndef macro 




