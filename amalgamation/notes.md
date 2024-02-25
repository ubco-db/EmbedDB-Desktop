I think the spline needs to go first because EmbedDB.h includes Spline first and then uses it to make a struct 

## Order of Dependencies: 

- Spline -> RadixSpline -> EmbedDB.h 
- schema.h -> advancedQueries <- EmbedDB.h
- 


- Spline -> RadixSpline -> EmbedDB.h 

