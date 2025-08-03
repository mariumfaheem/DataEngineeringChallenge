## Assumption

- data is store in mongoDB NoSQL database
- I'm running these command to transfer data into mongoDB and in my script i will make assumption that imbalance_price_estimation and imbalance_price_final data source is mongoDB



```
docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/imbalance/imbalance_price_estimation_2025-06-07T041800Z.csv" de_mongodb:/tmp/imbalance_price_estimation_2025-06-07T041800Z.csv
  
docker exec -it de_mongodb bash -c 'tail -n +2 /tmp/imbalance_price_estimation_2025-06-07T041800Z.csv | tr ";" "\t" | mongoimport --db flexpower --collection imbalance_price_estimation --type tsv --headerline --username user --password password --authenticationDatabase admin'
  
 

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/imbalance/imbalance_price_final_20250608_170800.csv" de_mongodb:/tmp/imbalance_price_final_20250608_170800.csv
  
docker exec -it de_mongodb bash -c 'tail -n +2 /tmp/imbalance_price_final_20250608_170800.csv | tr ";" "\t" | mongoimport --db flexpower --collection imbalance_price_final --type tsv --headerline --username user --password password --authenticationDatabase admin'
  
  ```

