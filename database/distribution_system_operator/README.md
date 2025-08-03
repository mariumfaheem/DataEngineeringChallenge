## Assumption

- data is store in mongoDB NoSQL database
- I'm running these command to transfer data into mongoDB and in my script i will make assumption that assets_contracts data source is mongoDB


### Final Production data



```

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/distribution_system_operator/final_production_MP-SOL-001_20250609.csv" de_mongodb:/tmp/final_production_MP-SOL-001_20250609.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection final_production \
  --type csv \
  --headerline \
  --file /tmp/final_production_MP-SOL-001_20250609.csv \
  --username user \
  --password password \
  --authenticationDatabase admin
  

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/distribution_system_operator/final_production_MP-SOL-002_20250609.csv" de_mongodb:/tmp/final_production_MP-SOL-002_20250609.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection final_production \
  --type csv \
  --headerline \
  --file /tmp/final_production_MP-SOL-002_20250609.csv \
  --username user \
  --password password \
  --authenticationDatabase admin
  

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/distribution_system_operator/final_production_MP-WND-002_20250609.csv" de_mongodb:/tmp/final_production_MP-WND-002_20250609.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection final_production \
  --type csv \
  --headerline \
  --file /tmp/final_production_MP-WND-002_20250609.csv \
  --username user \
  --password password \
  --authenticationDatabase admin
  

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/distribution_system_operator/final_production_MP-WND-003_20250609.csv" de_mongodb:/tmp/final_production_MP-WND-003_20250609.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection final_production \
  --type csv \
  --headerline \
  --file /tmp/final_production_MP-WND-003_20250609.csv \
  --username user \
  --password password \
  --authenticationDatabase admin
  
  ```

### redispatch_compensation


```

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/distribution_system_operator/redispatch_compensation_2025-06-20.csv" de_mongodb:/tmp/redispatch_compensation_2025-06-20.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection redispatch_compensation \
  --type csv \
  --headerline \
  --file /tmp/redispatch_compensation_2025-06-20.csv \
  --username user \
  --password password \
  --authenticationDatabase admin
  

```


### redispatch_flag


```

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/distribution_system_operator/redispatch_flag_MP-WND-003_20250609_1201.csv" de_mongodb:/tmp/redispatch_flag_MP-WND-003_20250609_1201.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection redispatch_flag \
  --type csv \
  --headerline \
  --file /tmp/redispatch_flag_MP-WND-003_20250609_1201.csv \
  --username user \
  --password password \
  --authenticationDatabase admin
  

```