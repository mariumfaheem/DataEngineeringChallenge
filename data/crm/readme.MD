## Assumption

- data is store in mongoDB NoSQL database
- I'm running these command to transfer data into mongoDB and in my script i will make assumption that assets_contracts data source is mongoDB




```
docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/crm/asset_contracts_20250525_045438.csv" de_mongodb:/tmp/asset_contracts_20250525_045438.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection asset_contracts \
  --type csv \
  --headerline \
  --file /tmp/asset_contracts_20250525_045438.csv \
  --username user \
  --password password \
  --authenticationDatabase admin


docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/crm/asset_contracts_20250601_043417.csv" de_mongodb:/tmp/asset_contracts_20250601_043417.csv


docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection asset_contracts \
  --type csv \
  --headerline \
  --file /tmp/asset_contracts_20250601_043417.csv \
  --username user \
  --password password \
  --authenticationDatabase admin


docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/crm/asset_contracts_20250608_043831.csv" de_mongodb:/tmp/asset_contracts_20250608_043831.csv

docker exec -it de_mongodb mongoimport \
  --db flexpower \
  --collection asset_contracts \
  --type csv \
  --headerline \
  --file /tmp/asset_contracts_20250608_043831.csv \
  --username user \
  --password password \
  --authenticationDatabase admin
  ```