## Assumptions

- The data is stored in a MongoDB NoSQL database.
- I am using the following commands to transfer data into MongoDB.
- In my script, I assume that the `assets_contracts` data source is MongoDB.



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