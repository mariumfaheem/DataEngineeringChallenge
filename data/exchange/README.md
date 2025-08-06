## Assumptions

### Final Production Data

- The assumption is to read data from CSV files (e.g., stored on S3), process it, and store the results in the landing layer.

### Redispatch Compensation

- The data is stored in a MongoDB NoSQL database.
- I am running commands to transfer the data into MongoDB.
- In the script, it is assumed that the `assets_contracts` data source resides in MongoDB.

```

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/exchange/Private_Trades-20250608-20250609T000516000Z.csv" de_mongodb:/tmp/Private_Trades-20250608-20250609T000516000Z.csv

docker exec -it de_mongodb bash -c 'tail -n +2 /tmp/Private_Trades-20250608-20250609T000516000Z.csv | tr ";" "\t" | mongoimport --db flexpower --collection private_exchange --type tsv --headerline --username user --password password --authenticationDatabase admin'
  


docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/exchange/Public_Trades-20250608-20250609T000516000Z.csv" de_mongodb:/tmp/Public_Trades-20250608-20250609T000516000Z.csv
  
docker exec -it de_mongodb bash -c 'tail -n +2 /tmp/Public_Trades-20250608-20250609T000516000Z.csv | tr ";" "\t" | mongoimport --db flexpower --collection public_exchange --type tsv --headerline --username user --password password --authenticationDatabase admin'
  
```