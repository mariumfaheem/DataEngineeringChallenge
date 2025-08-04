## Assumption

- data is store in mongoDB NoSQL database
- I'm running these command to transfer data into mongoDB and in my script i will make assumption that private_trade and public_trade data source is mongoDB



```

docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/exchange/Private_Trades-20250608-20250609T000516000Z.csv" de_mongodb:/tmp/Private_Trades-20250608-20250609T000516000Z.csv

docker exec -it de_mongodb bash -c 'tail -n +2 /tmp/Private_Trades-20250608-20250609T000516000Z.csv | tr ";" "\t" | mongoimport --db flexpower --collection private_exchange --type tsv --headerline --username user --password password --authenticationDatabase admin'
  


docker cp "/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/exchange/Public_Trades-20250608-20250609T000516000Z.csv" de_mongodb:/tmp/Public_Trades-20250608-20250609T000516000Z.csv
  
docker exec -it de_mongodb bash -c 'tail -n +2 /tmp/Public_Trades-20250608-20250609T000516000Z.csv | tr ";" "\t" | mongoimport --db flexpower --collection public_exchange --type tsv --headerline --username user --password password --authenticationDatabase admin'
  
```