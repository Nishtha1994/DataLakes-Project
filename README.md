## Data Lakes for Music Application

The purpose of this project is to build an ETL pipeline that will be able to extract song and log data from an S3 bucket, process the data using Spark and load the data back into s3 as a set of dimensional tables in spark parquet files. 
This helps analysts to continue finding insights on what their users are listening to.

## Database Schema Design
The tables created include one fact table, songplays and four dimensional tables namely users, songs, artists and time. This follows the star schema principle which will contain clean data that is suitable for OLAP(Online Analytical Processing) operations 
which will be what the analysts will need to conduct to find the insights they are looking for.

## Usages

Use the python scripts in the order to create tables and insert data and retrieve the parquet files.
Please use your own keys in the AWS config

```python
python etl.py
```

## Testing

Check the parquet files using the online tool.

```bash
http://parquet-viewer-online.com/result
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
Nishtha Bhattacharjee(nishthabhattacharjee94@gmail.com)