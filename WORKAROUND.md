Open issue and occurs for the below specific config - 

1. Spark launched in cluster mode
2. Bind Mount Volume on Docker
3. Windows OS




In the interest of time, here is a workaround<br/>
**Caveat:** The workaround should adhere to the principle of Big Data.

Instead of saving the data as a parquet file, lets save it as a JSON-L (each line represents a json string) File.

Why JSON-L File?
Since the dataframe consists of nested structures, JSON is preferred over CSV/TSV. To adhere to Big Data principles, we will write the data line by line.
(At a given point of time, we have a single micro batch in memory)

Step 1: Design JsonFileHandler Class
```python
# Custom Json File Handler Class
class JsonFileHandler(object):
    def __init__(self, file_path:str, mode:str):
        """
        Initialize file handler
        """
        self.f = open(file_path, mode)
    
    def write_dataframe_as_jsonl(self, batch_df):
        """
        Write a micro-batch dataframe to a jsonl file 
        """
        for row in batch_df.collect():
            res = json.dumps(row.asDict())
            self.f.write(res+"\n")
    
    def close(self):
        """
        Finalize file write object
        """
        self.f.close()
```

Step 2: Create JsonFileHandler Object
```python
# Create a JSON File Writer
json_file_writer = JsonFileHandler(file_path="result.jsonl", mode="w")
```

Step 3: Write the micro-batch dataframe to the file-system
```python
# Call the write_data fn instead of batch_df.write.parquet
json_file_writer.write_dataframe_as_jsonl(batch_df)
```

Step 4: Close the File Writer
```python
# Close the file write to force flush the write buffer onto the file
# Close the filer writer after query.close() call
json_file_writer.close()
```

Few points to consider in production environment:

With this workaround, any following downstream processes must be modified.
1. Either make use of a shim to convert the jsonl file to parquet file, so the downstream process need not change
2. Update downstream process to consume jsonl file instead of parquet

Improvements:
1. Consider partitioning jsonl file
