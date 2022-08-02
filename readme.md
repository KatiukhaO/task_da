Data analyst test task

Use free trial access to couchbase nosql database.
https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html

Create python script that:

a) loads the data from Travel Sample Bucket into csv file in pandas 
(in structured format - so it can be read in excel)

b) adds new column 'testColumn' to every record in Travel Sample Bucket 
(artificial test - if structure of data has changed in sample bucket) 
fill the column with test data

c) updates csv file created on step a).
Update should be done in a smart way - query only the data you need,
not updating full table again. Hint: use merge functionality in pandas.
