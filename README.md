# Using-Pig-or-MapReduce-extract-transform-and-load-the-stackexchange-data-in-GCP

# STEP1: EXTRACTION OF DATA FROM STACK EXCHANGE
# Acquire the top 200,000 posts by viewcount by quering it in stack exchange https://data.stackexchange.com/stackoverflow/query/new

Stackexchange> select * from posts where posts.ViewCount>57850 and posts.ViewCount <= 100000 order by posts.ViewCount desc

Stackexchange> select * from posts where posts.ViewCount>58030 and posts.ViewCount <=57850  order by posts.ViewCount desc

Stackexchange> select * from posts where posts.ViewCount> 42000 and posts.ViewCount <= 58030 order by posts.ViewCount desc

Stackexchange> select * from posts where posts.ViewCount> 33000 and posts.ViewCount <= 42000 order by posts.ViewCount desc

Stackexchange> select * from posts where posts.ViewCount> 31500  and posts.ViewCount <= 33000 order by posts.ViewCount desc

# STEP2: TRANSFORM DATA BY USING PIG
# Acquire the top 200,000 by uploading it in GCP console and with the -put command transfer all records to Hadoop
hadoop fs -put QueryResults* /
Hadoop fs -ls /

# Clean and transform data in pig of GCP
pig
cd ../..
ls

# Register and define piggybank in pig to load all 5 csv records from hadoop
REGISTER /usr/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

# Load records with LOAD command
data_stack1  = LOAD 'QueryResults1.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') as (Id:chararray, PostTypeId:chararray, AcceptedAnswerId:chararray,	ParentId:chararray,	CreationDate:chararray, DeletionDate:chararray,	Score:chararray, ViewCount:chararray,	Body:chararray,	OwnerUserId:chararray,	OwnerDisplayName:chararray,	LastEditorUserId:chararray, LastEditorDisplayName:chararray, LastEditDate:chararray,	LastActivityDate:chararray,	Title:chararray,	Tags:chararray,	AnswerCount:chararray, CommentCount:chararray,	FavoriteCount:chararray, ClosedDate:chararray, CommunityOwnedDate:chararray);

data_stack2  = LOAD 'QueryResults2.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') as (Id:chararray, PostTypeId:chararray, AcceptedAnswerId:chararray,	ParentId:chararray,	CreationDate:chararray,	DeletionDate:chararray,	Score:chararray, ViewCount:chararray,	Body:chararray,	OwnerUserId:chararray,	OwnerDisplayName:chararray,	LastEditorUserId:chararray, LastEditorDisplayName:chararray, LastEditDate:chararray,	LastActivityDate:chararray,	Title:chararray,	Tags:chararray,	AnswerCount:chararray, CommentCount:chararray,	FavoriteCount:chararray, ClosedDate:chararray, CommunityOwnedDate:chararray);

data_stack3  = LOAD 'QueryResults3.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') as (Id:chararray, PostTypeId:chararray, AcceptedAnswerId:chararray,	ParentId:chararray,	CreationDate:chararray,	DeletionDate:chararray,	Score:chararray, ViewCount:chararray,	Body:chararray,	OwnerUserId:chararray,	OwnerDisplayName:chararray,	LastEditorUserId:chararray, LastEditorDisplayName:chararray, LastEditDate:chararray,	LastActivityDate:chararray,	Title:chararray,	Tags:chararray,	AnswerCount:chararray, CommentCount:chararray,	FavoriteCount:chararray, ClosedDate:chararray, CommunityOwnedDate:chararray);

data_stack4  = LOAD 'QueryResults4.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') as (Id:chararray, PostTypeId:chararray, AcceptedAnswerId:chararray,	ParentId:chararray,	CreationDate:chararray,	DeletionDate:chararray,	Score:chararray, ViewCount:chararray,	Body:chararray,	OwnerUserId:chararray,	OwnerDisplayName:chararray,	LastEditorUserId:chararray, LastEditorDisplayName:chararray, LastEditDate:chararray,	LastActivityDate:chararray,	Title:chararray,	Tags:chararray,	AnswerCount:chararray, CommentCount:chararray,	FavoriteCount:chararray, ClosedDate:chararray, CommunityOwnedDate:chararray);

data_stack5  = LOAD 'QueryResults5.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') as (Id:chararray, PostTypeId:chararray, AcceptedAnswerId:chararray,	ParentId:chararray,	CreationDate:chararray,	DeletionDate:chararray,	Score:chararray, ViewCount:chararray,	Body:chararray,	OwnerUserId:chararray,	OwnerDisplayName:chararray,	LastEditorUserId:chararray, LastEditorDisplayName:chararray, LastEditDate:chararray,	LastActivityDate:chararray,	Title:chararray,	Tags:chararray,	AnswerCount:chararray, CommentCount:chararray,	FavoriteCount:chararray, ClosedDate:chararray, CommunityOwnedDate:chararray);

# Combine the loaded data from 5 csv into combined_data using UNION command 
combined_data = UNION data_stack1, data_stack2, data_stack3, data_stack4,data_stack5 ;

# Create a table in pig for the transformation process
d1 = FOREACH combined_data GENERATE Id, Score, ViewCount, Body,OwnerUserId, OwnerDisplayName, Title, Tags;

# Remove null values from combined_data by FITER command
filtera = FILTER d1 by ((OwnerUserId != '') AND (OwnerDisplayName != ''));

# Replace white spaces with spaces using REPLACE command
D = FOREACH filtera GENERATE REPLACE(REPLACE(REPLACE(REPLACE(Id,'\\n',''),'\\r',''),'\\r\\n',''),'<br>','') as Id,REPLACE(REPLACE(REPLACE(REPLACE(Score,'\\n',''),'\\r',''),'\\r\\n',''),'<br>','') as Score,ViewCount,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Body,'\'',''),'\\"',''),'\\.',''),'\\..,',''),',',''),'\\.,',''),'\\n','') as Body,OwnerUserId, OwnerDisplayName, Title, Tags;

f = FILTER D by Id != 'Id';

# Store all cleaned records in result1 to query in hive
STORE f INTO 'result1' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE');

ls
cd result1
# Merge all 4 parts of cleaned record result into Query.csv using -getmerge command in Hadoop of GCP
hadoop fs -getmerge hdfs://cluster-29-m/user/archana_kalapgar2/result2/part-m-00000 hdfs://cluster-29-m/user/archana_kalapgar2/result2/part-m-00001 hdfs://cluster-29-m/user/archana_kalapgar2/result2/part-m-00002 hdfs://cluster-29-m/user/archana_kalapgar2/result2/part-m-00003 hdfs://cluster-29-m/user/archana_kalapgar2/result2/part-m-00004 /home/archana_kalapgar2/Query.csv

# STEP 3: QUERY THEM WITH HIVE
# Create a table stack in hive of GCP and load cleaned records in query.csv
create external table if not exists stack (Id int, Score int, ViewCount int,Body String, OwnerUserId int, OwnerDisplayName string, Title string, Tags string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

load data local inpath 'Query.csv' overwrite into table stack;
# TOP 10 POSTS BY SCORE
select distinct score from stack order by score desc limit 10;
# TOP 10 USERS BY POST SCORE
create table group_owneruserid_body as select ownerUserId as a, Body as         b,SUM(Score) as c from stack group by ownerUserId,Body;
select a,c from group_owneruserid_body order by c desc limit 10;
# The number of distinct users, who used the word “Hadoop” in one of their posts
select COUNT (DISTINCT OwnerUserId) from stack where lower (Body) like ‘%hadoop%’;

# Using Mapreduce calculate the per-user TF-IDF 

To run MapReduce program to calculate TFIDF, I added stop words in MapperPhaseOne.Py.
stopwords= ['a','able','about','across','after','all','almost','also','am','among','an','and','any','are','as','at','be','because','been','but','by',
            'can','cannot','could','dear','did','do','does','either','else','ever','every','for','from','get','got','had','has','have','he','her','hers',
            'him','his','how','however','i','if','in','into','is','it','its','just','least','let','like','likely','may','me','might','most','must','my',
            'neither','no','nor','not','of','off','often','on','only','or','other','our','own','rather','said','say','says','she','should','since','so',
            'some','than','that','the','their','them','then','there','these','they','this','tis','to','too','twas','us','wants','was','we','were','what',
            'when','where','which','while','who','whom','why','will','with','would','yet','you','your'];

# Upload all phases of mapper and reducer to GCP 

chmod +x mapper* reducer*

cd tfidfdata

00000_0
# Replace all comma with space using SED command
sed 's/,/ /g' 000000_0  >result
# Make new directory with -mkdir command and upload result to hadoop

hadoop fs -mkdir /tfidf_result

hadoop fs -put result /tfidf_result

hadoop fs -ls /

# Run mapper and reducer for all 3 phases using Hadoop jar command in GCP
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/archana_kalapgar2/MapperPhaseOne.py /home/archana_kalapgar2/ReducerPhaseOne.py -mapper "python MapperPhaseOne.py" -reducer "python ReducerPhaseOne.py" -input hdfs://cluster-29-mm-m/tfidf_result/result -output hdfs://cluster-29-mm-m/output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/archana_kalapgar2/MapperPhaseTwo.py /home/archana_kalapgar2/ReducerPhaseTwo.py -mapper "python MapperPhaseTwo.py" -reducer "python ReducerPhaseTwo.py" -input hdfs://cluster-29-mm-m/output/part-00000 hdfs://cluster-29-mm-m/output/part-00001 hdfs://cluster-29-mm-m/output/part-00002 hdfs://cluster-29-mm-m/output/part-00003 hdfs://cluster-29-mm-m/output/part-00004	 -output hdfs://cluster-29-mm-m/output1

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/archana_kalapgar2/MapperPhaseThree.py /home/archana_kalapgar2/ReducerPhaseThree.py -mapper "python MapperPhaseThree.py" -reducer "python ReducerPhaseThree.py" -input hdfs://cluster-29-mm-m/outputt/part-00000 hdfs://cluster-29-mm-m/outputt/part-00001 hdfs://cluster-29-mm-m/outputt/part-00002 hdfs://cluster-29-mm-m/outputt/part-00003 hdfs://cluster-29-mm-m/outputt/part-00004  -output hdfs://cluster-29-mm-m/outputtt

# Get obtained result and merge with -getmerge command

hadoop fs -getmerge hdfs://cluster-29-mm-m/outputtt/part-00000 hdfs://cluster-29-mm-m/outputtt/part-00001 hdfs://cluster-29-mm-m/outputtt/part-00002 hdfs://cluster-29-mm-m/outputtt/part-00003 hdfs://cluster-29-mm-m/outputtt/part-00004 /home/archana_kalapgar2/tfidff

# Make csv file of output using SED command
sed -e 's/\s/,/g' tfidff  > output.csv

# Create a table in hive and load output.csv to get result
Hive>create external table if not exists TFIDF (Term String,Id int,tfidf float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
load data local inpath 'output.csv' overwrite into table TFIDF;


SELECT *
FROM (
SELECT ROW_NUMBER()
OVER(PARTITION BY Id
ORDER BY tfidf DESC) AS TfidfRank, *
FROM TfIDF) n
WHERE TfidfRank IN (1,2,3,4,5,6,7,8,9,10);
