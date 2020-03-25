# Redshift DWH IAC and Data Pipeline
------
### Table of Contents:-
1. Porject Summary
2. Infrastructure As Code (IAC)
3. Source Datasets
4. Exploratory Data Analysis (EDA)
5. Schemas
6. Loading Strategies
    1. Data Flow
    2. Initial & Incremental Load
7. Files Structures & Functionalities
8. Execution Steps
9. Removing the Cluster Steps


## 1. Porject Summary:-
------
<p>The Main Objectives of this project is to Populate a DWH schema with Initial & Incremental data starting from the Creation of the Cluster by Code (IAC) through Loading the Data from S3 Buckets to Staging Schema, and finally load the data into the Target Tables with strict data Quality Rules & Informative Logging of all the steps just by running Python Codes.</p>


## 2. Infrastructure As Code (IAC):-
------
The Target DWH of this project is a Redshift DWH hosted on AWS. The DWH can be Created & Mainatained by Python scripts attached in this project files, which Declare, Inialize, start & remove all the required services needed to load the data from S# to the Cluster, Some of these Services are IAM, S3, EC2, VPC & Redshift.


## 3. Source Datasets:-
---------
The Source Datasets are JSON Files hosted on S3 Bucket on AWS. Two Datasets available in two groups of Files Log Files & Songs Files.

- Log Files contain the Log of Songs plays, Artists, Users, & Time Data.
- Song Files contain the detailed information about all the songs & Artists.


## 4. Exploratory Data Analysis (EDA):-
----------
During the Data Analysis & Code Preparation Phases, I used two Python Notebooks to Profile & analyze the data to Develope the DDLs, DMLs & Python scripts for Initial & Incremental Data Loading.

For all the Logic behind the Transformation, Quality rules & Data Extraction please refer to these Notebooks.


## 5. Schemas:-
----------
Two Schemas exist in this Project, A Staging Schema to contain the data as it exist in the S3 Buckets using Redshift Copy command, Sparkify Schema which contain the Main Tables required for the Data Analytics.

- Staging Schema:-

![](Schemas/Redshift%20Staging%20Schema.png)

- Sparkify Schema:-

![](Schemas/Redshift%20Sparkify%20Schema.png)



## 6. Loading Strategies:-
----------
1. Data Flow:-
    - The Data Exist in S3 Buckets on AWS, the provided code files Load the Data from the S3 Buckets into the Staging Schema as it exists in the Buckets, then loads the data from the Staging Schema into the Sparkify Schema after applying all Transformation Rules, Quality Check, Initial & Incremental Logic for all the Tables.
    - An Assumsion is considered while loading the Data, Only the Incremental data from the Sources are loaded into the S3 Buckets each time the codes are executed, and due to having only read only access to the S3 Buckets, Archiving is not available to archive the already loaded data, So the bottom Line is the codes will handle whatever data exists in the S3 Buckets but the Incremental Data Extraction from the sources is considered to be handled in the systems the load the data into S3.
 
 
2. Initial & Incremental Load:-
    - The Codes Loads all the Data that exist in The S3 Buckets, whether it is Initial or Incremental Data. Please refer to the above Point.
    - In case it is an Initial Load, All the Data will be Loaded to the DWH with all the Defined Rules.
    - In case it is an Incremental Load, We apply SCD1 type Merge, the Data will be loaded into the Staging Schema then only the Latest Version of the will exist in the Sparkify Schema, the Merge Logic applied according to Redshift's best Practices 'Merge Method 1: Replacing Existing Rows', which Deletes the Records that have Updates then loads them again all in One transaction to Garauntee the Consistency of the Data in case a failure arises it Rolls back Automatically https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html.
    
    
## 7. Files Structures & Functionalities:-
-----------
> dwh.cfg : A Configuration files that contains the AWS services, DWH Configurations, DDL & DMLs Constants

> IAC_create_redshift_cluster.py : Creates all the Needed AWS Services to Create & Start a Redshift cluster with the info in dwh.cfg

> IAC_remove_redshift_cluster.py : Removes the Needed AWS Services with Info exist the in dwh.cfg

> Initial Profiling Song-And-Log Files .ipynb : Initial Data Loading & Profiling for S3 Data.

> EDA - Song Files - Load Preparation.ipynb : Contains all the Data Profiling, Analysis, DDLs, DMLs & Decsisions made for Data Loading for Staging Song, Song & Artist Tables.

> EDA - Log Files - Load Preparation.ipynb : Contains all the Data Profiling, Analysis, DDLs, DMLs & Decsisions made for Data Loading for Staging Log, Time, Song Play, User Tables.

> create_tables.py : Contains all DDLs & DMLs for all the Schemas & Tables, once it is ran it creates all The Schemas and the Tables.

> etl.py : Loads the Data from S3 through the Staging Schema and finally to Sparkify Schema after applying all Transformations, Quality Checks & Handling Delta Data.


## 8.Execution Steps:-
-----------

- Cluster Creation:

    1. Update dwh.cfg With all the Required Configurations
    2. execute `python IAC_create_redshift_cluster.py` in the Terminal
    
    ![](Read%20Me/IAC_Create_Cluster.PNG)
    

- Initial Load:

    - Execute `create_tables.py` in the Terminal
    
    ![](Read%20Me/create_tables.PNG)
    
    - Execute `python etl.py` in the Terminal
    
    ![](Read%20Me/etl.PNG)
    
    
- Delta Load:

    - Exactly the same as Initial Load and the Code will handle everything, Execute `python etl.py` in the Terminal
    
    
    
## 9. Removing the Cluster Steps
-------------
- Execute `IAC_remove_redshift_cluster.py` in the Terminal

![](Read%20Me/IAC_remove_cluster.PNG)
