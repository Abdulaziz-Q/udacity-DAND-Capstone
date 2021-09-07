# udacity-DAND-Capstone

## Project Overview
The purpose of the data engineering capstone project is to give me a chance to combine what I've learned throughout the program.


## Project Summary

In the Udacity provided project, I'll work with three datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


## Step 1: Scope the Project and Gather Data

#### Scope 
The objective of this project is to collect data from three different sources and produce fact and dimension tables in order to conduct immigration analysis using Spark and Pandas in the United States utilizing criteria such as city average temperature, city demographics, population number, and percentage.

#### Describe and Gather Data 
**I94 Immigration Data:** This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://www.trade.gov/national-travel-and-tourism-office) is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

**World Temperature Data:** This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).

**U.S. City Demographic Data:** This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

## Step 2: Explore and Assess the Data
> For exploratory data analysis, please refer to the jupyter notebook.

 
## Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

#### Staging tables:
| Immigration stage | Demographics stage | Temperature stage |
|:-----|:------:|:-----:|
|id                   |total_pop              |year
|date                 |num_male_pop          | month
|city_code            |prct_male_pop         |week#
|state_code           |num_female_pop        | city
|age                  |prct_female_pop         | avg_temperature
|gender               |num_veterans           | Latitude
|visa_type           |prct_veterans         |Longitude
|transportation_type |num_foreign_born
|                     |prct_foreign_born
|                      |race  
|                     |  state_code 
|                      |city
|                     | median_age



#### Fact table:
 | Immigration fact|
 | ----|
 |id|
 | date|
 |city|
 | city_code|
 |state_code|
 | count|
 

#### Dimension  tables
|immigration dim | demographics dim| temperature dim | time dim
|:----|:---:|:---:|:---|
|id|                 state_code        |city| date
|age|                city              | year| year
|visa_type           | median_age     |  month| month
|transportation_type| num_male_pop      |week#| week#
|   gender           |prct_male_pop     |avg_temperature|day
|                    |num_female_pop|
|                     |prct_female_pop|
|                   | num_veterans| 
|                     |prct_veterans|
|                      |num_foreign_born|
|                    |  prct_foreign_born|
|                  |total_pop| 
|                  |race| 
|                  |   Longitude  |
|                   | Latitude|

#### 3.2 Mapping Out Data Pipelines
Listing the steps necessary to pipeline the data into the chosen data model

1. Clean the data from null values, duplicates, etc
2. Load staging tables.
3. Create fact and dimension tables

## Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
#### 4.2 Data Quality Checks
> For the creation of the model and data quality checks, please refer to the jupyter notebook.

#### 4.3 Data dictionary 
   
 | Immigration fact|description|
 | ----|-
  |id:| id 
 | date:| arrival date 
 |city:| arrival city 
  | city_code:| arrival city code
  |state_code:| arrival state code
 | count:| used to count how many arrival to US

 &nbsp;
 
|immigration dim | description|
|:----|-|
|id:|   immigrant's id            
|age:|   immigrant's age             
|visa_type:   |    immigrant's visa type     
|transportation_type:| immigrant's transportation type
|   gender:  |  immigrant's gender        

&nbsp;

|demographics dim| description|
|:----|-|
|state_code:        |city port code|
|city:              | city name|
| median_age:     | median age of the city|
| num_male_pop:  |  number of the male population 
|prct_male_pop:  |   percentage of the male population
|num_female_pop:| number of the female population
|prct_female_pop:|percentage of the female population
| num_veterans:| number of the veterans population
|prct_veterans:| percentage of the veterans population
|num_foreign_born:| number of the foreign population
|  prct_foreign_born:|percentage of the foreign population
|total_pop:| total number of the city's population
|race:| Respondent race
|Longitude:  | city longitude
| Latitude:| city latitude

&nbsp;

 |temperature dim | description|
|:----|-|
|city:| city name
| year:| year of the record
|  month:| month of the record 
|week#:| week of the record 
|avg_temperature:| average temperature

&nbsp;

| time dim|description|
|:---|--|
| date:|date of the record|
| year:|year of the record|
| month:|month of the record|
| week#:|week of the record |
|day:|day week of the record |

## Step 5: Complete Project Write Up

Spark is chosen for this project as it isprovides a faster and more general data processing platform. Spark lets you run programs up to 100x faster in memory, or 10x faster on disk, than Hadoop. 

The data is used for reporting purposes. Whenever new data is needed, this code provides the ability to have cleaned data and organized.

How you would approach the problem differently under the following scenarios:

If the data was increased by 100x: In the future, we could explore scaling up the number of EC2 instances hosting Spark, as well as adding more Spark work nodes. Processing time should be able to be sped up as a result of increased capacity, which might come from either vertical or horizontal scaling.

If the data populates a dashboard that must be updated on a daily basis by 7am every day: We might explore utilizing Airflow to plan and automate the data pipeline jobs, which would be quite convenient. We may be able to satisfy customer requirements thanks to the built-in retry and monitoring mechanisms.

If the database needed to be accessed by 100+ people: We may think about putting our solution in a production-scale data warehouse in the cloud, which will have more capacity to service more users and will include workload management to guarantee that resources are distributed equally across users.

          
