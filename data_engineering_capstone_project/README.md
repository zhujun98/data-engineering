# DEND - Capstone Project

Jun Zhu
___

Describe your project at a high level.

Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>

## Dataset

Describe the data sets you're using. Where did it come from? What type of information is included? 

## Exploratory Data Analysis

#### Explore the Data
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data

## Data Modeling

#### Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

#### Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

## ETL Pipeline

#### Create the data model
Build the data pipelines to create the data model.

#### Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as 
expected. These could include:

- Integrity constraints on the relational database (e.g., unique key, data type, etc.)
- Unit tests for the scripts to ensure they are doing the right thing
- Source/Count checks to ensure completeness

Run Quality Checks

#### Data dictionary
Create a data dictionary for your data model. For each field, provide a brief 
description of what the data is and where it came from. You can include the 
data dictionary in the notebook or in a separate file.

## Reflection

- Clearly state the rationale for the choice of tools and technologies for the project.
- Propose how often the data should be updated and why.
- Write a description of how you would approach the problem differently under the following scenarios:
     * The data was increased by 100x.
     * The data populates a dashboard that must be updated on a daily basis by 7am every day.
     * The database needed to be accessed by 100+ people.