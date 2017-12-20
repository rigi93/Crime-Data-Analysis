Big Data - Project
----------------------
**Team Members** 

Name    | NetID
-------- | ---
Miti Anish Jhaveri | maj529
Rigved Ravindra Kale | rrk320

Analyzing NYPD Crime Complaint Data (Historic)

-------------

#### <i class="icon-file"> </i>Project Report Link
The project report containing both parts of the project (Data cleaning and Data Analysis) can be found [here!](https://docs.google.com/a/nyu.edu/document/d/190G90TBUJEbTvA8M0haRlhVO3CgpNcm_siW55Rp1cV8/edit?usp=sharing)

-------------
All the values from the dataset have been cleaned and marked for valid/invalid, updated values for missing values in few columns appropriately as well as validated the data in the allDataCleaning.py file. 

-------------

We used the following data for analysis of the number of reported criminal offenses. To do so, we have extracted eight columns of our interest from the main data set and cleaned it separately in the datacleaning.py file: 

- **CMPLNT_NUM** (Complaint number to report the event).

- **CMPLNT_FR_DT** (Exact date of occurrence for the reported event).

- **CMPLNT_FR_TM** (Exact time of occurrence for the reported event).

- **RPT_DT** (Exact date of reporting the offense).

- **KY_CD** (Three digit offense classification code).

- **CRM_ATPT_CPTD_CD** (Stating whether the crime was completed or only attempted).

- **LAW_CAT_CD** (Category of the offense).

- **BORO_NM** (Name of Borough). 

The analysis of data has been represented using the reproducible scripts in the Image Scripts section. The images that will be developed are as those in the Images folder.
