# Survivor
An ever growing collection of data and analyses of the survivor cast. Currently filled up to Season 42

## Catagories
##### Within this repository are files related to different questions/projects. Here is a list of those projects:
- Reading in players names and bios
- Webscraping contestant, episode, and season data from Wikipedia
- Analyzing how where a contestant is from and their age difference from the median age on their season affects the percentage of the time they make it to the jury, final tribal, and become a winner


## Problem Descriptions
### Reading in players names and bios:
#### Files related to this project:
- wholeCastScrape.py: The python script webscraping and formating the data
- nameSeasonBio.csv: a csv holding the data

#### Goal of this project: 
To read in contestants names, season number, and biographys from the CBS website. This information may eventually be used in other projects.


### Webscraping contestant, episode, and season data from Wikipedia:
#### Files related to this project:
- seasons.py: The main file related to the project
- season_data.py: A class file that holds the methods needed

#### Goal of this project:
To read in the data on each season from Wikipedia and upload it to a relational PostGreSQL database. This data will be used in subsequent projects.

#### What you need to know to use these files:
Because they upload to an RDS, you will need to add login credentials to a RDS you have the ability to create databases in. Also note that it is written into the script that this will be a PostGreSQL database, so that may need to be updated
