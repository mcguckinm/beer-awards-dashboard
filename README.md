Overview:
This project builds on data pipelines and interactive dashboard to analyze results from the world beer cup (one of the largest competitions).

This entire system is collects competition results from 1996 all the way to 2025, beer style information, and brewery location data from multiple sources. Stores them in SQLite database and visualizes insights through Dash dashboard

Goal of this project was to demonstrate webscraping, API data, data cleaning and normalization, SQL database integration, data analysis queries, and interactive visualization with Dash

Sources:
World Beer Cup (WBC)
Competition winners scraped from public accessible PDF documents. 
Data extracted:
Year
Medal (Gold, Silver, and Bronze)
Beer Name, 
Brewery Name

Source: https://www.worldbeercup.org

BJCP Beer Styles:

Beer style guidelines that this competition focuses on using the 2021 Style guide

Style ID
Style name
ABV range
IBU range
SRM range
OG / FG ranges

Source: https://www.bjcp.org/style/2021/beer

OBDB (open brewery database) API source

Brewery name
brewery type
city
state/province
Country (if applicable)
Laititude / longitude
Website

Issues with this site is it foucses on US based breweries not world wide breweries.

Source:
https://www.openbrewerydb.org

Project Architecture:
Data Sources
    World Beer Cup
    BJCP Beer styles
    OpenBreweryDB API


Put into this:

01_collect_data.py
(scraping and API)

outputs RAW CSV files
data/raw

02_import_sqlite.py
cleans files and normalizes

SQLite Database
beer_awards.sqlite

put into 
03_query_cli.py
(command line SQL )

To visualize it 
04_dashboard.py

Project stucture:
lesson14-beer-awards-dashboard 
    data
        -raw
        -clean

    db
        -beer_awards.sqlite

    src
        01_collect_data.py
        02_import_sqlite.py
        03_query_cli.py
        04_dashboard.py
        utils.py

    settings.json
    README.md
    requirements.txt

## HOW TO RUN PROJECT

clone repository:
git clone https://github.com/mcguckinm/beer-awards-dashboard.git

move into the project directory

create virtual environment

python -m venv .venv
source .venv/bin/activate

install dependencies: 

pip install -r requirements.txt

Run the data pipeline

python src/01_collect_data.py
python src/02_import_sqlite.py
python src/03_query_cli.py

python src/04_dashboard.py

open dashboard in browser should be:
http://127.0.0.1:8050



