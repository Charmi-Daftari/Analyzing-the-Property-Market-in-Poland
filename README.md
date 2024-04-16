# Data Analysis Project: Analyzing the Property Market in Poland

This repository contains a data analyst portfolio project focused on analyzing the property market in Poland using data from the Otodom website. The project involves web scraping, data cleaning, transformation, and analysis using Snowflake, Python, and Google Sheets.

## Project Overview

The project is structured into several key phases:

1. **Data Collection**:
   - We will use Bright Data (formerly Luminati) for web scraping data from the Otodom website. The scraped data will include information about properties listed for sale or rent in Poland.

2. **Data Loading into Snowflake**:
   - The scraped data will be loaded into Snowflake, a cloud-based data warehousing platform. Snowflake will serve as the central repository for our property market dataset.

3. **Data Analysis and Transformation**:
   - Using Snowflake, we will analyze, clean, and transform the raw data to derive actionable insights. This will involve SQL queries to perform aggregations, filtering, and data manipulation.

4. **Python and Google Sheets Integration**:
   - Some data transformation tasks will be performed using Python scripts and Google Sheets. Python libraries like pandas will help clean and prepare data, and Google Sheets will be used for specific transformations or data enrichment.

5. **Reporting**:
   - We will answer detailed questions related to the property market in Poland.

## Repository Structure

The repository structure is organized as follows:

- **`scripts/`**: Contains Python scripts and Snowflake scripts for data cleaning, transformation, and integration with Google Sheets.

- **`data/`**: This directory will store any intermediate or processed datasets.

- **`reports/`**: Contains final reports generated from the analysis.

- **`README.md`**: The main documentation file explaining the project overview, structure, and usage.

## Usage

To replicate and run this project:

1. Set up Bright Data (Luminati) for web scraping and configure the scraping parameters.
2. Create a Snowflake account and set up the required database schema.
3. Use the provided Python scripts to clean and transform the scraped data.
4. Execute SQL queries in Snowflake to perform data analysis and generate insights.
5. Utilize Google Sheets for specific data transformations as needed.
6. Generate reports using the analyzed data.

## Dependencies

- Python 3.x
- Pandas
- Snowflake (SnowSQL)
- Google Sheets API (for Python integration)
- Bright Data (formerly Luminati) for web scraping
- Jupyter Notebook (optional, for data exploration and visualization)

## Contributors

- Charmi Daftari (https://github.com/Charmi-Daftari) - Data Analyst

## Resources

- [Bright Data](https://brightdata.com/)
- [Snowflake](https://www.snowflake.com/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- [Otodom Website](https://www.otodom.pl/)
