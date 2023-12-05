# CVST Server Monitoring and Reporting, Smart City
This Python script is designed for monitoring and reporting vehicle counts in a parking area, specifically tailored for the context of CVST (Computer Vision Smart Traffic) server data. It utilizes Elasticsearch for data storage and retrieval, along with various email notifications to keep stakeholders informed.

## Key Features:
- Interacts with an Elasticsearch server to retrieve and store data related to parking area occupancy.
- Fetches the latest timestamped entry from the Elasticsearch index, extracting information such as counts and timestamps.
- Retrieves additional data from multiple Elasticsearch indices, aggregates the information, and calculates the total count of vehicles in the parking area.
---> Final result, including the timestamp, total counts, and available spots, is stored in a JSON file (report.json) for further analysis or reporting.
