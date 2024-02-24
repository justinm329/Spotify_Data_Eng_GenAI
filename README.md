# Spotify_Data_Eng_GenAI
This project encapsulates data engineering and GenAI to recommend songs

## Downloading DBT

https://www.kipi.bi/post/dbt-python-model-implementation


## Next Steps

### Engineering
* Automate Spotify data extraction with Airflow, scheduling weekly updates to keep the dataset current.

* Use Airflow for managing the assistant's CSV file and handling periodic updates and deletions.

* Schedule weekly DBT jobs to refresh data models following each Spotify data retrieval.

* Restructure the database schema into RAW, REFINED, and MODELD layers, optimizing data flow from raw extraction to transformation for analysis and the main_spotify model.

* Refine the OpenAI Assistant's prompt to improve the accuracy and relevance of song recommendations, including feature adjustments to prevent "overfitting."


### Analytics
* Develop new DBT models based on RAW_history data to lay the groundwork for comprehensive analytics.

* Introduce a new dashboard in the Streamlit app to provide users with visual analytics based on historical data, enhancing user interaction by offering insights into musical trends and preferences.

* Fine-tune the assistant's processing of queries to significantly enhance user experience through more consistent and relevant output.
