# Spotify_Data_Eng_GenAI

Got inspired and built a cool Generative AI app for all of us music lovers out there! You know how you can get stuck listening to the same playlists or artists on repeat? And how a live music festival's diverse lineup always introduces you to new beats? Yeah, I wanted to bring a bit of that magic into our daily lives. So, I created this app where you and your friends can share your favorite playlists, tell the AI how you're feeling or what's going on, and it'll throw back song suggestions that match your vibe. The goal? To get you vibing to new genres, artists, and tracks by tapping into the details of your mood or the scene you're in. It's all about making discovering fresh tunes as easy and tailored to you as possible.

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
