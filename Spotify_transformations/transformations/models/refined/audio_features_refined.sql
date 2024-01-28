-- Authors:
--   Justin Farnan - jfarnan@sandiego.edu

-- Change Log:
-- 01/25/2024 - started log, added documentation


-- What:
-- Model is to rename the columns that were in the orginal table ingested from the spotify api, formatting of column names
-- was incorrect in snowflake also needed to filter out the duplicate tracks.

-- Why:
-- Wanted to keep concistencty between name conventions in SQL, "Column_1" vs Column_1, removed quotes in the name.
-- Since we have multiple tracks within different playlists in our playlist model we can differeniate them by the playlist_id
-- but only in that model. We then use the track_ids form the playlist model to grab the tracks for the audio_features model.
-- When we do this we get duplicate trck_ids which we can't have because this will be our PK

-- How:
-- 1. renamed exisitng columns withouts quotes.
-- 2. Used a window function to filter out all the duplicate track_ids so track_ids meets the criteria for a PK (not_null, unique)

-- INPUT:
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.AUDIO_FEATURES
-- OUTPUT:
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.AUDIO_FEATURES_REFINED




-- ##########################################################################
-- Model Varaibles and configuration settings
-- ##########################################################################

{%- set base_table_audio_features = 'rawaudio_features' -%}

{{ config(
    enabled = true, 
    materialized='table') }}


with
    audio_features_refined as (


     select 
        "Track_id" as track_id,
        "danceability" as danceability,
        "duration_ms" as duration_ms,
        "energy" as energy,
        "instrumentalness" as instrumental,
        "key" as song_key,
        "liveness" as liveness,
        "loudness" as loudness,
        "mode" as mode,
        "speechiness" as speechiness,
        "time_signature" as time_signature,
        "track_uri" as trackuri,
        row_number() OVER (PARTITION by track_id order by track_id) as t_id
    
    from  
        {{ base_table_audio_features }}


)

Select 
    *
from 
    audio_features_refined
 where t_id = 1