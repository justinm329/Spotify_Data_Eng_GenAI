-- Authors:
--   Justin Farnan - jfarnan@sandiego.edu

-- Change Log:
-- 01/28/2024 - started log, added documentation


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
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.RAW_TRACK_INFORMATION
-- OUTPUT:
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.RACK_INFORMATION_REFINED



-- ##########################################################################
-- Model Varaibles and configuration settings
-- ##########################################################################

{%- set base_table_track_information = 'raw_track_information' -%}


{{ config(enabled = true,
    materialized='table') }}

with track_info_data as (

    select 
        "Track_Id" as track_id,
        "Track_Name" as track_name,
        "Track_Popularity" as track_popularity,
        "Track_Uri" as track_uri,
        row_number() OVER (PARTITION by track_id order by track_id) as t_id

    from  {{base_table_track_information}}

)

Select 
    *
from 
    track_info_data
where t_id = 1