-- Authors:
--   Justin Farnan - jfarnan@sandiego.edu

-- Change Log:
-- 01/25/2024 - started log, added documentation


-- What:
-- Model is to rename the columns that were in the orginal table ingested from the spotify api, formatting of column names
-- was incorrect in snowflake.

-- Why:
-- Wanted to keep concistencty between name conventions in SQL, "Column_1" vs Column_1, removed quotes in the name.

-- How:
-- 1. renamed exisitng columns withouts quotes.


-- INPUT:
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.ALL_PLAYLISTS
-- OUTPUT:
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.ALL_PLAYLISTS_REFINED


-- ##########################################################################
-- Model Varaibles and configuration settings
-- ##########################################################################



{{ config(enabled = true,
    materialized='table') }}

with playlist_data as (

    select 
        "Playlist_Id" as playlist_id,
        "Playlist_Name" as playlist_name,
        "Track_Name" as track_name,
        "Artist_Names" as artists,
        "Artist_Id" as artists_id,
        "Track_Id" as track_id,
        "Genres" as genres,
        current_date() as refreshed_date

    from {{ source('spotify_schema', 'RAW_History_of_Playlists') }}

)

Select 
    *
from 
    playlist_data