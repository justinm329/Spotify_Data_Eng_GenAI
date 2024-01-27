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

{%- set base_table_playlists = 'all_playlists' -%}


{{ config(materialized='table') }}

with playlist_data as (

    select 
        "Playlist_Id" as playlist_id,
        "Playlist_Name" as playlist_name,
        "Track_Name" as track_name,
        "Artist_Names" as artists,
        "artist_id" as artists_id,
        "Track_Id" as track_id

    from  {{base_table_playlists}}

)

Select 
    *
from 
    playlist_data