-- Authors:
--   Justin Farnan - jfarnan@sandiego.edu

-- Change Log:
-- 01/28/2024 - started log, added documentation


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