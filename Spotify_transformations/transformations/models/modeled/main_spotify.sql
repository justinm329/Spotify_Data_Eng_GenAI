
-- Authors:
--   Justin Farnan - jfarnan@sandiego.edu

-- Change Log:
-- 01/25/2024 - started log, added documentation


-- What:
-- Model is to establish the main_spotify model which will be used in our upstream application.

-- Why:
-- main_spotify.sql only uses certain columns from both the refined models.

-- How:
-- 1. specifc columns are pulled from each refined model that will be used in hte upstream application
-- 2. the refined models are then inner joined on track_id to grab the same ids from both models to form the main-spotify model


-- INPUT:
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.ALL_PLAYLISTS_REFINED
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.AUDIO_FEATURES_REFINED
-- OUTPUT:
--    TABLE - ANALYTICS.SPOTIFY_SCHEMA.MAIN_SPOTIFY



-- ##########################################################################
-- Model Varaibles and configuration settings
-- ##########################################################################

{{ config( enabled = true,
        materialized='table') }}

with 
    main as (
        select 

            p.track_id,
            p.playlist_id,
            p.playlist_name,
            p.track_name,
            i.track_popularity,
            p.artists,
            a.danceability,
            a.duration_ms,
            a.energy,
            a.instrumental,
            a.song_key,
            a.liveness,
            a.loudness,
            a.speechiness

        from {{ref("all_playlists_refined")}} p
        inner join {{ref("audio_features_refined")}} a on p.track_id = a.track_id
        inner join {{ref('track_information_refined')}} i on p.track_id = i.track_id
        
    )




select *
from main

