import pandas as pd
import pyarrow
import numpy as np
from utils_2.sf_conn_2 import Config
import snowflake.connector as sfc


### Create a class to handle the refined and modeled transformations in snowflake


class spotify_transformation:

    def __init__(self):

        self.sf_config = Config()
    
    def refined_playlists(self):
        
        conn = self.sf_config.create_sf_conn()
        cursor = conn.cursor()
        try:
            
            query = """
                CREATE OR REPLACE TRANSIENT TABLE ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.ALL_PLAYLISTS_REFINED AS
            SELECT 
                "Playlist_Id" as playlist_id,
                "Playlist_Name" as playlist_name,
                "Track_Name" as track_name,
                "Artist_Names" as artists,
                "Artist_Id" as artists_id,
                "Track_Id" as track_id,
                "Genres" as genres
            FROM 
                ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.RAW_ALL_PLAYLISTS;
            """

            cursor.execute(query)
        except sfc.Error as e: 
            print(f'Error happened in All_Playlists_Refined: {e}')
        finally:
            cursor.close()
            conn.close()

    def refined_audio_features(self):

        conn = self.sf_config.create_sf_conn()
        cursor = conn.cursor()
        try:
            
            query = """
                CREATE OR REPLACE TRANSIENT TABLE ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.AUDIO_FEATURES_REFINED AS
                    WITH first_query AS (    
                        SELECT 
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
                            ROW_NUMBER() OVER (PARTITION BY "Track_id" ORDER BY "Track_id") AS t_id
                        FROM 
                            ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.RAW_AUDIO_FEATURES
                            )
                        SELECT 
                            *
                        FROM 
                            first_query
                        WHERE t_id = 1;
            """

            cursor.execute(query)
        except sfc.Error as e: 
            print(f'Error happened in AUDIO_FEATURES_REFINED: {e}')
        finally:
            cursor.close()
            conn.close()

    def refined_track_information(self):

        conn = self.sf_config.create_sf_conn()
        cursor = conn.cursor()
        try:
            
            query = """
                CREATE OR REPLACE TRANSIENT TABLE ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.TRACK_INFORMATION_REFINED AS
                    WITH first_query AS ( 
                        SELECT 
                            "Track_Id" as track_id,
                            "Track_Name" as track_name,
                            "Track_Popularity" as track_popularity,
                            "Track_Uri" as track_uri,
                            row_number() OVER (PARTITION by track_id order by track_id) as t_id
                        FROM 
                            ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.RAW_TRACK_INFORMATION
                            )
                        SELECT 
                            *
                        FROM 
                            first_query
                        WHERE t_id = 1;
            """

            cursor.execute(query)
        except sfc.Error as e: 
            print(f'Error happened in TRACK_INFORMATION_REFINED: {e}')
        finally:
            cursor.close()
            conn.close()


    def final_spotify_model(self):

        conn = self.sf_config.create_sf_conn()
        cursor = conn.cursor()
        try:
            
            query = """
                CREATE OR REPLACE TRANSIENT TABLE ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.MAIN_SPOTIFY AS
                        SELECT 
                            p.track_id,
                            p.playlist_id,
                            p.playlist_name,
                            p.track_name,
                            i.track_popularity,
                            p.artists,
                            p.genres,
                            a.danceability,
                            a.duration_ms,
                            a.energy,
                            a.instrumental,
                            a.song_key,
                            a.liveness,
                            a.loudness,
                            a.speechiness
                        FROM ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.ALL_PLAYLISTS_REFINED p
                        INNER JOIN ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.AUDIO_FEATURES_REFINED a ON p.track_id = a.track_id
                        INNER JOIN ANALYTICS_JUSTIN.SPOTIFY_SCHEMA.TRACK_INFORMATION_REFINED i ON p.track_id = i.track_id
            """

            cursor.execute(query)
        except sfc.Error as e: 
            print(f'Error happened in MAIN_SPOTIFY: {e}')
        finally:
            cursor.close()
            conn.close()
