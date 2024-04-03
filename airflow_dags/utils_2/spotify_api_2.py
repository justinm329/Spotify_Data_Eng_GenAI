import os
import requests
from dotenv import load_dotenv
load_dotenv()
import base64
import json
import numpy as np
import pandas as pd
import pyarrow
from snowflake.connector.pandas_tools import write_pandas
from utils_2.sf_conn_2 import Config
from json.decoder import JSONDecodeError
import time
from utils_2.playlist_ids import spotify_playlist_ids

### these are the api keys that we will use
# client_id = os.getenv('CLIENT_ID')
# client_secret = os.getenv('CLIENT_SECRET')

# ### This is going to be the playlists that we are extracting data from
# spotify_playlist_ids = ['37i9dQZF1EQp9BVPsNVof1?si=b0e0182f48ea461c'
#                 "37i9dQZF1EIh5VHows1LoY?si=3f242c7e9e7a43f8",
#                 '37i9dQZF1EVKuMoAJjoTIw?si=2ed6df3d293b4519',
#                 '37i9dQZF1EQpoj8u9Hn81e?si=92566118c4954e73',
#                 '37i9dQZF1EQqlvxWrOgFZm?si=2b9ab93cc7714708',
#                 '37i9dQZF1EIhBF9gUW4UsT?si=089940507c6b46f3',
#                 '37i9dQZF1EIherXksVvnrN?si=d8e28d16e28b40b0',
#                 '37i9dQZF1EVHGWrwldPRtj?si=98356d3db1994d67',
#                 "37i9dQZF1DWSf2RDTDayIx?si=1d3f916447094372",
#                 "37i9dQZF1DX0AMssoUKCz7?si=80092935821a4201",
#                 '37i9dQZF1DX0hvSv9Rf41p?si=72b7f2562f0a4020',
#                 "6FSFUWzuF2KigQDKV42Uru?si=cfc88421610b46c3",
#                 '37i9dQZF1E35DbZEmy06Qp?si=b6e38bde3d8d4116',
#                 '37i9dQZF1EIgtj4OvJCT7Q?si=d18338df81cc4ff1']

# # Encode Client ID and Client Secret
# client_creds = f"{client_id}:{client_secret}"
# client_creds_b64 = base64.b64encode(client_creds.encode()).decode()

# auth_url = 'https://accounts.spotify.com/api/token'
# headers = {
#     'Authorization': f"Basic {client_creds_b64}"
# }

# payload = {
#     "grant_type": "client_credentials"
# }

# auth_response = requests.post(auth_url, headers=headers, data=payload)
# auth_response_data = auth_response.json()
# # print(auth_response_data)
# access_token = auth_response_data['access_token']




########### Main Functions used to Extract Data from Spotifys API #########################
## We get the artists generes so we can add it to the playlist dataframe 

class Spotify():

    def __init__(self):
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.access_token = self.get_access_token()

    def get_access_token(self):
        client_creds = f"{self.client_id}:{self.client_secret}"
        client_creds_b64 = base64.b64encode(client_creds.encode()).decode()
        auth_url = 'https://accounts.spotify.com/api/token'
        headers = {
            'Authorization': f"Basic {client_creds_b64}"
        }
        payload = {
            "grant_type": "client_credentials"
        }
        auth_response = requests.post(auth_url, headers=headers, data=payload)
        if auth_response.status_code != 200:
            raise Exception("Could not authenticate with Spotify")
        auth_response_data = auth_response.json()
        return auth_response_data['access_token']

    def get_artist_genres(self, artist_id):
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        headers = {'Authorization': f'Bearer {self.access_token}'}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            artist_data = response.json()
            return ', '.join(artist_data.get('genres', []))
        else:
            print(f"Error fetching genres for artist {artist_id}")
            return ''
    
    def get_playlist_name_tracks(self, playlist_ids):
        all_playlists_data = []
        for ids in playlist_ids:
            # Get the playlist's name
            url = f"https://api.spotify.com/v1/playlists/{ids}"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            name_response = requests.get(url, headers=headers)

            # Check the response status and decode JSON
            if name_response.status_code != 200:
                print(f"Failed to fetch playlist name for {ids}: {name_response.status_code}")
                continue
            try:
                playlist_name = name_response.json().get("name", "Unknown Playlist")
            except JSONDecodeError:
                print(f"JSON decoding failed for playlist name {ids}")
                continue

            # Initialize pagination variables
            limit = 50
            offset = 0
            total_tracks = None

            # Loop through the paginated endpoint until all tracks have been fetched
            while total_tracks is None or offset < total_tracks:
                # Get the playlist's tracks
                tracks_url = f'https://api.spotify.com/v1/playlists/{ids}/tracks?limit={limit}&offset={offset}'
                track_response = requests.get(tracks_url, headers=headers)
    

                
                if track_response.status_code != 200:
                    print(f"Failed to fetch tracks for playlist {ids}: {track_response.status_code}")
                    break
                try:
                    track_data = track_response.json()

                    #print(track_data)
                except JSONDecodeError:
                    print(f"JSON decoding failed for tracks in playlist {ids}")
                    break

                # Update the total tracks count (only once)
                if total_tracks is None:
                    total_tracks = track_data.get('total', 0)
                
                # Process each track
                for item in track_data.get('tracks', {}).get('items', []):
                    track = item.get('track')
                    if track:
                        track_id = track.get('id')
                        track_name = track.get('name')
                        artist_names = ', '.join(artist.get('name', '') for artist in track.get('artists', []))
                        artist_ids = ', '.join(artist.get('id', '') for artist in track.get('artists', []))
                        genres_list = [self.get_artist_genres(artist_id) for artist_id in artist_ids.split(', ')]
                        genres = '; '.join(genres_list)

                        all_playlists_data.append({
                            'Playlist_Id': ids,
                            "Playlist_Name": playlist_name,
                            'Track_Name': track_name,
                            "Artist_Names": artist_names,
                            "Artist_Id": artist_ids,
                            "Track_Id": track_id,
                            "Genres": genres
                        })
                    
                # Increment the offset for the next page of results
                offset += limit

        # Create a DataFrame from the collected data
            playlist_df = pd.DataFrame(all_playlists_data)
        return playlist_df

    def get_track_information(self, track_ids):
        track_data_list = []
        for ids in track_ids:
            url = f"https://api.spotify.com/v1/tracks/{ids}"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(url, headers=headers)
           # track_data = response.json()
            # print(response.status_code)
            # print(response.headers)
            # Check if the response status code indicates success
            if response.status_code == 200:
                try:
                    track_data = response.json()
                    track_name = track_data.get('name')
                    track_popularity = track_data.get('popularity')
                    track_uri = track_data.get('uri')

                    track_data_list.append({
                        
                    "Track_Id": ids,
                    "Track_Name": track_name,
                    "Track_Popularity": track_popularity,
                    "Track_Uri": track_uri
                })

                except:
                    print("Error: Could not decode JSON. Response content:", response.text)
                    raise
            else:
                # The API request was not successful
                print(f"Error: Received response with status code {response.status_code}. Response content:", response.text)
                return None
        return pd.DataFrame(track_data_list)

    def get_track_audio_features(self, track_ids):
        track_audio_features = []
        for id in track_ids:
            url = f"https://api.spotify.com/v1/audio-features/{id}"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(url, headers=headers)


            if response.status_code == 200:
                    track = response.json()

                    # for audio in repsone_json['track']:
                    audio_data = {
                        'Track_id': id,
                        'danceability': track['danceability'],
                        'duration_ms': track['duration_ms'],
                        'energy': track['energy'],
                        'instrumentalness': track['instrumentalness'],
                        'key': track['key'],
                        'liveness': track['liveness'],
                        'loudness': track['loudness'],
                        'mode': track['mode'],
                        'speechiness': track['speechiness'],
                        'time_signature':track['time_signature'],
                        'track_uri': track['uri']

                    }
                    track_audio_features.append(audio_data)

            else:
                raise Exception(f"Error in API request: {response.status_code}")
        track_audio_features_df = pd.DataFrame(track_audio_features)
        return track_audio_features_df
## def write_to_sf(access_token, playlist_ids):
    ######### CREATE CONNECTION TO SNOWFLAKE AND WRITE THE TABLES TO THE SPOTIFY_SCHEMA #################
    def write_to_sf(self, playlist_ids = spotify_playlist_ids):
        # spotify = Spotify(self.access_token)
        config = Config()
        sf_conn = config.create_sf_conn()
        playlist_df = self.get_playlist_name_tracks(playlist_ids)
        track_ids_list = playlist_df['Track_Id'].to_list()
        track_information = self.get_track_information(track_ids_list)
        audio_features_df = self.get_track_audio_features(track_ids_list)
        config.drop_and_recreate(sf_conn, playlist_df, "RAW_ALL_PLAYLISTS")
        config.drop_and_recreate(sf_conn, track_information, "RAW_TRACK_INFORMATION")
        config.drop_and_recreate(sf_conn, audio_features_df, "RAW_AUDIO_FEATURES")
        # write_pandas(sf_conn, playlist_df, "RAW_History_of_Playlists",  auto_create_table = True)
        # write_pandas(sf_conn, audio_features_df, "RAW_History_of_Audio_Features",  auto_create_table = True)
        # write_pandas(sf_conn, track_information, "RAW_History_of_Track_Information",  auto_create_table = True)
        config.close_sf_conn(sf_conn)

