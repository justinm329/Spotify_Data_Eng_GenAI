import os
import requests
from dotenv import load_dotenv
load_dotenv()
import base64
import json
import numpy as np
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from utils.sf_conn import Config
import time

### these are the api keys that we will use
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

### This is going to be the playlists that we are extracting data from
spotify_playlist_ids = ['37i9dQZF1EQp9BVPsNVof1?si=b0e0182f48ea461c'
                "37i9dQZF1EIh5VHows1LoY?si=3f242c7e9e7a43f8",
                '37i9dQZF1EVKuMoAJjoTIw?si=2ed6df3d293b4519',
                '37i9dQZF1EQpoj8u9Hn81e?si=92566118c4954e73',
                '37i9dQZF1EQqlvxWrOgFZm?si=2b9ab93cc7714708',
                '37i9dQZF1EIhBF9gUW4UsT?si=089940507c6b46f3',
                '37i9dQZF1EIherXksVvnrN?si=d8e28d16e28b40b0',
                '37i9dQZF1EVHGWrwldPRtj?si=98356d3db1994d67',
                "37i9dQZF1DWSf2RDTDayIx?si=1d3f916447094372",
                "37i9dQZF1DX0AMssoUKCz7?si=80092935821a4201",
                '37i9dQZF1DX0hvSv9Rf41p?si=72b7f2562f0a4020',
                "6FSFUWzuF2KigQDKV42Uru?si=cfc88421610b46c3",
                '37i9dQZF1E35DbZEmy06Qp?si=b6e38bde3d8d4116']

# Encode Client ID and Client Secret
client_creds = f"{client_id}:{client_secret}"
client_creds_b64 = base64.b64encode(client_creds.encode()).decode()

auth_url = 'https://accounts.spotify.com/api/token'
headers = {
    'Authorization': f"Basic {client_creds_b64}"
}

payload = {
    "grant_type": "client_credentials"
}

auth_response = requests.post(auth_url, headers=headers, data=payload)
auth_response_data = auth_response.json()
access_token = auth_response_data['access_token']

# # Step 2: Make API Request with the Access Token
# api_headers = {
#     "Authorization": "Bearer " + access_token
# }


########### Main Functions used to Extract Data from Spotifys API #########################
## We get the artists generes so we can add it to the playlist dataframe 

class Spotify():

    def __init__(self, access_token):

        self.access_token = access_token

    def get_artist_genres(self, artist_id):
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        headers = {'Authorization': f'Bearer {self.access_token}'}
        response = requests.get(url, headers=headers)
        print(response.status_code)
        print(response.headers)

        if response.status_code == 200:
            artist_data = response.json()
            return ', '.join(artist_data.get('genres', []))
        else:
            print(f"Error fetching genres for artist {artist_id}")
            return ''
    
    def get_playlist_name_tracks(self, playlist_ids):
        all_playlists_data = []
        for ids in playlist_ids:
            # Get the playlists name
            url = f"https://api.spotify.com/v1/playlists/{ids}"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            name_response = requests.get(url, headers=headers)
            playlist_name = name_response.json().get("name")
            # get the track_ids

            url = f'https://api.spotify.com/v1/playlists/{ids}/tracks'
            headers = {'Authorization': f'Bearer {self.access_token}'}
            track_id_response = requests.get(url, headers=headers)
            print(track_id_response.headers)
            print(track_id_response.status_code)
            if track_id_response.status_code == 200:

                track_response = track_id_response.json()
                tracks_data = track_response['tracks']
                for item in tracks_data['items']:
                    if item['track'] is not None:
                        track_id = item['track']['id']
                        track_name = item['track']['name']
                        artist_names = ', '.join(artist['name'] for artist in item['track']['artists'])
                        artist_id = ', '.join(artist['id'] for artist in item['track']['artists'])
                        genres_list = [self.get_artist_genres(artist['id']) for artist in item['track']['artists']]
                        genres = '; '.join(genres_list)

                        all_playlists_data.append({
                            'Playlist_Id': ids,
                            "Playlist_Name": playlist_name,
                            'Track_Name': track_name,
                            "Artist_Names": artist_names,
                            "artist_id":artist_id,
                            "Track_Id": track_id,
                            "Genres": genres
                        })
            else:
                raise Exception(f"Error in API request: {track_id_response.status_code}")
        playlist_df = pd.DataFrame(all_playlists_data)
        return playlist_df

    def get_track_information(self, track_ids):
        track_data_list = []
        for ids in track_ids:
            url = f"https://api.spotify.com/v1/tracks/{ids}"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(url, headers=headers)
           # track_data = response.json()
            print(response.status_code)
            print(response.headers)
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
            #track = response.json()
            print(response.headers)
            print(response.status_code)
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
            elif response.status_code == 429:
                    # response.header['retry_after']
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

######### CREATE CONNECTION TO SNOWFLAKE AND WRITE THE TABLES TO THE SPOTIFY_SCHEMA #################
def write_to_sf(access_token, playlist_ids):
    spotify = Spotify(access_token)
    config = Config()
    sf_conn = config.create_sf_conn()
    playlist_df = spotify.get_playlist_name_tracks(playlist_ids)
    track_ids_list = playlist_df['Track_Id'].to_list()
    #time.sleep(30)
    track_information = spotify.get_track_information(track_ids_list)
    #time.sleep(60)
    audio_features_df = spotify.get_track_audio_features(track_ids_list)
    #time.sleep(30)
    #write_pandas(sf_conn, playlist_df, "ALL_PLAYLISTS", auto_create_table = True)
    config.drop_and_recreate(sf_conn, playlist_df, "RAW_ALL_PLAYLISTS")
    config.drop_and_recreate(sf_conn, track_information, "RAW_TRACK_INFORMATION")
    config.drop_and_recreate(sf_conn, audio_features_df, "RAWAUDIO_FEATURES")
    write_pandas(sf_conn, playlist_df, "RAW_History_of_Playlists",  auto_create_table = True)
    write_pandas(sf_conn, audio_features_df, "RAW_History_of_Audio_Features",  auto_create_table = True)
    write_pandas(sf_conn, track_information, "RAW_History_of_Track_Information",  auto_create_table = True)
    #config.drop_and_recreate(sf_conn, track_information, "TRACK_INFORMATION")
    #write_pandas(sf_conn, track_information, "TRACK_INFORMATION",  auto_create_table = True)
    #write_pandas(sf_conn, audio_features_df, "AUDIO_FEATURES",  auto_create_table = True)
    config.close_sf_conn(sf_conn)

write_to_sf(access_token=access_token, playlist_ids = spotify_playlist_ids)
