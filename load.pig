-- This script execute the first query of the project LasDivinas from CC5212-1 Otoño

raw_tracks = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/tracks_sample.csv' USING PigStorage('\t') AS (id, name, popularity, duration_ms, explicit,artists, id_artist, release_date, danceability, energy, key, loudness, mode , speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature);


raw_artists = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/artists_sample.csv' USING PigStorage('\t') AS (id, followers,genres,name,popularity);
