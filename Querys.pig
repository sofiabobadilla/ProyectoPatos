-- This script execute the first query of the project LasDivinas from CC5212-1 Otoño

raw_tracks = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/tracks_sample.csv' USING PigStorage('\t') AS (id, name, popularity, duration_ms, explicit,artists, id_artist, release_date, danceability, energy, key, loudness, mode , speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output


raw_artists = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/artists_sample.csv' USING PigStorage('\t') AS (id, followers,genres,name,popularity);


correctMovies= FILTER raw_roles BY type == 'THEATRICAL_MOVIE';

good_movies_raw = FILTER raw_ratings BY type == 'THEATRICAL_MOVIE';

roles= FOREACH correctMovies GENERATE star, gender, CONCAT(title, '##' , year, '##', num) AS a_key;


gm_rows = FOREACH good_movies_raw GENERATE  CONCAT(title, '##', year, '##', num) AS gm_key , votes, score ;


movie_roles_pairs = JOIN roles BY a_key LEFT OUTER, gm_rows by gm_key;

actors_raw = FILTER movie_roles_pairs BY gender =='MALE';

actors_and_gm = FOREACH actors_raw GENERATE star,  a_key, (score >= 7.8 AND votes >= 10001 ? 1 : 0) AS gm;

actors_grouped = GROUP actors_and_gm BY star; 

actors= FOREACH actors_grouped GENERATE flatten(group), SUM(actors_and_gm.gm) AS suma;

correction = FOREACH actors GENERATE flatten(group), (suma > 0 ? suma : 0) AS count;

actors_final = ORDER correction BY count DESC;
  
actress_raw = FILTER movie_roles_pairs BY gender =='FEMALE';

actress_and_gm = FOREACH actress_raw GENERATE star,  a_key, (score >= 7.8 AND votes >= 10001 ? 1 : 0) AS gm;

actress_grouped = GROUP actress_and_gm BY star; 

actress= FOREACH actress_grouped GENERATE flatten(group), SUM(actress_and_gm.gm) AS suma;
correction = FOREACH actress GENERATE flatten(group), (suma > 0 ? suma : 0) AS count;

actress_final = ORDER correction BY count DESC;

    

STORE actors_final INTO 'hdfs://cm:9000/uhadoop2021/group19/actor_topStars/';
STORE actress_final INTO 'hdfs://cm:9000/uhadoop2021/group19/actress_topStars/';
--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
