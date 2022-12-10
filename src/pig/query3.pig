-- This script execute the third query of the project LasDivinas from CC5212-1 Otoño

-- hdfs dfs -ls /uhadoop2021/LasDivinas/
raw_tracks = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/tracks_sample.csv' 
			 USING PigStorage('_') 
			 AS (
					id:chararray, 
					name:chararray, 
					popularity:int, 
					duration_ms:int, 
					explicit:int,
					artists:bag{}, 
					id_artist:bag{}, 
					release_date:datetime, 
					danceability:float, 
					energy:float, 
					key:int, 
					loudness:float, 
					mode:int, 
					speechiness:float, 
					acousticness:float, 
					instrumentalness:float, 
					liveness:float, 
					valence:float, 
					tempo:float, 
					time_signature:int
				);


raw_artists = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/artists_sample.csv' 
			  USING PigStorage('_') 
			  AS (
			  		id:chararray, 
			  		followers:int,
			  		genres:bag{},
			  		name:chararray,
			  		popularity:int
			  	);

--Separar bags de artists en filas distintas
flat = FOREACH raw_tracks GENERATE name, popularity, FLATTEN(artists) AS artist;
DUMP flat;

-- filtrar solo colaboraciones (count(id) > 0, suponiendo que cada artista está en fila separada)
select_columns = FOREACH flat GENERATE name, artist;
group_songs = GROUP select_columns BY name;
artists_per_song = FOREACH group_songs GENERATE FLATTEN($0) AS name, COUNT($1) AS num_artists;
DUMP artists_per_song;

SPLIT artists_per_song INTO solos IF (long)num_artists == 1, collabs IF (long)num_artists > 1;
-- "solos" tiene las canciones con un solo artista, "collabs" las que tienen más de uno

-- generar tabla original filtrada con colaboraciones
mixed_collabs = JOIN select_columns BY name, collabs BY name;
song_collabs = FOREACH mixed_collabs GENERATE select_columns::name, select_columns::artist;
DUMP song_collabs;

-- generar tabla original filtrada con solos
mixed_solos = JOIN select_columns BY name, solos BY name;
song_solos = FOREACH mixed_solos GENERATE select_columns::name, select_columns::artist;
DUMP song_solos;

-- tupla (canción x artista 1 x artista 2)
song_collabs_alias = FOREACH song_collabs GENERATE name AS name2, artist AS artist2;
raw_collabs_paired = JOIN song_collabs BY name, song_collabs_alias BY name2;
filtered_collabs_paired = FILTER raw_collabs_paired BY artist < artist2;
collabs_paired = FOREACH filtered_collabs_paired GENERATE name, artist, artist2;
DUMP collabs_paired;

-- contar colaboraciones para cada par: (artista 1 x artista 2 x número de colaboraciones)
group_artists = GROUP collabs_paired BY (artist, artist2);
pairs_collabs_unsorted = FOREACH group_artists GENERATE FLATTEN($0), COUNT($1) AS num_collabs;
pairs_collabs = ORDER pairs_collabs_unsorted BY num_collabs DESC;
DUMP pairs_collabs;
-- pairs_collabs es final

-- obtener (artista x cantidad de artistas con los que ha colaborado)
group_artist1 = GROUP collabs_paired BY artist;
collabs_by_artist1 = FOREACH group_artist1 GENERATE FLATTEN($0) AS artist, COUNT($1) AS num_artists_collabs;
-- obtener al revés
group_artist2 = GROUP collabs_paired BY artist2;
collabs_by_artist2 = FOREACH group_artist2 GENERATE FLATTEN($0) AS artist, COUNT($1) AS num_artists_collabs;
-- unir, agrupar y sumar
collabs_union = UNION collabs_by_artist1, collabs_by_artist2;
collabs_grouped = GROUP collabs_union BY artist;
collabs_sum = FOREACH collabs_grouped GENERATE group AS artist, SUM(collabs_union.num_artists_collabs) AS num_artists_collabs; 
collabs_artists = ORDER collabs_sum BY num_artists_collabs DESC;
-- collabs_artists es final

-- obtener (artista x total de colaboraciones)
group_song_artist = GROUP song_collabs BY artist;
song_collabs_by_artist_unsorted = FOREACH group_song_artist GENERATE FLATTEN($0) AS artist, COUNT($1) AS num_songs_collabs;
song_collabs_by_artist = ORDER song_collabs_by_artist_unsorted BY num_songs_collabs DESC;
DUMP song_collabs_by_artist;
-- song_collabs_by artist es final

-- STORE pairs_collabs INTO 'hdfs://cm:9000/uhadoop2021/LasDivinas/pairs_collabs/';
-- STORE collabs_artists INTO 'hdfs://cm:9000/uhadoop2021/LasDivinas/num_artists_collabs/';
-- STORE song_collabs_by_artist INTO 'hdfs://cm:9000/uhadoop2021/LasDivinas/num_songs_collabs/';

-- PENDIENTE QUIZÁ: agregar otros datos de los artistas (nombre, popularidad, género, seguidores)
