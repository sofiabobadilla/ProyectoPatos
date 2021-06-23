raw_tracks = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/tracks_sample.csv' USING PigStorage('_') AS 
(id, name, popularity, duration_ms, explicit,artists, 
id_artists, release_date, danceability, energy, key, loudness, 
mode , speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature);

raw_artists = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/artists_sample.csv' USING PigStorage('_') AS
(id, followers, genres, name, popularity);


-- pasar de bags a filas (tracks)


-- filtrar solo colaboraciones (count(id) > 0, suponiendo que cada artista está en fila separada)
select_columns = FOREACH raw_tracks GENERATE id, id_artists;
group_songs = GROUP select_columns BY id;
artists_per_song = FOREACH group_songs GENERATE group, COUNT(id_artists) AS num_artists;
DUMP artists_per_song;

SPLIT artists_per_song INTO solos IF num_artists == 0, collabs IF num_artists > 0;
-- "solos" tiene las canciones con un solo artista, "collabs" las que tienen más de uno

-- generar tabla original filtrada con colaboraciones
mixed_collabs = JOIN select_columns BY id, collabs BY $0;
song_collabs = FOREACH mixed_collabs GENERATE id, id_artists;
DUMP song_collabs;

-- generar tabla original filtrada con solos
mixed_solos = JOIN select_columns BY id, solos BY $0;
song_solos = FOREACH mixed_solos GENERATE id, id_artists;
DUMP song_solos;

-- tupla (canción x artista 1 x artista 2)
song_collabs_alias = FOREACH song_collabs GENERATE $0 ..;
raw_collabs_paired = JOIN song_collabs BY id, song_collabs_alias BY id;
collabs_paired = FILTER raw_collabs_paired BY song_collabs::id_artists < song_collabs_alias::id_artists;
DUMP collabs_paired;

-- contar colaboraciones para cada par: (artista 1 x artista 2 x número de colaboraciones)
group_artists = GROUP collabs_paired BY (song_collabs::id_artists, song_collabs_alias::id_artists);
pairs_collabs_unsorted = FOREACH group_artists GENERATE FLATTEN(group), COUNT(id) AS num_collabs;
pairs_collabs = ORDER pairs_collabs_unsorted BY num_collabs DESC;
DUMP pairs_collabs;

-- obtener (artista x cantidad de artistas con los que ha colaborado x total de colaboraciones que ha hecho)
group_artist_1 = GROUP pairs_collabs BY $0;
collabs_by_artist_unsorted = FOREACH group_artist_1 GENERATE group, COUNT($1) AS num_artists_collabs, SUM($2) AS total_num_collabs;
cba_artist_sort = ORDER collabs_by_artist_unsorted BY num_artists_collabs, total_num_collabs;
cba_collab_sort = ORDER collabs_by_artist_unsorted BY total_num_collabs, num_artists_collabs;
DUMP cba_artist_sort;
DUMP cba_collab_sort;

-- PENDIENTE: agregar otros datos de los artistas (nombre, popularidad, género, seguidores)