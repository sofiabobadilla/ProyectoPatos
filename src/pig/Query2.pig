-- This script execute the first query of the project LasDivinas from CC5212-1 Otoño
--Debería estar acorde a las carpetas en hdfs , chequear con:
    --  hdfs dfs -ls /uhadoop/LasDivinas/
    -- deberías ver artists_sample.csv
    -- y tracks_sample.csv
raw_tracks = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/tracks_sample.csv' USING PigStorage('_') AS 
(id, name, popularity, duration_ms, explicit,artists, 
id_artists, release_date, danceability, energy, key, loudness, 
mode , speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature);




raw_artists = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/artists_sample.csv' USING PigStorage('_') AS 
(id, followers,genres,name,popularity);
-- Después de que funcione el código para las samples cargar a la carpeta con:
    --scp -P 220 C:\Users\sofia\Documents\Universidad\SeptimoSemestre\PATOS\Proyecto\artists.csv uhadoop@cm.dcc.uchile.cl:/data/2021/uhadoop/LasDivinas/
    --scp -P 220 C:\Users\sofia\Documents\Universidad\SeptimoSemestre\PATOS\Proyecto\tracks.csv uhadoop@cm.dcc.uchile.cl:/data/2021/uhadoop/LasDivinas/
    --Lo anterior debería quedar guardado en la carpeta, revisar con:  
        -- cd /data/2021/uhadoop/LasDivinas/
    --LUEGO hacer copyFromLocal
        -- hdfs dfs -copyFromLocal artists.csv /uhadoop/LasDivinas/
        -- hdfs dfs -copyFromLocal tracks.csv /uhadoop/LasDivinas/
    -- y con eso cambiar las partes de LOAD de las lineas 6 y 9


-- llegará un artista con x características

--se filtra por artistas con el mismo género musical
filterByGenres= FILTER raw_artists BY genres=='' ;

tracksGroupByArtist= GROUP raw_tracks By artists;
tracksGroupByArtistANDAvg=
moreThanAVGArtist= FOREACH filterByGenres GENERATE id , AVG(danceability), AVG(energy), AVG(instrumentalness), AVG(liveness);


--Acá la idea es hacer un join pero tengo problemas por el bag en donde se guarda id_artists
--HALP
flatt1= FOREACH explicitSongs GENERATE FLATTEN(id_artists);
flatt2= FOREACH flatt1 GENERATE FLATTEN(artists);
explicitAndPopularityCond = JOIN flatt2 BY id_artists LEFT OUTER, moreThanAVGArtist by id;

dump moreThanAVGArtist


--STORE explicitAndPopularityCond INTO 'hdfs://cm:9000/uhadoop2021/LasDivinas/explicitandpopularity/';