-- This script execute the first query of the project LasDivinas from CC5212-1 Otoño
--Debería estar acorde a las carpetas en hdfs , chequear con:
    --  hdfs dfs -ls /uhadoop/LasDivinas/
    -- deberías ver artists_sample.csv
    -- y tracks_sample.csv
-- This script execute the first query of the project LasDivinas from CC5212-1 Oto�o

-- hdfs dfs -ls /uhadoop/LasDivinas/
raw_tracks = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/tracks_sample_100000.csv'
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


raw_artists = LOAD 'hdfs://cm:9000/uhadoop2021/LasDivinas/artists_sample_100000.csv'
                          USING PigStorage('_')
                          AS (
                                        id:chararray,
                                        followers:int,
                                        genres:bag{},
                                        name:chararray,
                                        popularity:int
                                );

-- Después de que funcione el código para las samples cargar a la carpeta con:
    --scp -P 220 C:\Users\sofia\Documents\Universidad\SeptimoSemestre\PATOS\Proyecto\artists.csv uhadoop@cm.dcc.uchile.cl:/data/2021/uhadoop/LasDivinas/
    --scp -P 220 C:\Users\sofia\Documents\Universidad\SeptimoSemestre\PATOS\Proyecto\tracks.csv uhadoop@cm.dcc.uchile.cl:/data/2021/uhadoop/LasDivinas/
    --Lo anterior debería quedar guardado en la carpeta, revisar con:  
        -- cd /data/2021/uhadoop/LasDivinas/
    --LUEGO hacer copyFromLocal
        -- hdfs dfs -copyFromLocal artists.csv /uhadoop2021/LasDivinas/
        -- hdfs dfs -copyFromLocal tracks.csv /uhadoop2021/LasDivinas/
    -- y con eso cambiar las partes de LOAD de las lineas 6 y 9


--Separar bags de artists en filas distintas

--dump flat_tracks;


--energy, speechiness, acousticness, instrumentalness,  liveness, valence 
--se filtra por artistas mayores al promedio de popularidad
filterByPopularity= FILTER raw_artists BY popularity < 9.42 ;
moreThanAVGArtist= FOREACH filterByPopularity GENERATE id as artist_id, name as artist_name;

interestSongRaw= FILTER raw_tracks BY id=='5t4qhdEnUoVnTSZF1TSfCl';
interestSong= FOREACH interestSongRaw GENERATE  id, 
danceability AS danceability2,
energy AS energy2, 
speechiness AS speechiness2, 
acousticness AS acousticness2, 
instrumentalness AS instrumentalness2,  
liveness AS liveness2, 
valence AS valence2;
--dump moreThanAVGArtist
-- Acá cambiamos explicit en función de la canción seleccionada (0 o 1)
explicitSongs= FILTER raw_tracks By explicit==0  ;
flat_tracks = foreach raw_tracks generate name AS song_name, flatten(id_artist) as id_artist,
danceability, 
energy, 
speechiness, 
acousticness, 
instrumentalness,  
liveness, 
valence ;
--dump flat_tracks
--Acá la idea es hacer un join pero tengo problemas por el bag en donde se guarda id_artists
--HALP
explicitAndPopularityJOIN = JOIN flat_tracks BY id_artist, moreThanAVGArtist by artist_id;
explicitAndPopularity= FOREACH explicitAndPopularityJOIN GENERATE song_name, artist_name,
danceability,
energy, 
speechiness, 
acousticness, 
instrumentalness,  
liveness, 
valence ;

Final= CROSS explicitAndPopularity, interestSong;

--prueba = JOIN flat_tracks BY id_artist, raw_artists BY id;
--dump prueba
--dump explicitAndPopularityCond



STORE Final INTO 'hdfs://cm:9000/uhadoop2021/LasDivinas/explicitandpopularity/';