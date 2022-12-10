-- This script loads our datasets into pig latin's variables
-- It should be inside in our Hadoop server. You can check using the following commands.
    --  hdfs dfs -ls /uhadoop/LasDivinas/
    -- You should be able to see artists_sample.csv
    -- and tracks_sample.csv

-- This script execute the first query of the project LasDivinas from CC5212-1 Oto�o
-- enter and cache our files in our directory:
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

-- Once the code has loaded the samples, upload other folders with: (Path may change. This is Sofia's path)
-- The important path is from artists.csv and tracks.csv
    --scp -P 220 C:\Users\sofia\Documents\Universidad\SeptimoSemestre\PATOS\Proyecto\artists.csv uhadoop@cm.dcc.uchile.cl:/data/2021/uhadoop/LasDivinas/
    --scp -P 220 C:\Users\sofia\Documents\Universidad\SeptimoSemestre\PATOS\Proyecto\tracks.csv uhadoop@cm.dcc.uchile.cl:/data/2021/uhadoop/LasDivinas/
    -- This should be now in the content of the folder, check with:  
        -- cd /data/2021/uhadoop/LasDivinas/
    -- Use copyFromLocal to send data from normal Operative System to Hadoop multidisk partition:
        -- hdfs dfs -copyFromLocal artists.csv /uhadoop2021/LasDivinas/
        -- hdfs dfs -copyFromLocal tracks.csv /uhadoop2021/LasDivinas/


-- Some values we are interested in
-- energy, speechiness, acousticness, instrumentalness,  liveness, valence 
-- We filter by artist whose popularity is more than the average
-- filterByPopularity is just a filter, we need to turn it into a Apache Pig's Bag ( some kind of array or datatable )
filterByPopularity= FILTER raw_artists BY popularity < 9.42 ;
moreThanAVGArtist= FOREACH filterByPopularity GENERATE id as artist_id, name as artist_name;

-- we check our data by looking at one song, filtering by id
interestSongRaw= FILTER raw_tracks BY id=='5t4qhdEnUoVnTSZF1TSfCl';
-- We need to rename columns for it to work fine
interestSong= FOREACH interestSongRaw GENERATE  id, 
danceability AS danceability2,
energy AS energy2, 
speechiness AS speechiness2, 
acousticness AS acousticness2, 
instrumentalness AS instrumentalness2,  
liveness AS liveness2, 
valence AS valence2;

-- Print more Than AVG artists
--dump moreThanAVGArtist


-- Consider only explicit songs
explicitSongs= FILTER raw_tracks By explicit==0  ;
flat_tracks = FOREACH raw_tracks GENERATE name AS song_name, flatten(id_artist) as id_artist,
danceability, 
energy, 
speechiness, 
acousticness, 
instrumentalness,  
liveness, 
valence ;

-- We can show these values by writing the following line
--dump flat_tracks

-- Performing a join between tracks and artist, considering tracks by popular artists
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


-- Save our result
STORE Final INTO 'hdfs://cm:9000/uhadoop2021/LasDivinas/explicitandpopularity/';