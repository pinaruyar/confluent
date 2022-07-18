--creating the first stream : no filreting
CREATE STREAM ARTIST_STREAM_FULL (title string KEY, artist string,  artistNationality string, objectBeginDate integer)
WITH (KAFKA_TOPIC='artworks', VALUE_FORMAT='JSON_SR', KEY_FORMAT='JSON_SR');

--creating FRENCH_STREAM
CREATE STREAM FRENCH_STREAM AS
SELECT * FROM  ARTIST_STREAM_FULL 
WHERE ARTISTNATIONALITY = 'French'
EMIT CHANGES;

--creating IMPRESSIONIST_STREAM
CREATE STREAM IMPRESSIONIST_STREAM AS
SELECT * FROM  FRENCH_STREAM 
WHERE OBJECTBEGINDATE < 1886 AND OBJECTBEGINDATE > 1867
EMIT CHANGES;
