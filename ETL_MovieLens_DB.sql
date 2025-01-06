USE ROLE TRAINING_ROLE;
USE WAREHOUSE LION_WH;

-- Vytvorenie databazy
CREATE DATABASE LION_MOVIELENS_DB;
-- Vytvorenie schemy
CREATE SCHEMA LION_MOVIELENS_SCHEMA;

USE SCHEMA LION_MOVIELENS_SCHEMA;

-- Vytvorenie staging_tabuliek
CREATE OR REPLACE TABLE age_group_staging(
    id INT PRIMARY KEY,
    name VARCHAR(45)
);

CREATE OR REPLACE TABLE occupations_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE OR REPLACE TABLE users_staging(
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (age) REFERENCES age_group_staging(id),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);

CREATE OR REPLACE TABLE movies_staging(
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

CREATE OR REPLACE TABLE ratings_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at TIMESTAMP_NTZ,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

CREATE OR REPLACE TABLE tags_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(40000),
    created_at TIMESTAMP_NTZ,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

CREATE OR REPLACE TABLE genres_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE OR REPLACE TABLE genres_movies_staging(
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(id)
);

-- Vytvorenie stage
CREATE OR REPLACE STAGE LION_movielens_stage;

COPY INTO age_group_staging
FROM @LION_movielens_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO occupations_staging
FROM @LION_movielens_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO users_staging
FROM @LION_movielens_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ';' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO movies_staging
FROM @LION_movielens_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @LION_movielens_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ';' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO tags_staging
FROM @LION_movielens_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ';' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @LION_movielens_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_movies_staging
FROM @LION_movielens_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ';' SKIP_HEADER = 1);

-- Vytvorenie tabuliek dimenzii
CREATE OR REPLACE TABLE dim_primary_genres AS (
SELECT DISTINCT
    id AS idPrimaryGenres,
    name AS genre_name
FROM genres_staging
);

CREATE OR REPLACE TABLE dim_time AS (
SELECT DISTINCT
    TO_TIME(r.rated_at) AS idTime,
    TO_TIME(r.rated_at) AS time,
    HOUR(r.rated_at) AS hour,
    MINUTE(r.rated_at) AS minute,
    SECOND(r.rated_at) AS second,
    CASE 
            WHEN HOUR(r.rated_at) < 12 THEN 'am'
            ELSE 'pm'
        END AS am_pm 
FROM ratings_staging r
);

CREATE OR REPLACE TABLE dim_date AS (
SELECT DISTINCT
    CONCAT(YEAR(r.rated_at) , MONTH(r.rated_at), DAY(r.rated_at)) AS idDate,
    DATE(r.rated_at) AS date,
    YEAR(r.rated_at) AS year,
    MONTH(r.rated_at) AS month,
    DAY(r.rated_at) AS day,
    QUARTER(r.rated_at) AS quarter,
    CASE 
            DAYNAME(r.rated_at)
            WHEN 'Mon' THEN 'Monday'
            WHEN 'Tue' THEN 'Tuesday'
            WHEN 'Wed' THEN 'Wednesday'
            WHEN 'Thu' THEN 'Thursday'
            WHEN 'Fri' THEN 'Friday'
            WHEN 'Sat' THEN 'Saturday'
            WHEN 'Sun' THEN 'Sunday'
    END AS weekday 
FROM ratings_staging r
);

CREATE OR REPLACE TABLE dim_tags AS (
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY m.title) AS idTags,
    m.title AS movie_title,
    MIN(t.tags) AS tags,
    MIN(t.created_at) AS created_at
FROM tags_staging t
JOIN movies_staging m ON m.id = t.movie_id
GROUP BY m.title
);

CREATE OR REPLACE TABLE dim_users AS (
SELECT DISTINCT
    u.id AS idUsers,
    a.name AS age_group,
    u.gender AS gender,
    u.zip_code AS zip_code,
    o.name AS occupation_name
FROM users_staging u
JOIN age_group_staging a ON a.id = u.age
JOIN occupations_staging o ON o.id = u.occupation_id
);

CREATE OR REPLACE TABLE dim_movies AS (
SELECT DISTINCT
    m.id AS idMovies,
    m.title AS movie_title,
    m.release_year AS release_year,
    MIN(g.name) AS primary_genre,
    LISTAGG(g.name, ', ') WITHIN GROUP (
        ORDER BY
            g.name
    ) AS genres
FROM movies_staging m
JOIN genres_movies_staging gm ON m.id = gm.movie_id
JOIN genres_staging g ON gm.genre_id = g.id
GROUP BY m.id, m.title, m.release_year
);
-- Vytvorenie tabulky faktov
CREATE OR REPLACE TABLE fact_ratings AS (
SELECT
    r.id AS idRatings,
    ddate.iddate AS idDate,
    dtime.idtime AS idTime,
    dmovies.idmovies AS idMovies,
    dgenres.idprimarygenres AS idPrimaryGenres,
    duser.idusers AS idUsers,
    dtags.idtags AS idTags,
    r.rating AS rating,
    r.rated_at AS rated_at,
    (SELECT AVG(rating) FROM ratings_staging WHERE movie_id = r.movie_id) AS avg_movie_rating,
    (SELECT MEDIAN(rating) FROM ratings_staging WHERE movie_id = r.movie_id) AS median_movie_rating,
    (SELECT COUNT(*) FROM ratings_staging WHERE movie_id = r.movie_id) AS movie_ratings_count,
    (SELECT AVG(rating) FROM ratings_staging WHERE user_id = r.user_id) AS avg_user_rating,
    (SELECT COUNT(*) FROM ratings_staging WHERE user_id = r.user_id) AS user_ratings_count
FROM ratings_staging r
JOIN dim_movies dmovies ON r.movie_id = dmovies.idmovies
JOIN dim_primary_genres dgenres ON dmovies.primary_genre = dgenres.genre_name
JOIN dim_time dtime ON TO_TIME(r.rated_at) = dtime.idtime
JOIN dim_date ddate ON DATE(r.rated_at) = ddate.date
LEFT JOIN dim_tags dtags ON dmovies.movie_title = dtags.movie_title
JOIN dim_users duser ON r.user_id = duser.idusers
);
-- DROP staging tabuľky
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;