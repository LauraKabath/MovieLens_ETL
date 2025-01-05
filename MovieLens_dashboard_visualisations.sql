
-- Graf 1 Aktivita používateľov počas mesiacov v roku podľa pohlavia
SELECT u.gender, d.month, COUNT(*) as ratings_trend
FROM fact_ratings r
JOIN dim_date d ON d.iddate = r.iddate
JOIN dim_users u ON r.idusers = u.idusers
GROUP BY u.gender, d.month
ORDER BY d.month;

-- Graf 2 Top 7 žánrov podľa priemerného hodnotenia 
SELECT g.genre_name, AVG(r.rating) AS avg_genre_rating
FROM fact_ratings r
JOIN dim_primary_genres g ON r.idprimarygenres = g.idprimarygenres
GROUP BY g.genre_name
ORDER BY avg_genre_rating DESC
LIMIT 7;

-- Graf 3 Najviac používané tagy (Top 15 tags)
SELECT t.tags, COUNT(*) AS tags_count
FROM fact_ratings r
JOIN dim_tags t ON r.idtags = t.idtags
GROUP BY t.tags
ORDER BY tags_count DESC
LIMIT 15; 

-- Graf 4 Aktivita používateľov počas dňa podľa zamestnaní
SELECT u.occupation_name AS occupation, t.am_pm, COUNT(*) AS ratings_count 
FROM fact_ratings r
JOIN dim_time t ON r.idtime = t.idtime
JOIN dim_users u ON r.idusers = u.idusers
GROUP BY occupation, t.am_pm
ORDER BY t.am_pm, ratings_count;

-- Graf 5 Top 9 filmov podľa počtu hodnotení vs. podľa priemerného hodnotenia
SELECT m.movie_title, AVG(r.rating) AS avg_movie_rating,
    COUNT(*) AS movie_ratings_count
FROM fact_ratings r
JOIN dim_movies m ON r.idmovies = m.idmovies
GROUP BY m.movie_title
ORDER BY movie_ratings_count DESC
LIMIT 9;