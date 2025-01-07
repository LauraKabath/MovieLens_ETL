# **ETL proces datasetu MovieLens**

Daný repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z MovieLens datasetu. Zameranie projektu je na preskúmanie správania používateľov, ich preferencií v pozeraní filmov a hodnotení na základe demografických údajov a aktivít používateľov. Navrhnutý dátový model vo forme hviezdicovej schémy umožňuje efektívnu multidimenzionálnu analýzu a vizualizáciu kľúčových metrík, ako sú populárne žánre čí aktivita používateľov počas dňa.

---
## **1. Úvod a popis zdrojových dát**
Semestrálny projekt má za cieľ analyzovať dáta súvisiace s filmami, žánrami, používateľmi, ich hodnoteniami a tagmi. Analýzou tohto datasetu môžeme identifikovať používateľské preferencie, populárne filmy a správanie divákov.

 Z GroupLens datasetu, dostupného [tu] (https://grouplens.org/datasets/movielens/), sme získali zdrojové dáta. Získaný dataset obsahuje sedem hlavných tabuliek a jednu spojovaciu tabuľku:
- `movies`
-	`ratings`
-	`users`
-	`tags`
-	`age_group`
- `pccupations`
-	`genres`
-	`genres_movies`

Dáta sme pripravili, transformovali a sprístupnili pre viacdimenzionálnu analýzu prostredníctvom ETL procesu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
V relačnom modeli, ktorý je znázornený na entitno-relačnom diagrame (ERD), sú usporiadané surové dáta.

<p align="center">
  <img src="https://github.com/LauraKabath/MovieLens_ETL/blob/main/MovieLens_ERD_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Pre efektívnu analýzu bol navrhnutý hviezdicový model (star schema), kde faktová tabuľka fact_ratings predstavuje centrálny bod prepojený s nasledujúcimi dimenziami:
•	dim_movies: Obsahuje informácie o filmoch, ako je názov filmu, rok vydania, hlavný žáner (primary genre) a všetky žánre spojené s filmom.
-	**`dim_users`**: Uchováva demografické informácie o používateľoch vrátane pohlavia, vekovej kategórie a povolania.
-	**`dim_date`**: Poskytuje detailné informácie o dátume hodnotenia, ako je rok, mesiac, deň a deň v týždni.
-	**`dim_time`**: Obsahuje časové informácie, ako hodiny, minúty a sekundy, AM/PM.
-	**`dim_tags`**: Reprezentuje informácie o primárnych tagoch priradených k filmom.
-	**`dim_primaryGenres`**: Uchováva jednotlivé žánre filmov.

Na diagrame nižšie je znázornená štruktúra modelu hviezdy. V diagrame vidíme prepojenia medzi tabuľkou faktov a dimenziami, čo zjednodušuje implementáciu a pochopenie modelu.

<p align="center">
  <img src="https://github.com/LauraKabath/MovieLens_ETL/blob/main/MovieLens_star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

---
## **3. ETL proces v Snowflake**
Postup spracovania dát, ktorý sa používa na získanie, transformáciu a načítanie dát do dátového skladu sa nazýva ETL proces.  Pozostáva z troch hlavných fáz: `získanie/extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). S cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu, ktorý by bol vhodný na analýzu a vizualizáciu dát, sme implementovali tento proces v Snowflake.

---
### **3.1 Extract (Extrahovanie dát)**
Prostredníctvom interného stage s názvom `LION_movielens_stage` boli do Snowflake najprv nahraté dáta zo zdrojového datasetu vo formáte `.csv`. Stage slúži ako dočasné úložisko na import alebo export dát v Snowflake. Stage sme vytvorili príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE LION_movielens_stage;
```
Následne boli do stage nahraté súbory obsahujúce údaje o filmoch, používateľoch, žánroch, hodnoteniach, zamestnaní, vekových kategóriách a tagoch. Do staging tabuliek boli dáta importované pomocou príkazu COPY INTO. Pre každú tabuľku bol použitý podobný príkaz:

```sql
COPY INTO users_staging
FROM @LION_movielens_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ';' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

---
### **3.2 Transform (Transformácia dát)**

V tejto fáze bolo hlavným cieľom pripraviť faktovú tabuľku a dimenzie, ktoré umožnia jednoduchú a efektívnu analýzu. Dáta boli preto zo staging tabuliek vyčistené, transformované a obohatené.

Dimenzie boli navrhnuté tak, aby poskytovali kontext pre faktovú tabuľku. `Dim_movies` uchováva údaje o filmoch, ako názov, rok vydania, žánre a primárne žánre. Transformácia zahŕňala identifikáciu primárneho žánru filmu a uchovanie ostatných žánrov v reťazci. Táto dimenzia je typu SCD 1, čo nám umožňuje aktualizovať hodnoty, ako napríklad zmenu primárneho žánru.

```sql
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
```
Dimenzia `dim_primary_genres` obsahuje názvy žánrov. Názvy žánrov sú väčšinou stabilné a nemenia sa. Táto dimenzie je preto typu SCD 0.

```sql
CREATE OR REPLACE TABLE dim_primary_genres AS (
SELECT DISTINCT
    id AS idPrimaryGenres,
    name AS genre_name
FROM genres_staging
);
```
Podobne `dim_users` uchováva informácie o používateľoch vrátane vekových kategórii, zamestnania a pohlavia. Daná dimenzia je typu SCD 2, čo nám umožňuje sledovať historické zmeny v zamestnaní a veku používateľa.

```sql
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
```
Dimenzia `dim_tags` je navrhnutá tak, aby uchovávala názov filmu, jeho primárnu značku (tag) a čas vytvorenia značky. Tagy môžu byť upravované a aktualizované, preto bol pre danú dimenziu zvolený typ SCD 1.

```sql
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
```
Časová dimenzia `dim_time` je navrhnutá tak, aby uchovávala podrobné časové informácie o hodnotení filmov. Obsahuje odvodené údaje ako sú hodiny, minúty, sekundy a či sa jedná o dopoludňajší (am) alebo popoludňajší čas (pm). Časy sú považované za nemenné údaje, čiže `dim_time` je navrhnutá ako dimenzia typu SCD 0 s možnosťou rozširovania o nové záznamy podľa potreby.

```sql
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
```
Podobne aj dátumová dimenzia `dim_date` uchováva podrobné dátumové údaje o hodnoteniach filmov, ako sú  rok, mesiac, deň, štvrťrok a deň v týždni v textovom formáte. Štruktúra tejto dimenzie a dimenzie času nám umožňuje robiť podrobné časové analýzy, ako sú trendy hodnotení podľa rokov, mesiacov, dní v týždni alebo hodín v dni. Z hľadiska SCD je dátumová dimenzia typu SCD 0 podobne ako dimenzia časov. V našom aktuálnom modeli nie je potreba aktualizovať hodnoty alebo uchovávať históriu zmien, preto daný typ postačuje.

```sql
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
```
Na záver bola vytvorená tabuľka faktov `fact_ratings`, ktorá obsahuje záznamy o hodnoteniach a je prepojená na všetky dimenzie. Nachádzajú sa v nej kľúčové metriky, ako je hodnota hodnotenia, časový údaj, priemerné hodnotenie filmu, celkový počet hodnotení na film, medián hodnôt hodnotení na film, priemerné hodnotenie používateľa a celkový počet hodnotení daného používateľa.

```sql
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
```

---
### **3.3 Load (Načítanie dát)**

Dáta boli po úspešnom vytvorení dimenzií a tabuľky faktov nahraté do finálnej štruktúry. Staging tabuľky boli na záver odstránené, aby sa optimalizovalo využitie úložiska.

```sql
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
```
---
## **4 Vizualizácia dát**

