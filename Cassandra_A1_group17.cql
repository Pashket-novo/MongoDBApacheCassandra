/////////////////
//  Cassandra //
////////////////

////////////////////////////////////////////////////////
//  C.1. Creating the Databases and Importing Data.  //
//////////////////////////////////////////////////////

// 5. Create a keyspace called FIT5137A1_MRDB for the Cassandra database, with
// SimpleStrategy and replication factor of 1.

// creating keyspace
CREATE KEYSPACE FIT5137A1_MRDB
WITH replication = {'class': 'SimpleStrategy',
'replication_factor' : 1};

// showing exisitng keyspaces
DESCRIBE KEYSPACES;

// showing information about created keyspace
DESCRIBE FIT5137A1_MRDB;

// switching to the created keyspace
USE FIT5137A1_MRDB;

// 6. Create the following column families using appropriate data types :
// a. user_ratings,

// Creating appropriate user defined types

// Creating data type for user personal traits
CREATE TYPE user_personal_traits(
birth_year int,
weight int,
height float,
marital_status text);

// Creating data type for user personality
CREATE TYPE user_personality (
interest text,
type_of_worker text,
fav_color text,
drink_level text);

// Creating data type for user preferences
CREATE TYPE user_preferences (
budget text,
smoker Boolean,
dress_preference text,
ambience text,
transport text);

// Creating data type for user other demographics
CREATE TYPE user_other_demographics (
religion text,
employment text);

// Creating column family user_ratings
CREATE TABLE user_ratings (rating_id int,
user_id int,
place_id int,
rating_place int,
rating_food int,
rating_service int,
user_personal_traits frozen<user_personal_traits>,
user_personality frozen<user_personality>,
user_preferences frozen<user_preferences>,
user_other_demographics frozen<user_other_demographics>,
user_fav_cuisines set<text>,
user_fav_payment_method set<text>,
PRIMARY KEY (rating_id, user_id)
);

// showing information about column family user_ratings
DESCRIBE TABLE user_ratings;

// b. place_ratings

// Creating data type for place address
CREATE TYPE place_address (
street text,
city text,
state text,
country text);

// Creating data type for place features
CREATE TYPE place_features (
alcohol text,
smoking_area text,
dress_code text,
accessibility text,
price text,
franchise text,
area text,
other_services text);

// Creating column family place_ratings
CREATE TABLE place_ratings (rating_id int,
user_id int,
place_id int,
rating_place int,
rating_food int,
rating_service int,
place_name text,
place_address frozen <place_address>,
place_features frozen <place_features>,
parking_arrangements text,
accepted_payment_modes set<text>,
cuisines set<text>,
PRIMARY KEY (rating_id, place_id)
) ;

// showing information about created column family place_ratings
DESCRIBE place_ratings;

// show created column families
DESCRIBE TABLES;

// 7. Using the cassandra COPY command import the following data into the tables:
// a. user_ratings.csv into the user_ratings table.
// b. place_ratings.csv into the place_ratings table.

// a
// importing data into user_ratings
COPY FIT5137A1_MRDB.user_ratings (rating_id,
user_id,
place_id,
rating_place,
rating_food,
rating_service,
user_personal_traits,
user_personality,
user_preferences,
user_other_demographics,
user_fav_cuisines,
user_fav_payment_method) FROM 'documents/fit5137/user_ratings.csv' WITH DELIMITER=',' AND HEADER=TRUE;

// showing inserted information
SELECT * FROM user_ratings;

// b
// importing data into place_ratings
COPY FIT5137A1_MRDB.place_ratings (rating_id,
user_id,
place_id,
rating_place,
rating_food,
rating_service,
place_name,
place_address,
place_features,
parking_arrangements,
accepted_payment_modes,
cuisines) FROM 'documents/fit5137/place_ratings.csv' WITH DELIMITER=',' AND HEADER=TRUE;

// showing inserted information
SELECT * FROM place_ratings;

/////////////////////////////////////
//  C.2. Modifying the Databases. //
///////////////////////////////////

// 4. To be consistent with the changes made in Task C.2 (1), (2), and (3), the management
// has also requested to update the reviews information to reflect the changes made to
// the users 1108 and remove the user 1063’s reviews. They have looked up the data in
// the reviews table and have provided the information in table 2.

// queries to show data for user 1063 existing in the column family user_ratings
SELECT * FROM user_ratings WHERE user_id = 1063 and rating_id = 137;
SELECT * FROM user_ratings WHERE user_id = 1063 and rating_id = 138;
SELECT * FROM user_ratings  WHERE user_id = 1063 and rating_id = 139;
SELECT * FROM user_ratings  WHERE user_id = 1063 and rating_id = 140;
SELECT * FROM user_ratings  WHERE user_id = 1063 and rating_id = 141;

// queries to show data for user 1063 existing in the column family place_ratings
SELECT * FROM place_ratings WHERE rating_id = 137;
SELECT * FROM place_ratings WHERE rating_id = 138;
SELECT * FROM place_ratings WHERE rating_id = 139;
SELECT * FROM place_ratings WHERE rating_id = 140;
SELECT * FROM place_ratings WHERE rating_id = 141;

// delete operations for user 1063 in the column family user_ratings
DELETE FROM user_ratings  WHERE user_id = 1063 and rating_id = 137;
DELETE FROM user_ratings  WHERE user_id = 1063 and rating_id = 138;
DELETE FROM user_ratings  WHERE user_id = 1063 and rating_id = 139;
DELETE FROM user_ratings  WHERE user_id = 1063 and rating_id = 140;
DELETE FROM user_ratings  WHERE user_id = 1063 and rating_id = 141;

// delete operations for user 1063 in the column family place_ratings
DELETE FROM place_ratings WHERE rating_id = 137;
DELETE FROM place_ratings WHERE rating_id = 138;
DELETE FROM place_ratings WHERE rating_id = 139;
DELETE FROM place_ratings WHERE rating_id = 140;
DELETE FROM place_ratings WHERE rating_id = 141;

// queries to show that data for user 1063 no longer exist in the column family user_ratings
SELECT * FROM user_ratings WHERE user_id = 1063 and rating_id = 137;
SELECT * FROM user_ratings WHERE user_id = 1063 and rating_id = 138;
SELECT * FROM user_ratings  WHERE user_id = 1063 and rating_id = 139;
SELECT * FROM user_ratings  WHERE user_id = 1063 and rating_id = 140;
SELECT * FROM user_ratings  WHERE user_id = 1063 and rating_id = 141;

// queries to show that data for user 1063 no longer exist in the column family place_ratings
SELECT * FROM place_ratings WHERE rating_id = 137;
SELECT * FROM place_ratings WHERE rating_id = 138;
SELECT * FROM place_ratings WHERE rating_id = 139;
SELECT * FROM place_ratings WHERE rating_id = 140;
SELECT * FROM place_ratings WHERE rating_id = 141;

// queries to show current data for user 1108 in the column family user_ratings
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 65;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 66;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 67;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 68;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 69;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 70;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 71;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 72;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 73;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 74;

// queries to update information about user 1108 in the column family user_ratings
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 65;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 66;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 67;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 68;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 69;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 70;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 71;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 72;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 73;
UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food' },
user_fav_payment_method = user_fav_payment_method - {'cash'},
user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE user_id = 1108 and rating_id = 74;

// queries to show updated information for user 1108 in the column family user_ratings
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 65;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 66;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 67;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 68;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 69;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 70;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 71;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 72;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 73;
SELECT * FROM user_ratings  WHERE user_id = 1108 and rating_id = 74;

// 5. It was also seen that user 1060 has reviewed Taco Jacks (ie. the new place with place
// id 70000), therefore using the information from table 3, insert the following data: (for
// this insert only you may look up the details of user 1060).

// creating index on user_id in user_ratings column family
CREATE INDEX ON user_ratings ( user_id );

// creating index on user_id in place_ratings column family
CREATE INDEX ON place_ratings ( user_id );

// query to lookup user 1060 data
SELECT * FROM user_ratings WHERE user_id = 1060;

// insert query into user_ratings column family
INSERT INTO user_ratings (rating_id,
                                user_id,
                                 place_id,
                                 rating_place,
                                 rating_food,
                                 rating_service,
                                 user_personal_traits,
                                 user_personality,
                                 user_preferences,
                                 user_other_demographics,
                                 user_fav_cuisines,
                                 user_fav_payment_method )
VALUES (7777, 1060, 70000, 2, 1, 2, {birth_year: 1991, weight: 82, height: 1.84, marital_status: 'single'}, {interest: 'technology', type_of_worker: 'thrifty-protector', fav_color: 'blue', drink_level: 'casual drinker'}, {budget: 'medium', smoker: FALSE, dress_preference: 'formal', ambience: 'family', transport: 'public'},{religion: 'Catholic', employment: 'student'},{'Burgers', 'Cafeteria', 'Pizzeria', 'Juice', 'American', 'Tex-Mex', 'Spanish', 'Mexican', 'Fast_Food', 'Cafe-Coffee_Shop', 'Soup', 'Hot_Dogs', 'Italian'},{'cash'}
);

// query to lookup user 1060 data
SELECT * FROM place_ratings WHERE user_id = 1060;

// insert query into place_ratings column family
INSERT INTO place_ratings (rating_id,
                                user_id,
                                 place_id,
                                 rating_place,
                                 rating_food,
                                 rating_service,
                                 place_name,
                                 place_address,
                                 place_features,
                                 parking_arrangements,
                                 accepted_payment_modes,
                                 cuisines )
VALUES (7777, 1060, 70000, 2, 1, 2, 'Taco Jacks', {street: 'Carretera Central Sn' ,
city: 'San Luis Potosi' , state:'SLP' , country: 'Mexico'},
{alcohol: 'No_Alcohol_Served', smoking_area: 'not permitted',
 dress_code: 'informal', accessibility: 'completely',
 price: 'medium', franchise: 't', area: 'open',
 other_services: 'Internet'}, 'none', {'any'}, {'Mexican','Burgers'}
);


/////////////////////////////////////
//  C.3. Querying the Management. //
///////////////////////////////////

// 3. How many reviews were made in the database?

SELECT COUNT(rating_id) from user_ratings;

// 4. How many reviews were created by places having public parking arrangements?

// creating index on parking_arrangements column in place_ratings column family
CREATE INDEX ON place_ratings (parking_arrangements);

// answer to the question
SELECT COUNT (*) FROM place_ratings WHERE parking_arrangements = 'public';

// query to show the data, which were counted in the column family place_ratings
SELECT * FROM place_ratings WHERE parking_arrangements = 'public';

// 5. Display the user id, rating of places and the user’s personality for all users who are
// interested in technology, thrifty protector types of workers, with blue as their favorite
// colour and are casual drinkers.
// (Note that the number of rows returned should be greater than zero)

// creating index on user_personality in user_ratings column family
CREATE INDEX ON user_ratings(user_personality);

// question query
SELECT user_id,
       rating_place,
       user_personality
FROM user_ratings
WHERE user_personality = {interest : 'technology',
      type_of_worker : 'thrifty-protector',
      fav_color : 'blue',
      drink_level : 'casual drinker'};

// 6. What are the place ids and ratings for food for places serving only Pizzeria cuisine?
// (Note the result should include cuisine).

// creating index on cuisines column in place_ratings column family
CREATE INDEX ON place_ratings (cuisines);

// answer to the question
SELECT place_id, rating_place, rating_food, rating_service, cuisines
FROM place_ratings WHERE cuisines  CONTAINS 'Pizzeria';

// 10. Display the average place rating, average food rating, and average service rating for
// puesto de tacos. Show the average values in decimal points.

// creating index on place_name column in place_ratings column family
CREATE INDEX ON place_ratings (place_name);

// querry to show data for puesto de tacos in the column family place_ratings
SELECT * FROM place_ratings WHERE place_name = 'puesto de tacos';

// answer to the question
SELECT AVG(CAST(rating_place as float)) AS avg_place_rating,
AVG(CAST(rating_food as float)) AS avg_food_rating,
AVG(CAST(rating_service as float)) AS avg_rating_service
FROM place_ratings WHERE place_name = 'puesto de tacos';

// 12. For each user whose favourite cuisine is Family, display the place ID, the place rating,
// the food rating and the user’s budget.

// creating index on user_fav_cuisines column in user_ratings column family
CREATE INDEX ON user_ratings (user_fav_cuisines);

// answer to the question
SELECT place_id, rating_place, rating_food, user_preferences.budget as user_budget
FROM user_ratings WHERE user_fav_cuisines CONTAINS 'Family';

//Own questions

///////////////////////////
//  additional queries  //
/////////////////////////

// 17. Find the places with zero rating in all three categories.
// Show user_id, place_id, rating_place, rating_food, rating_service,
// place_name and cuisines in that restaurant.

// answer to the question
SELECT user_id, place_id, rating_place, rating_food, rating_service, place_name, cuisines
FROM place_ratings
WHERE rating_place = 0 and rating_food = 0 and rating_service = 0 ALLOW FILTERING;

// 20. What are the max and min ratings of restaurants who accept “VISA” as a payment.

// creating index on accepted_payment_modes column in place_ratings column family
CREATE INDEX ON place_ratings (accepted_payment_modes);

// answer to the question
SELECT MAX(rating_place) AS max_place_rating,
    MAX(rating_food) AS max_food_rating,
    MAX(rating_service) AS max_service_rating,
    MIN(rating_place) AS min_place_rating,
    MIN(rating_food) AS min_food_rating,
    MIN(rating_service) AS min_service_rating
FROM place_ratings
WHERE accepted_payment_modes CONTAINS 'VISA';
