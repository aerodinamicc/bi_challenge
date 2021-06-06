----
-- Accident_severity
----
DROP TABLE IF EXISTS dim_accident_severity CASCADE;

CREATE TABLE dim_accident_severity (
	accident_severity_id INT PRIMARY KEY,
	accident_severity VARCHAR(10)
);

INSERT INTO dim_accident_severity (accident_severity_id, accident_severity)
VALUES (1, 'Slight'),
		(2, 'Serious'),
		(3, 'Fatal');
		
----
-- Age bands
----
DROP TABLE IF EXISTS dim_age_band_of_driver CASCADE;
		
CREATE TABLE dim_age_band_of_driver (
	age_band_of_driver_id INT PRIMARY KEY,
	age_band_of_driver VARCHAR(30)
);

INSERT INTO dim_age_band_of_driver (age_band_of_driver_id, age_band_of_driver)
VALUES (1, '0 - 5'),
		(2, '6 - 10'),
		(3, '11 - 15'),
		(4, '16 - 20'),
		(5, '21 - 25'),
		(6, '26 - 35'),
		(7, '36 - 45'),
		(8, '46 - 55'),
		(9, '56 - 65'),
		(10, '66 - 75'),
		(11, 'Over 75'),
		(0, 'Data missing or out of range');
		
-----
-- Area type
-----
DROP TABLE IF EXISTS dim_driver_home_area_type CASCADE;
		
CREATE TABLE dim_driver_home_area_type (
	driver_home_area_type_id INT PRIMARY KEY,
	driver_home_area_type VARCHAR(30)
);

INSERT INTO dim_driver_home_area_type (driver_home_area_type_id, driver_home_area_type)
VALUES (1, 'Urban area'),
		(2, 'Small town'),
		(3, 'Rural'),
		(0, 'Data missing or out of range');
		
----
-- Journey purpose
----

DROP TABLE IF EXISTS dim_journey_purpose_of_driver CASCADE;

CREATE TABLE dim_journey_purpose_of_driver (
	journey_purpose_of_driver_id INT PRIMARY KEY,
	journey_purpose_of_driver VARCHAR(30)
);

INSERT INTO dim_journey_purpose_of_driver (journey_purpose_of_driver_id, journey_purpose_of_driver)
VALUES (1, 'Commuting to/from work'),
		(2, 'Taking pupil to/from school'),
		(3, 'Pupil riding to/from school'),
		(4, 'Journey as part of work'),
		(5, 'Other'),
		(6, 'Not known'),
		(7, 'Other/Not known (2005-10)'),
		(0, 'Data missing or out of range');

----
-- Fact table
----
DROP TABLE IF EXISTS recent_accidents;

CREATE TABLE recent_accidents (
	accident_index 				    VARCHAR(20),
	accident_severity_id 		    INT REFERENCES dim_accident_severity (accident_severity_id),
	date 						    DATE,
	day_of_week 				    VARCHAR(10),
	age_band_of_driver_id		    INT REFERENCES dim_age_band_of_driver (age_band_of_driver_id),
	age_of_vehicle 				    FLOAT,
	driver_home_area_type_id	    INT REFERENCES dim_driver_home_area_type (driver_home_area_type_id),
	journey_purpose_of_driver_id	INT REFERENCES dim_journey_purpose_of_driver (journey_purpose_of_driver_id)
);