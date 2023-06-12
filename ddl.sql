CREATE DATABASE MOVIES;

CREATE TABLE MOVIES.basics (
        tconst VARCHAR(50) PRIMARY KEY,
		titleType VARCHAR(20),
		CHECK (titleType = 'movie'), 
		primaryTitle VARCHAR(100),  
		originalTitle VARCHAR(100), 
		isAdult BOOLEAN, 
		startYear YEAR, 
		endYear YEAR, 
		runtimeMinutes VARCHAR(20), 
		genres SET(Short,Drama,Comedy,Documentary,Adult, Action,Romance,Thriller,Animation,Family,Crime,Horror,Music,Adventure,Fantasy,Sci-Fi,Mystery,Biography,Sport,History,Musical,Western,War,Reality-TV,News,Talk-Show,Game-Show,Film-Noir,Lifestyle,Experimental,Commercial)
	);

CREATE TABLE MOVIES.ratings (
  		tconst VARCHAR(50) PRIMARY KEY, 
		averageRating VARCHAR(100),
		numVotes INT
	);

CREATE TABLE MOVIES.principals (
        tconst VARCHAR(50) FOREIGN KEY, 
        ordering INT, 
        nconst VARCHAR(100) primary key, 
        category VARCHAR(100), 
        job VARCHAR(100), 
        characters VARCHAR(100), 
    );