import pandas as pd 
import psycopg2 as psy
import matplotlib.pyplot as plt
import numpy as np

data1 = pd.read_csv(r'circuits.csv')
df = pd.DataFrame(data1)
df1 = df.drop_duplicates()

data2 = pd.read_csv(r'constructors.csv')
df = pd.DataFrame(data2)
df2 = df.drop_duplicates()

data3 = pd.read_csv(r'drivers.csv')
df = pd.DataFrame(data3)
df3 = df.drop_duplicates()

data4 = pd.read_csv(r'pit_stops.csv')
df = pd.DataFrame(data4)
df4 = df.drop_duplicates()

data5 = pd.read_csv(r'qualifying.csv')
df = pd.DataFrame(data5)
df5 = df.drop_duplicates()

data6 = pd.read_csv(r'races.csv')
df = pd.DataFrame(data6)
df6 = df.drop_duplicates()

data7 = pd.read_csv(r'results.csv')
df = pd.DataFrame(data7)
df7 = df.drop_duplicates()
df7 = df7.fillna(0)

data8 = pd.read_csv(r'status.csv')
df = pd.DataFrame(data8)
df8 = df.drop_duplicates()

co=None
try:
    co = psy.connect(host='berlin',
                    database='dbtilevadoux',
                    user='tilevadoux',
                    password='')
    curs = co.cursor()

    curs.execute('''DROP TABLE IF EXISTS Resultats''')
    curs.execute('''DROP TABLE IF EXISTS Courir;''')
    curs.execute('''DROP TABLE IF EXISTS PitStop;''')
    curs.execute('''DROP TABLE IF EXISTS Constructeur;''')
    curs.execute('''DROP TABLE IF EXISTS Pilotes;''')
    curs.execute('''DROP TABLE IF EXISTS Status;''')
    curs.execute('''DROP TABLE IF EXISTS Course;''')
    curs.execute('''DROP TABLE IF EXISTS Circuit;''')

    curs.execute('''CREATE TABLE Circuit(
                    id  numeric(4) PRIMARY KEY,
                    nom varchar(50),
                    localisation varchar(50),
                    pays varchar(30),
                    latitude numeric(8,5),
                    longitude numeric(8,5),
                    altitude varchar(4)
                    );''')

    for row in df1.itertuples():
        curs.execute('''INSERT INTO Circuit VALUES (%s,%s,%s,%s,%s,%s,%s);''',                                                                              
                        (row.circuitId,row.name,row.location,row.country,row.lat,row.lng,row.alt))

    co.commit()


    curs.execute('''CREATE TABLE Constructeur(
                    constructeurId numeric(3) PRIMARY KEY,
                    nom varchar(30),
                    nationalité varchar(30)
                    );''')

    for row in df2.itertuples():
            curs.execute('''INSERT INTO Constructeur VALUES (%s,%s,%s);''',
                            (row.constructorId,row.name,row.nationality))


    curs.execute('''CREATE TABLE Pilotes(
                    reference numeric(3) PRIMARY KEY,
                    numero varchar(3),
                    prenom varchar(30),
                    nom varchar(30),
                    dateNaissance date,
                    nationalite varchar(30)
                    );''')

    for row in df3.itertuples():
        curs.execute('''INSERT INTO Pilotes VALUES (%s,%s,%s,%s,%s,%s);''',                                                                              
                        (row.driverId,row.number,row.forename,row.surname,row.dob,row.nationality))


    curs.execute('''CREATE TABLE Course(
                    date date,
                    id numeric(4) NOT NULL UNIQUE,
                    round numeric(2),
                    nom varchar(30),
                    temps char(9),
                    idCircuit numeric(4) REFERENCES Circuit,
                    PRIMARY KEY (id,date)
                    );''')
    
    for row in df6.itertuples():
        curs.execute('''INSERT INTO Course VALUES (%s,%s,%s,%s,%s,%s);''',                                                                              
                        (row.date,row.raceId,row.round,row.name,row.time,row.circuitId))

    co.commit()


    curs.execute('''CREATE TABLE Status(
                    id numeric(3) PRIMARY KEY,
                    designation varchar(30)
                    );''')

    for row in df8.itertuples():
        curs.execute('''INSERT INTO Status VALUES (%s,%s);''',                                                                              
                        (row.statusId,row.status))

    co.commit()


    curs.execute('''CREATE TABLE PitStop(
                    stopNumber numeric(2),
                    LapNumber numeric(2),
                    time varchar(8),
                    duration varchar(10),
                    pilote numeric(3),
                    courseId numeric(4),
                    PRIMARY KEY (stopNumber,pilote,courseId),
                    FOREIGN KEY (pilote) REFERENCES Pilotes(reference),
                    FOREIGN KEY (courseId) REFERENCES Course(id)
                    );''')
    
    for row in df4.itertuples():
        curs.execute('''INSERT INTO PitStop VALUES (%s,%s,%s,%s,%s,%s);''',                                                                              
                        (row.stop,row.lap,row.time,row.duration,row.driverId,row.raceId))

    co.commit()

    print(df7)

    curs.execute('''CREATE TABLE Resultats(
                    pilote numeric(3),
                    courseId numeric(4),
                    numeroVoiture numeric(3),
                    positionGrille numeric(2),
                    positionFinale numeric(2),
                    points numeric(2),
                    nbTours numeric(3),
                    tempsMilli numeric(8),
                    meilleurTour numeric(3),
                    rangMeilleurTour numeric(2),
                    tempsMeilleurTour varchar(8),
                    vitesseMaxMeilleurTour numeric(7),
                    constructeur numeric(3),
                    status numeric(3),
                    FOREIGN KEY (pilote) REFERENCES Pilotes(reference),
                    FOREIGN KEY (courseId) REFERENCES Course (id),
                    FOREIGN KEY (constructeur) REFERENCES Constructeur(constructeurId),
                    FOREIGN KEY (status) REFERENCES Status,
                    PRIMARY KEY(pilote,courseId,numeroVoiture)
                    );''')

    for row in df7.itertuples():
        curs.execute('''INSERT INTO Resultats VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''',                                                                              
                        (row.driverId,row.raceId,row.number,row.grid,row.position,row.points,row.laps,row.milliseconds,row.fastestLap,row.rank,row.fastestLapTime,row.fastestLapSpeed,row.constructorId,row.statusId))

    co.commit()

except(Exception, psy.DatabaseError) as error:
    print(error)

finally:
    if co is not None:
        co.close()
