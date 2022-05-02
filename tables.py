import pandas as pd 
import psycopg2 as psy
import matplotlib.pyplot as plt

data1 = pd.read_csv(r'circuits.csv')
df = pd.DataFrame(data1)
df2 = df.drop_duplicates()

data2 = pd.read_csv(r'constructor_results.csv')
df = pd.DataFrame(data2)
df2 = df.drop_duplicates()

data3 = pd.read_csv(r'constructor_standings.csv')
df = pd.DataFrame(data3)
df2 = df.drop_duplicates()

data4 = pd.read_csv(r'constructors.csv')
df = pd.DataFrame(data4)
df2 = df.drop_duplicates()

data5 = pd.read_csv(r'driver_standings.csv')
df = pd.DataFrame(data5)
df2 = df.drop_duplicates()

data6 = pd.read_csv(r'drivers.csv')
df = pd.DataFrame(data6)
df2 = df.drop_duplicates()

data7 = pd.read_csv(r'lap_times.csv')
df = pd.DataFrame(data7)
df2 = df.drop_duplicates()

data8 = pd.read_csv(r'pit_stops.csv')
df = pd.DataFrame(data8)
df2 = df.drop_duplicates()

data9 = pd.read_csv(r'qualifying.csv')
df = pd.DataFrame(data9)
df2 = df.drop_duplicates()

data10 = pd.read_csv(r'races.csv')
df = pd.DataFrame(data10)
df2 = df.drop_duplicates()

data11 = pd.read_csv(r'results.csv')
df = pd.DataFrame(data11)
df2 = df.drop_duplicates()

data12 = pd.read_csv(r'seasons.csv')
df = pd.DataFrame(data12)
df2 = df.drop_duplicates()

data13 = pd.read_csv(r'status.csv')
df = pd.DataFrame(data13)
df2 = df.drop_duplicates()

co=None
try:
    co = psy.connect(host='berlin',
                    database='db[username]',
                    user='[username]',
                    password='[mdp]')
    curs = co.cursor()

    
    curs.execute('''DROP TABLE IF EXISTS Course;''')
    curs.execute('''DROP TABLE IF EXISTS Circuit;''')

    curs.execute('''CREATE TABLE Circuit(
                    id  numeric(2) PRIMARY KEY,
                    nom varchar(50),
                    localisation varchar(50),
                    pays varchar(30),
                    latitude numeric(8,5),
                    longitude numeric(8,5),
                    altitude numeric(4)
                    );''')

    for row in df2.itertuples():
    curs.execute('''INSERT INTO Circuit VALUES (%s,%s,%s,%s,%s,%s,%s);''',                                                                              
                        (row.CircuitId,row.Name,row.Location,row.Country,row.Lat,row.Lng,row.Alt))

    co.commit()

    curs.execute('''CREATE TABLE Course(
                    date date PRIMARY KEY,
                    round numeric(2),
                    nom varchar(50),
                    heure numeric(4),
                    idCircuit numeric(2) REFERENCES Circuit(id)
                    );''')
    
    for row in df2.itertuples():
    curs.execute('''INSERT INTO Circuit VALUES (%s,%s,%s,%s,%s);''',                                                                              
                        (row.Date,row.Round,row.Name,row.Time,row.CircuitId))

    co.commit()

    curs.execute('''CREATE TABLE Constructeur(
                    nom varchar(30) PRIMARY KEY,
                    nationalité varchar(30)
                    );''')

    for row in df2.itertuples():
    curs.execute('''INSERT INTO Circuit VALUES (%s,%s);''',                                                                              
                        (row.Name,row.Nationality))

    co.commit()
    
    curs.execute('''CREATE TABLE PitStop(
                    id numeric() PRIMARY KEY,
                    nationalité varchar(30)
                    );''')
    

    co.commit()



except(Exception, psy.DatabaseError) as error:
    print(error)

finally:
    if co is not None:
        co.close()
