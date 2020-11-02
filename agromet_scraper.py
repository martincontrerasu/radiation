#!/usr/bin/env python
# coding: utf-8

# """This python script scrapes https://agrometeorologia.cl/ data for oceanographic and meteorologic proposes.
# The focus is given to X and XI Regions, particularly:
#
#     Corral: Corral - ESSAL
#     Los Muermos: Los Canelos
#     Puerto Montt: Escuela Mirasol
#     Ancud: Liceo Agrícola de Ancud
#     Curaco de Vélez: Huyar Alto
#     Chonchi: Tara
#     Chaitén: Nueva Chaitén
#     Quellón: Aeródromo de Quellón
#     Guaitecas: Aeródromo Melinka
#     Cisnes: La Junta
#     Aisén: Aeródromo de Puerto Aysén
#     Tortel: Tortel
#     Natales: Puerto Natales
#     Porvenir: Aeródromo Fuentes Martínez
#
# The varibles to collect are:
#
#     Wind speed (avg)in km/h
#     Wind Direction (avg) in degrees(N = 0 degrees)
#     Precipitations (cumulative)in mm
#     Air's Temperature (max) in Celsius
#     Radiation (avg) in mj/m2
#     """

# In[1]:


#web
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta, date

#postgress
import psycopg2
# sql server
import pyodbc


# In[2]:


#database credentials
server = "drona.db.elephantsql.com"
usrdb = "dhwahxdx"
password = "GA7LbhEV58lC5fFiilUdBSUTAMoisF4w"


# In[3]:


def create_db():
    try:
        conn = psycopg2.connect(dbname=usrdb, user=usrdb, password=password, host=server)
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE agromet(fecha TIMESTAMP,
                                               estacion VARCHAR,
                                               comuna VARCHAR,
                                               PP_SUM REAL,
                                               TA_MAX REAL,
                                               TA_MIN REAL,
                                               VV_AVG REAL,
                                               DV_AVG REAL,
                                               RD_AVG REAL)""")
        conn.commit()
        conn.close()
    except Exception as error:
        print('Ups, ocurrió un error al crear la tabla. Contáctece con el encargado!')
        print(error)


# In[4]:

def get_data():
    url = "https://agrometeorologia.cl/assets/db/items-resumen.json?"
    r = requests.get(url)

    soup = BeautifulSoup(r.text,'html.parser')
    data = json.loads(soup.text)
    return data


# In[5]:


#a class of the station data
class Station:
    def __init__(self,datum):
        self.fecha = datetime.date(datetime.now())
        self.fecha_ayer = datetime.date(datetime.now() - timedelta(days=1))
        self.station = datum['nombre']
        self.comuna = datum['comuna']
        self.lat = datum['latitud']
        self.lon = datum['longitud']
        self.elev = datum['elevacion']
        self.hoy = datum['STACK-DAY']['hoy']
        self.ayer = datum['STACK-DAY']['ayer']


    def upload(self):
        #first make the connection
        conn = psycopg2.connect(dbname=usrdb, user=usrdb, password=password, host=server)
        c = conn.cursor()
        #select everething from today
        c.execute("SELECT * FROM agromet WHERE agromet.fecha = (%s) AND comuna = %s",(self.fecha,self.comuna))
        #if there's nothin today
        if len(c.fetchall()) == 0:
            #tries to insert the values
            try:
                c.execute("""INSERT INTO agromet(fecha,
                                                 estacion,
                                                 comuna,
                                                 pp_sum,
                                                 ta_max,
                                                 ta_min,
                                                 vv_avg,
                                                 dv_avg,
                                                 rd_avg) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (self.fecha,
                                                                                               self.station,
                                                                                               self.comuna,
                                                                                               self.hoy['PP-SUM'],
                                                                                               self.hoy['TA-MAX'],
                                                                                               self.hoy['TA-MIN'],
                                                                                               self.hoy['VV-AVG'],
                                                                                               self.hoy['DV-AVG'],
                                                                                               self.hoy['RD-AVG']))
            except Exception as error:
                print("Ha ocurrido un problema al INSERTAR los datos a la base de datos! Contacta al encargado.")
                print(error)
        #if there's already data from today...
        else:
            #tries to update it
            try:
                c.execute("""UPDATE agromet SET  estacion = %s,
                                                 comuna = %s,
                                                 pp_sum = %s,
                                                 ta_max = %s,
                                                 ta_min = %s,
                                                 vv_avg = %s,
                                                 dv_avg = %s,
                                                 rd_avg = %s WHERE fecha = %s AND comuna = %s""", (self.station,
                                                                                       self.comuna,
                                                                                       self.hoy['PP-SUM'],
                                                                                       self.hoy['TA-MAX'],
                                                                                       self.hoy['TA-MIN'],
                                                                                       self.hoy['VV-AVG'],
                                                                                       self.hoy['DV-AVG'],
                                                                                       self.hoy['RD-AVG'],
                                                                                       self.fecha,
                                                                                       self.comuna))
            except Exception as error:
                print("Ha ocurrido un problema al ACTUALIZAR los datos de la base de datos! Contacta al encargado.")
                print(error)

        #close the connection
        conn.commit()
        conn.close()
        
    def upload_local(self):
        user = 'admin_app'
        password = '$aK58%&pa'
        host = '200.126.102.51'
        db = 'app'
        driver = '{SQL Server}'

        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+host+';DATABASE='+db+';UID='+user+';PWD='+ password)
        c = conn.cursor()


        try:
            c.execute("""INSERT INTO agrometeorologia(
                                             fecha,
                                             estacion,
                                             comuna,
                                             pp_sum,
                                             ta_max,
                                             ta_min,
                                             vv_avg,
                                             dv_avg,
                                             rd_avg) VALUES (?,?,?,?,?,?,?,?,?)""", datetime.now(),
                                                                                   str(self.station),
                                                                                   str(self.comuna),
                                                                                   self.hoy['PP-SUM'],
                                                                                   self.hoy['TA-MAX'],
                                                                                   self.hoy['TA-MIN'],
                                                                                   self.hoy['VV-AVG'],
                                                                                   self.hoy['DV-AVG'],
                                                                                   self.hoy['RD-AVG'])
        except Exception as error:
            print("Ha ocurrido un problema al INSERTAR los datos a la base de datos! Contacta al encargado.")
            print(error)
        
        #close the connection
        conn.commit()
        conn.close()


stations = [['Corral', 'Corral - ESSAL'],
            ['Los Muermos', 'Los Canelos'],
            ['Puerto Montt', 'Escuela Mirasol'],
            ['Ancud', 'Liceo Agrícola de Ancud'],
            ['Curaco de Vélez', 'Huyar Alto'],
            ['Chonchi', 'Tara'],
            ['Chaitén', 'Nueva Chaitén'],
            ['Quellón', 'Aeródromo de Quellón'],
            ['Guaitecas', 'Aeródromo Melinka'],
            ['Cisnes', 'La Junta'],
            ['Aisén', 'Aeródromo de Puerto Aysén'],
            ['Tortel', 'Tortel'],
            ['Natales', 'Puerto Natales'],
            ['Porvenir', 'Aeródromo Fuentes Martínez']]


# In[7]:

def upload():
    data = get_data()
    for station in stations:
        for datum in data:
            if 'nombre' in datum.keys() and datum['nombre'] == station[-1] and datum['comuna']==station[0]:
                try:
                    var = Station(datum)
                    var.upload()
                    var.upload_local()
                except Exception as error:
                    print (str(error) + ' en '+ station[1])
                    print()


# In[ ]:
