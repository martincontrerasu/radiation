import requests
import json
import pandas as pd
import ast
import numpy as np
import pandas as pd
import psycopg2
import time

def  main():
    #database info
    server='rajje.db.elephantsql.com'
    usrdb='wjufwnaw'
    password='PchOU_rrCD3lHZ3sm19RAIppZ6e0u_49'

    estaciones=[
    {"Castro":"CSTR"},
    {"Puerto Montt":"PMON"},
    {"Ancud":"ANCU"},
    {"Melinka":"PMEL"},
    {"Bahia Mansa":"BMSA"},
    {"Corral":"CORR"},
    {"Chacabuco":"PCHA"},
    {"Puerto Eden":"PEDN"},
    {"Queule":"QUEL"},
    {"Caleta Meteoro":"CMET"},
    {"Punta Arenas":"PTAR"},
    {"Bahia Gregorio":"GREG"},
    #{"Puerto Williams":"PWIL"}
    ]
    coordinates={
        "Castro":(-73.701,-42.469),
        "Puerto Montt":(-72.975,-41.498),
        "Queule":(-73.228,-39.435),
        "Ancud":(-73.894,-41.913),
        "Bahia Mansa":(-73.735,-40.584),
        "Corral":(-73.436,-39.893),
        "Melinka":(-73.712,-43.897),
        "Chacabuco":(-72.805,-45.455),
        "Puerto Eden":(-74.426,-49.137),
        "Caleta Meteoro":(-74.300,-52.854),
        "Punta Arenas":(-70.882,-53.141),
        "Bahia Gregorio":(-70.186,-52.608),
        "Puerto Williams":(-67.623,-54.920)


    }
    sensores=[{"Temperatura del agua":"tw"}, {"Presion":"bp"}, {"Humedad relativa":"rh"}]

    def shoa_querier(estacion, sensor, periodo=12):
        if type(periodo)== int:
            periodo = str(periodo)
            url = "http://wsprovimar.mitelemetria.cl/apps/src/ws/wsgw.php?wsname=getData&idsensor="+sensor+"&idestacion="+estacion+"&period="+periodo+"&fmt=json&tipo=tecmar&orden=ASC&callback=jQuery331023048477019762714_1601904567895&_=1601904567896"
            text = requests.get(url).text
            try:
                text = text.split("(")[1][:-2]
                text = text.replace('null','None')
                data = ast.literal_eval(text)
            except:
                print(text)

            return data
        else:
            return "Algunos de los parámetros ingresados son incorrectos, revisa dir(shoa_querier) para ver los detalles"

    def data_parser(data):
        var = []
        fecha = []

        for value in data:
            try:
                if value['DATO'] != None:
                    var.append(float(value['DATO'])/1000)
                    fecha.append(value['FECHA'])
            except:
                pass
        return np.median(var),fecha[-2]

    #first make the connection
    conn = psycopg2.connect(dbname=usrdb, user=usrdb, password=password, host=server)
    c = conn.cursor()

    for estacion in estaciones:
        try:
            sensor1 = 'tw'#water temperature
            sensor2 = 'ta'#air temperature
            sensor3 = 'bp'#barometric pressure
            est_name =  list(estacion.keys())[0]
            est_code = list(estacion.values())[0]
            sst,fecha = data_parser(shoa_querier(est_code, sensor1))
            ta = data_parser(shoa_querier(est_code, sensor2))[0]
            bp = data_parser(shoa_querier(est_code, sensor3))[0]
            if type(sst)==float:
                c.execute("""INSERT INTO SHOA (estacion,fecha,SST,ta,bp) VALUES (%s,%s,%s,%s,%s) """, (est_name,fecha,sst,ta,bp))
                conn.commit()
                #close the connection

        except Exception as error:
            print("Ha ocurrido un problema al obtener los datos de "+ est_name)
            print(error)


    conn.close()
