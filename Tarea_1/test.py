import redis as rd
import time

def sum_queries():
    test = rd.Redis(host='localhost', port=6379)

    pipe = test.pipeline()

    for i in range(10):
        pipe.set(f'user:session:{i}', f'token_abc_{i}')

    pipe.execute()

    print("Se hicieron las 10 cargas de datos") 

sum_queries()       