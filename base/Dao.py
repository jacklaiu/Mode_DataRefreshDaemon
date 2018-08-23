import pymysql.cursors

host='95.163.200.245'
user='root'
password='queue11235813'
db='security_data'
charset='utf8mb4'
cursorclass=pymysql.cursors.DictCursor

# host='localhost'
# user='root'
# password='123456'
# db='conceptlistener'
# charset='utf8mb4'
# cursorclass=pymysql.cursors.DictCursor

def getConn():
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 db=db,
                                 charset=charset,
                                 cursorclass=cursorclass)
    return connection

def updatemany(sql, arr_values):
    connection = getConn()
    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, arr_values)
        connection.commit()
    except Exception as e:
        connection.rollback()
    finally:
        connection.close()

def update(sql, values):
    connection = getConn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, values)
        connection.commit()
    except Exception as e:
        connection.rollback()
    finally:
        connection.close()

def select(sql, values):
    connection = getConn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, values)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()