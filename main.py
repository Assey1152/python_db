from pprint import pprint
import psycopg2
from psycopg2.sql import SQL, Identifier


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
            id SERIAL PRIMARY KEY, 
            first_name VARCHAR(60) NOT NULL,
            last_name VARCHAR(60) NOT NULL,
            email VARCHAR(80) NOT NULL,
            CONSTRAINT valid_email CHECK (email ~* '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$')
            );
        """)
        cur.execute("""
              CREATE TABLE IF NOT EXISTS phone(
              id SERIAL PRIMARY KEY,
              phone_num text UNIQUE, 
              user_id INTEGER NOT NULL REFERENCES client(id),
              CONSTRAINT valid_phone CHECK (phone_num ~ '^[0-9]{10}$')
              );
        """)
        conn.commit()


def clear_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS phone;
        """)
        cur.execute("""
            DROP TABLE IF EXISTS client;
        """)
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s) RETURNING id;
        """, (first_name, last_name, email))
        new_id = cur.fetchone()[0]
        if phones:
            for phone in phones:
                add_phone_by_id(conn, new_id, phone)
        

def add_phone_by_id(conn, client_id, new_phone):
    with conn.cursor() as cur:
        cur.execute("""
                INSERT INTO phone(phone_num, user_id) VALUES(%s, %s) RETURNING id;
        """, (new_phone, client_id))
        return cur.fetchone()[0]


def del_phone_by_id(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone WHERE id=%s AND phone_num = %s;
        """, (client_id, phone))
        conn.commit()


def del_client_by_id(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone WHERE user_id=%s;
        """, (client_id,))
        cur.execute("""
            DELETE FROM client WHERE id=%s;
        """, (client_id,))
        conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    arg_list = {'first_name': first_name, "last_name": last_name, 'email': email, 'phone_num': phone}
    with conn.cursor() as cur:
        for key, arg in arg_list.items():
            if arg:
                cur.execute(
                    SQL("""SELECT cl.id, cl.first_name, cl.last_name, cl.email, p.phone_num FROM client cl
                        LEFT JOIN phone p ON cl.id = p.user_id
                        WHERE {} = %s;
                    """).format(Identifier(key)), (arg,)
                )
        return cur.fetchall()


def update_client(conn, client_id, first_name=None, last_name=None, email=None, old_phone=None, new_phone=None):
    arg_list = {'first_name': first_name, "last_name": last_name, 'email': email}
    with conn.cursor() as cur:
        for key, arg in arg_list.items():
            if arg:
                cur.execute(
                    SQL("UPDATE client SET {} = %s WHERE id = %s;").format(Identifier(key)), (arg, client_id)
                )
        conn.commit()
        if old_phone and new_phone:
            cur.execute("""
                UPDATE phone SET phone_num = %s WHERE user_id = %s AND phone_num = %s;
            """, (new_phone, client_id, old_phone))
        conn.commit()


def show_full_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cl.id, cl.first_name, cl.last_name, cl.email, p.phone_num FROM client cl
            LEFT JOIN phone p ON cl.id = p.user_id
            ORDER BY cl.id ASC;    
        """)
        return cur.fetchall()


with psycopg2.connect(database='python_db', user='postgres', password='5734169') as conn1:
    clear_db(conn1)
    create_db(conn1)
    add_client(conn1, 'cl1', 'cl_last1', 'abc@d.ef', ['1234567895', '9876543214'])
    add_client(conn1, 'cl2', 'cl_last2', 'abcd@g.fa')
    add_phone_by_id(conn1, 2, '1111111111')

    print("find client with last_name = 'cl_last2': ", find_client(conn1, last_name='cl_last2'))
    del_phone_by_id(conn1, 1, '1234567895')
    pprint(show_full_table(conn1))
    del_client_by_id(conn1, 1)
    pprint(show_full_table(conn1))
    update_client(conn1, 2, 'new name', old_phone='1111111111', new_phone='2222222222')
    pprint(show_full_table(conn1))


