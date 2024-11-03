from pprint import pprint

import psycopg2
from psycopg2.sql import SQL, Identifier


class Client:
    def __init__(self, first_name, last_name, email, phones=None):
        self. first_name = first_name
        self.last_name = last_name
        self.email = email
        if phones is None:
            phones = []
        self.phones = phones


class DbManager:
    def __init__(self):
        self.conn = psycopg2.connect(database='python_db', user='postgres', password='5734169')

    def __del__(self):
        self.close()

    def close(self):
        self.conn.close()

    def clear_db(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS phone;
                """)
            cur.execute("""
                DROP TABLE IF EXISTS client;
                """)
            self.conn.commit()

    def create_db(self):
        with self.conn.cursor() as cur:
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
        self.conn.commit()

    def add_user(self, new_client: Client):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s) RETURNING id;
                """, (new_client.first_name, new_client.last_name, new_client.email))
            new_id = cur.fetchone()[0]
            if new_client.phones:
                for phone in new_client.phones:
                    self.add_phone_by_id(new_id, phone)

    def add_phone_by_id(self, client_id, new_phone):
        with self.conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO phone(phone_num, user_id) VALUES(%s, %s) RETURNING id;
            """, (new_phone, client_id))
            return cur.fetchone()[0]

    def get_by_id(self, client_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM client            
                WHERE id = %s;
            """, (client_id,))
        return cur.fetchone()

    def update_client(self, client_id, first_name=None, last_name=None, email=None, old_phone=None, new_phone=None):
        arg_list = {'first_name': first_name, "last_name": last_name, 'email': email}
        with self.conn.cursor() as cur:
            for key, arg in arg_list.items():
                if arg:
                    cur.execute(
                        SQL("UPDATE client SET {} = %s WHERE id = %s;").format(Identifier(key)), (arg, client_id)
                    )
            self.conn.commit()
            if old_phone and new_phone:
                cur.execute("""
                    UPDATE phone SET phone_num = %s WHERE user_id = %s AND phone_num = %s;
                """, (new_phone, client_id, old_phone))
            self.conn.commit()

    def find_client(self, first_name=None, last_name=None, email=None, phone=None):
        arg_list = {'first_name': first_name, "last_name": last_name, 'email': email, 'phone_num': phone}
        with self.conn.cursor() as cur:
            for key, arg in arg_list.items():
                if arg:
                    cur.execute(
                        SQL("""SELECT cl.id, cl.first_name, cl.last_name, cl.email, p.phone_num FROM client cl
                            LEFT JOIN phone p ON cl.id = p.user_id
                            WHERE {} = %s;
                        """).format(Identifier(key)), (arg,)
                    )
            return cur.fetchall()

    def del_phone_by_id(self, client_id, phone):
        with self.conn.cursor() as cur:
            cur.execute("""
                DELETE FROM phone WHERE id=%s AND phone_num = %s;
            """, (client_id, phone))
            self.conn.commit()

    def del_client_by_id(self, client_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                DELETE FROM phone WHERE user_id=%s;
            """, (client_id,))
            cur.execute("""
                DELETE FROM client WHERE id=%s;
            """, (client_id,))
            self.conn.commit()

    def show_full_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT cl.id, cl.first_name, cl.last_name, cl.email, p.phone_num FROM client cl
                LEFT JOIN phone p ON cl.id = p.user_id
                ORDER BY cl.id ASC;    
            """)
            return cur.fetchall()


db = DbManager()
db.clear_db()
db.create_db()
cl1 = Client('Client1', 'Surname1', 'email_1@best.db', ['1234567895', '3344556677'])
cl2 = Client('Client2', 'Surname2', 'email_2@best.db')
db.add_user(cl1)
db.add_user(cl2)
db.add_phone_by_id(1, '5555555555')
db.add_phone_by_id(2, '2222222222')
print('Full table: ', end='')
pprint(db.show_full_table())
print('Find client: ', db.find_client(last_name='Surname2'))
db.del_phone_by_id(1, '1234567895')
pprint(db.show_full_table())
db.del_client_by_id(1)
print('Full table after deleting client 1: ', end='')
pprint(db.show_full_table())
db.update_client(2, last_name='new_surname', old_phone='2222222222', new_phone='1478523695')
print('Table after updating client 2: ', end='')
pprint(db.show_full_table())
db.close()
