import psycopg2


def create_db(conn):
    cur = conn.cursor()
    cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(20) NOT NULL,
                last_name VARCHAR(20) NOT NULL,
                email TEXT UNIQUE
            );
            """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS client_phone(
                id SERIAL PRIMARY KEY,
                phones TEXT,
                client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE
            );
            """)
    conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s) RETURNING id;
            """, (first_name, last_name, email))
        client_id = cur.fetchone()

        cur.execute("""
        INSERT INTO client_phone (client_id, phones) VALUES (%s, %s);
                """, (client_id, phones))
        conn.commit()
        print(f"Клиент создан c ID {client_id}")
        return client_id

    except:
        print("Клиент c таким email уже существует")
        return None


def add_phone(conn, client_id, phones):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id FROM client WHERE id = %s ;
            """, (client_id, ))
        if not cur.fetchone():
            print("Ошибка: клиент с указанным ID не существует")
            return False
        cur.execute("""INSERT INTO client_phone (client_id, phones) VALUES (%s, %s)
            """, (client_id, phones))

        conn.commit()
        print("Телефон успешно добавлен")
        return True

    except:
        print(f"Ошибка: клиент с указанным ID  не существует")
        return False


def change_client(conn, id, last_name, first_name=None, email=None, phones=None):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id FROM client WHERE id = %s ;
            """, (id, ))
        if not cur.fetchone():
            print("Ошибка: клиент с указанным ID не существует")
            return False
        cur.execute("""
                    UPDATE client SET last_name=%s WHERE id=%s;
                    """, (last_name, id))
        conn.commit()
        print("Фамилия успешно изменена")
        return True

    except:
        print(f"Ошибка: клиент с указанным ID  не существует")
        return False


def delete_phone(conn, id, phone):
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM client_phone WHERE client_id = %s AND phones = '%s';
            """, (id,phone ))
        if cur.rowcount == 0:
            print("Ошибка: телефон не найден")
            return False
        conn.commit()
        print("Телефон успешно удален")
        return True

    except psycopg2.Error as e:
        print(f"Ошибка: {e}")
        return False


def delete_client(conn, id):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id FROM client WHERE id = %s ;
            """, (id, ))
        if not cur.fetchone():
            print("Ошибка: клиент с указанным ID не существует")
            return False
        cur.execute("""
                    DELETE FROM client WHERE id=%s;
                    """, (id,))
        cur.execute("""
                    SELECT * FROM client;
                    """)
        print("Kлиент с указанным ID успешно удален")
        return True

    except:
        print(f"Ошибка: клиент с указанным ID не существует")
        return False


def find_client(conn, first_name, last_name=None, email=None, phone=None):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id FROM client WHERE first_name = %s ;
            """, (first_name, ))
        if not cur.fetchone():
            print("Ошибка: клиент с указанным именем не существует")
            return False
        cur.execute("""
                    SELECT id, first_name, last_name, email FROM client WHERE first_name= %s;
                    """, (first_name,))
        print(cur.fetchone())
        return True

    except:
        print(f"Ошибка: клиент с указанным именем не существует")
        return False


with psycopg2.connect(database="clients_db", user="postgres", password="PostgreS") as conn:
    create_tabl = create_db(conn)

    # add_client1 = add_client(conn, "Иван","Помоев","ivan@mail.ru")
    # add_client2 = add_client(conn, "Сергей","Чащин","sergey@mail.ru","891231231")
    # add_client3 = add_client(conn, "Константин","Золотов","kosty@mail.ru","891732333")
    # add_client4 = add_client(conn, "Аркадий","Эдуардов","arkasha@mail.ru","891923231")
    # add_client5 = add_client(conn, "Алексей","Молчанов","lexa@mail.ru","891923231")

    # add_phones = add_phone(conn,5, 8909123123)

    # change_client1 = change_client(conn, 5, 'Аляскин')

    # delete_phones = delete_phone(conn,5, 8909123123)

    # delete_clients = delete_client(conn,5)

    # find_clients = find_client(conn,'Иван')

conn.close()