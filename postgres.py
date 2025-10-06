import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "solar_db2",
    "user": "postgres",
    "password": "clase202122",
}


def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS projects2 (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            location VARCHAR(100),
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tabla 'projects' creada o ya existente.")


def insert_projects():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    data = [
        ("Parque Solar El Desierto", "La Guajira", "Proyecto solar fotovoltaico de 50 MW"),
        ("Planta FV Campus", "Bogotá", "Sistema de 1.2 MW para demostración académica"),
        ("Microred Caribe", "Cartagena", "Instalación experimental híbrida solar-diésel"),
    ]

    cur.executemany(
        """
        INSERT INTO projects (name, location, description)
        VALUES (%s, %s, %s);
    """,
        data,
    )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Datos insertados correctamente.")


def get_projects():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT id, name, location, description, created_at FROM projects;")
    rows = cur.fetchall()

    for r in rows:
        print(r)

    cur.close()
    conn.close()


create_table()
insert_projects()
get_projects()
