import sqlite3

# 1. Conectar a la base de datos (si no existe, se crea automáticamente)
conexion = sqlite3.connect("mi_base_datos.db")

# 2. Crear un cursor para ejecutar sentencias SQL
cursor = conexion.cursor()

# 3. Crear una tabla (si no existe)
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    edad INTEGER,
    email TEXT UNIQUE
)
"""
)

# 4. Insertar algunos datos
cursor.execute(
    "INSERT INTO usuarios (nombre, edad, email) VALUES (?, ?, ?)",
    ("Carlos", 25, "ccarlos@example.com"),
)
cursor.execute(
    "INSERT INTO usuarios (nombre, edad, email) VALUES (?, ?, ?)", ("Ana", 30, "aana@example.com")
)

# 5. Guardar los cambios
conexion.commit()

# 6. Leer los datos
cursor.execute("SELECT * FROM usuarios")
for fila in cursor.fetchall():
    print(fila)

# 7. Cerrar la conexión
conexion.close()
