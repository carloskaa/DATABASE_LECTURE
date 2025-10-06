"""
create_solar_db.py
Script para crear una base de datos SQLite robusta para proyectos solares,
con tablas: projects, systems, meteo_data, electrical_data.
Incluye funciones para:
 - crear la estructura (create_db)
 - insertar proyectos y sistemas
 - cargar un CSV de datos eléctricos por sistema (ingest_electrical_csv)

Requisitos:
 - Python 3.8+
 - pandas (solo para la función de ingestión CSV)
"""

import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, Iterable
import os

# Si vas a usar la función de ingestión de CSV:
try:
    import pandas as pd
except Exception:
    pd = None  # manejamos la ausencia de pandas en tiempo de import


# -------------------------
# Funciones de utilidad DB
# -------------------------
def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA foreign_keys = ON;")
    # Para rendimiento cuando insertas muchos rows:
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


# -------------------------
# Creación de la estructura
# -------------------------
def create_db(db_path: str):
    """Crea la base de datos y todas las tablas si no existen."""
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        location TEXT,
        description TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS systems (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        capacity_kw REAL,
        inverter_type TEXT,
        notes TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE ON UPDATE CASCADE
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS meteo_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        system_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        ghi REAL,    -- Global Horizontal Irradiance (W/m2)
        dni REAL,    -- Direct Normal Irradiance (W/m2)
        dhi REAL,    -- Diffuse Horizontal Irradiance (W/m2)
        temp_c REAL,
        wind_m_s REAL,
        precip_mm REAL,
        source TEXT,
        inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (system_id) REFERENCES systems(id) ON DELETE CASCADE
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS electrical_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        system_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        power_kw REAL,      -- potencia instantánea en kW
        voltage_v REAL,
        current_a REAL,
        energy_kwh REAL,    -- energía medida (por intervalo) en kWh si aplica
        status TEXT,
        inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (system_id) REFERENCES systems(id) ON DELETE CASCADE
    );
    """
    )

    # Índices importantes para consultas por sistema y por timestamp
    cur.execute("CREATE INDEX IF NOT EXISTS idx_systems_project ON systems(project_id);")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_meteo_system_time ON meteo_data(system_id, timestamp);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_elec_system_time ON electrical_data(system_id, timestamp);"
    )

    conn.commit()
    conn.close()
    print(f"Base de datos y tablas creadas en: {db_path}")


# -------------------------
# Operaciones CRUD simples
# -------------------------
def add_project(
    db_path: str, name: str, location: Optional[str] = None, description: Optional[str] = None
) -> int:
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO projects (name, location, description) VALUES (?, ?, ?);",
        (name, location, description),
    )
    conn.commit()
    project_id = cur.lastrowid
    conn.close()
    return project_id


def add_system(
    db_path: str,
    project_id: int,
    name: str,
    capacity_kw: Optional[float] = None,
    inverter_type: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO systems (project_id, name, capacity_kw, inverter_type, notes) VALUES (?, ?, ?, ?, ?);",
        (project_id, name, capacity_kw, inverter_type, notes),
    )
    conn.commit()
    system_id = cur.lastrowid
    conn.close()
    return system_id


# -------------------------
# Ingestión CSV de datos eléctricos
# -------------------------
def ingest_electrical_csv(
    db_path: str,
    system_id: int,
    csv_path: str,
    timestamp_col: str = "timestamp",
    col_mappings: Optional[Dict[str, str]] = None,
    chunk_size: int = 1000,
):
    """
    Lee un CSV con pandas y lo inserta en la tabla electrical_data.
    - db_path: ruta a la base de datos
    - system_id: id del sistema en la tabla systems
    - csv_path: ruta al CSV
    - timestamp_col: nombre de la columna de tiempo en el CSV
    - col_mappings: dict que mapea nombres del CSV a las columnas DB, p.ej.
        {'potencia': 'power_kw', 'voltaje': 'voltage_v'}
      Si no se pasa, se intentan columnas estándar: ['timestamp','power_kw','voltage_kW','voltage_v','current_a','energy_kwh','status']
    - chunk_size: inserciones por transacción
    """
    if pd is None:
        raise ImportError(
            "Para usar ingest_electrical_csv necesitas instalar pandas: pip install pandas"
        )

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No existe el archivo CSV: {csv_path}")

    # Mapeo default
    default_map = {
        "timestamp": "timestamp",
        "time": "timestamp",
        "datetime": "timestamp",
        "power_kw": "power_kw",
        "power_kw_avg": "power_kw",
        "power": "power_kw",
        "power_kW": "power_kw",
        "voltage_v": "voltage_v",
        "voltage": "voltage_v",
        "current_a": "current_a",
        "current": "current_a",
        "energy_kwh": "energy_kwh",
        "energy": "energy_kwh",
        "status": "status",
    }
    if col_mappings:
        # valores en col_mappings sobreescriben default_map
        combined_map = {**default_map, **{k: v for k, v in col_mappings.items()}}
    else:
        combined_map = default_map

    # Leer CSV (dejamos que pandas infiera tipos; parseamos fecha después)
    df_iter = pd.read_csv(csv_path, chunksize=chunk_size, iterator=True)

    conn = get_connection(db_path)
    cur = conn.cursor()

    total_inserted = 0
    for chunk_df in df_iter:
        # Normalizar columnas a minúsculas para facilitar matching
        chunk_df.columns = [c.strip() for c in chunk_df.columns]
        # Mapeo de columnas existentes
        col_map_existing = {}
        for c in chunk_df.columns:
            lower = c.lower()
            if lower in combined_map:
                col_map_existing[c] = combined_map[lower]
            # allow exact match to target column names (if user already used standard names)
            elif lower in {
                "timestamp",
                "power_kw",
                "voltage_v",
                "current_a",
                "energy_kwh",
                "status",
            }:
                col_map_existing[c] = lower

        # Debemos tener una columna de timestamp mapeada
        if not any(v == "timestamp" for v in col_map_existing.values()):
            raise ValueError(
                f"El CSV en {csv_path} no tiene una columna de timestamp reconocida. Columnas detectadas: {list(chunk_df.columns)}"
            )

        # Crear DataFrame con columnas objetivo (posibles ausentes)
        df_to_insert = pd.DataFrame()
        for src_col, target_col in col_map_existing.items():
            df_to_insert[target_col] = chunk_df[src_col]

        # Parseo y normalización del timestamp a ISO 8601 en UTC-naive (o conserva lo que venga)
        df_to_insert["timestamp"] = pd.to_datetime(
            df_to_insert["timestamp"], infer_datetime_format=True, errors="coerce"
        )
        # Drop filas con timestamps inválidos
        n_before = len(df_to_insert)
        df_to_insert = df_to_insert.dropna(subset=["timestamp"])
        n_after = len(df_to_insert)
        if n_after < n_before:
            print(
                f"  Se descartaron {n_before - n_after} filas con timestamp inválido en chunk actual."
            )

        # Convertimos a string ISO para almacenar en SQLite
        df_to_insert["timestamp"] = df_to_insert["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Aseguramos columnas presentes en la DB
        for col in ["power_kw", "voltage_v", "current_a", "energy_kwh", "status"]:
            if col not in df_to_insert.columns:
                df_to_insert[col] = None

        # Inserción por lotes
        rows = [
            (
                system_id,
                row["timestamp"],
                _safe_float(row["power_kw"]),
                _safe_float(row["voltage_v"]),
                _safe_float(row["current_a"]),
                _safe_float(row["energy_kwh"]),
                _safe_str(row["status"]),
            )
            for _, row in df_to_insert.iterrows()
        ]

        # Ejecutar dentro de una transacción
        try:
            cur.executemany(
                "INSERT INTO electrical_data (system_id, timestamp, power_kw, voltage_v, current_a, energy_kwh, status) VALUES (?, ?, ?, ?, ?, ?, ?);",
                rows,
            )
            conn.commit()
            total_inserted += len(rows)
            print(f"  Insertadas {len(rows)} filas (acumulado: {total_inserted})")
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Error insertando chunk en DB: {e}")

    conn.close()
    print(f"Ingestión terminada. Total filas insertadas: {total_inserted}")


# -------------------------
# Helpers
# -------------------------
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, float):
            return x
        s = str(x).strip()
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        # Reemplaza comas por puntos si vienen como "1,23"
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None


# -------------------------
# Ejemplo de uso
# -------------------------
if __name__ == "__main__":
    DB = "solar_projects.db"
    create_db(DB)

    # Crear un proyecto
    proyecto_id = add_project(
        DB,
        name="Parque Solar Ejemplo",
        location="La Guajira, CO",
        description="Proyecto piloto 5 MW",
    )
    print("Proyecto creado:", proyecto_id)

    # Agregar dos sistemas al proyecto
    sys1 = add_system(
        DB,
        proyecto_id,
        "Array Norte",
        capacity_kw=3000.0,
        inverter_type="String",
        notes="Orientación N",
    )
    sys2 = add_system(
        DB,
        proyecto_id,
        "Array Sur",
        capacity_kw=2000.0,
        inverter_type="Central",
        notes="Orientación S",
    )
    print("Sistemas creados:", sys1, sys2)

    # Supongamos que tienes un CSV con columnas: timestamp, power_kW, voltage, current, energy
    # Para probar la ingestión, descomenta la línea siguiente y ajusta la ruta al CSV:
    # ingest_electrical_csv(DB, sys1, "datos_electricos.csv")

    print("Setup inicial completado. Ahora puedes usar ingest_electrical_csv para cargar datos.")

    # from create_solar_db import ingest_electrical_csv

    # ingest_electrical_csv("solar_projects.db", system_id=1, csv_path="datos_electricos.csv")
