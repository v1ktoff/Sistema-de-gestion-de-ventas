from flask import Flask, jsonify, request
import mysql.connector
from mysql.connector import Error
from flask_cors import CORS
import os
import sys

# Agregar la carpeta raíz al path para importar config.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, SERVER_CONFIG

app = Flask(__name__)
CORS(app)  # Permitir solicitudes desde el frontend

# Función para crear la conexión a MariaDB
def create_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error al conectar a MariaDB: {e}")
        return None

# Ruta para obtener los datos con búsqueda opcional
@app.route('/datos', methods=['GET'])
def get_data():
    offset = int(request.args.get('offset', 0))
    limit = 50
    search = request.args.get('search', '').strip()

    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            if search:
                query = """
                    SELECT * FROM productos
                    WHERE CÓDIGO LIKE %s
                    ORDER BY (CÓDIGO = %s) DESC, CÓDIGO
                    LIMIT %s OFFSET %s
                """
                cursor.execute(query, (f"%{search}%", search, limit, offset))
            else:
                query = "SELECT * FROM productos LIMIT %s OFFSET %s"
                cursor.execute(query, (limit, offset))

            results = cursor.fetchall()
            return jsonify(results)
        except Error as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    else:
        return jsonify({"error": "No se pudo conectar a la base de datos"}), 500

# Iniciar el servidor
if __name__ == '__main__':
    app.run(port=SERVER_CONFIG['port'], debug=SERVER_CONFIG['debug'])
