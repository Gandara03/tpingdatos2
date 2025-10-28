# Sistema de Gestión de Sensores

## Descripción
Sistema de gestión de sensores IoT con interfaz gráfica desarrollado en Python usando Tkinter. El proyecto implementa integración con tres bases de datos:
- MongoDB Atlas para almacenamiento de datos
- Neo4j Aura para relaciones complejas
- Redis Cloud para caché y sesiones

## Características principales
- Gestión de sensores y mediciones
- Sistema de alertas climáticas
- Análisis de datos y generación de informes
- Facturación y transacciones
- Chat y mensajería entre usuarios
- Dashboard con estadísticas en tiempo real
- Servicios de consulta en línea por ubicación

## Requisitos
- Python 3.8+
- MongoDB Atlas
- Neo4j Aura
- Redis Cloud

## Instalación
```bash
pip install -r requirements.txt
```

## Configuración
Configurar las credenciales de conexión en los archivos de configuración:
- `backend/app/config_mongodb_real.py`
- `backend/app/config_neo4j.py`
- `backend/app/config_redis.py`

## Uso
```bash
python aplicacion_sensores_final.py
```

## Tecnologías
- Python
- Tkinter
- MongoDB
- Neo4j
- Redis
- Python MongoDB Driver
- Neo4j Driver for Python
- Redis-py

