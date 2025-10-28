"""
Configuración MongoDB Atlas - CONNECTION STRING REAL
Sistema de Gestión de Sensores - Trabajo Práctico Ingeniería de Datos II
"""

import os
from typing import Optional

class ConfiguracionMongoDBReal:
    """Configuración específica para MongoDB Atlas con connection string real"""
    
    def __init__(self):
        # CONNECTION STRING REAL DE TU MONGODB ATLAS
        # REEMPLAZA <db_password> CON TU CONTRASEÑA REAL
        self.mongodb_atlas_url = "mongodb+srv://alexis:Alexis2011@cluster0.3chmx0v.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.mongodb_database = "sensors_db"
        
        # Configuración Redis (para pruebas locales)
        self.redis_url = "redis://localhost:6379"
        
        # Configuración Neo4j (para pruebas locales)
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "password123"
        
        # Configuración de prueba
        self.modo_prueba = True
        self.timeout_conexion = 10
        
    def configurar_password(self, password: str):
        """Configurar la contraseña real"""
        self.mongodb_atlas_url = self.mongodb_atlas_url.replace("<db_password>", password)
        
    def obtener_connection_string(self) -> str:
        """Obtener string de conexión completo"""
        return f"{self.mongodb_atlas_url}&appName=sensors-app"
    
    def obtener_configuracion_completa(self) -> dict:
        """Obtener configuración completa para modo híbrido"""
        return {
            "mongodb_url": self.mongodb_atlas_url,
            "mongodb_database": self.mongodb_database,
            "redis_url": self.redis_url,
            "neo4j_uri": self.neo4j_uri,
            "neo4j_user": self.neo4j_user,
            "neo4j_password": self.neo4j_password,
            "modo_prueba": self.modo_prueba,
            "timeout": self.timeout_conexion
        }
    
    def validar_configuracion(self) -> bool:
        """Validar que la configuración esté completa"""
        return all([
            self.mongodb_atlas_url and "<db_password>" not in self.mongodb_atlas_url,
            self.mongodb_database,
            self.redis_url,
            self.neo4j_uri,
            self.neo4j_user,
            self.neo4j_password
        ])

# Instancia global
config_mongodb_real = ConfiguracionMongoDBReal()
