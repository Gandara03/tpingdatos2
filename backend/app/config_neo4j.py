"""
Configuración Neo4j - Especialista en Relaciones
Sistema de Gestión de Sensores - Trabajo Práctico Ingeniería de Datos II

ARQUITECTURA:
- MongoDB Atlas: Sensores, Mediciones, Usuarios, Transacciones ACID, Alertas
- Neo4j: Mensajes, Grupos, Relaciones complejas de comunicación
- Redis: Sesiones, Cache (pendiente)
"""

import os
from typing import Optional, Dict, Any

class ConfiguracionNeo4j:
    """Configuración específica para Neo4j"""
    
    def __init__(self):
        # Configuración Neo4j local (para desarrollo)
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "password123"
        
        # Configuración Neo4j Aura (para producción)
        self.neo4j_aura_uri = "neo4j+s://92ce3bd5.databases.neo4j.io"
        self.neo4j_aura_user = "neo4j"
        self.neo4j_aura_password = "E7GNKq6zR-Yj0VewH1zpP5YH6KQlEMETVGHiyoXSJqc"
        
        # Configuración de base de datos
        self.database_name = "neo4j"
        
        # Configuración de conexión
        self.timeout_conexion = 10
        self.max_retries = 3
        
        # Configuración de índices
        self.indices_requeridos = [
            "CREATE INDEX user_id_index IF NOT EXISTS FOR (u:User) ON (u.user_id)",
            "CREATE INDEX message_id_index IF NOT EXISTS FOR (m:Message) ON (m.message_id)",
            "CREATE INDEX group_id_index IF NOT EXISTS FOR (g:Group) ON (g.group_id)",
            "CREATE INDEX created_at_index IF NOT EXISTS FOR (m:Message) ON (m.created_at)"
        ]
        
        # Configuración de constraints
        self.constraints_requeridos = [
            "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
            "CREATE CONSTRAINT message_id_unique IF NOT EXISTS FOR (m:Message) REQUIRE m.message_id IS UNIQUE",
            "CREATE CONSTRAINT group_id_unique IF NOT EXISTS FOR (g:Group) REQUIRE g.group_id IS UNIQUE"
        ]
    
    def obtener_configuracion_local(self) -> Dict[str, Any]:
        """Obtener configuración para Neo4j local"""
        return {
            "uri": self.neo4j_uri,
            "user": self.neo4j_user,
            "password": self.neo4j_password,
            "database": self.database_name,
            "timeout": self.timeout_conexion,
            "max_retries": self.max_retries
        }
    
    def obtener_configuracion_aura(self) -> Dict[str, Any]:
        """Obtener configuración para Neo4j Aura"""
        return {
            "uri": self.neo4j_aura_uri,
            "user": self.neo4j_aura_user,
            "password": self.neo4j_aura_password,
            "database": self.database_name,
            "timeout": self.timeout_conexion,
            "max_retries": self.max_retries
        }
    
    def validar_configuracion(self) -> bool:
        """Validar que la configuración esté completa"""
        return all([
            self.neo4j_uri,
            self.neo4j_user,
            self.neo4j_password,
            self.database_name
        ])

# Instancia global
config_neo4j = ConfiguracionNeo4j()
