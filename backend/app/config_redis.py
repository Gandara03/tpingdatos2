"""
Configuración Redis - Especialista en Velocidad
Sistema de Gestión de Sensores - Trabajo Práctico Ingeniería de Datos II

ARQUITECTURA:
- MongoDB Atlas: Sensores, Mediciones, Usuarios, Transacciones ACID, Alertas
- Neo4j Aura: Mensajes, Grupos, Relaciones complejas (en creación)
- Redis: Sesiones, Cache, Datos en tiempo real
"""

import os
from typing import Optional, Dict, Any

class ConfiguracionRedis:
    """Configuración específica para Redis"""
    
    def __init__(self):
        # Configuración Redis local (para desarrollo)
        self.redis_host = "localhost"
        self.redis_port = 6379
        self.redis_password = None
        self.redis_db = 0
        
        # Configuración Redis Cloud (para producción)
        self.redis_cloud_url = "redis://default:dcr1Tf7QohchOEpj4QN2RFUL070NBr3y@redis-15332.crce181.sa-east-1-2.ec2.redns.redis-cloud.com:15332"
        self.redis_cloud_host = "redis-15332.crce181.sa-east-1-2.ec2.redns.redis-cloud.com"
        self.redis_cloud_port = 15332
        self.redis_cloud_password = "dcr1Tf7QohchOEpj4QN2RFUL070NBr3y"
        
        # Configuración de conexión
        self.timeout_conexion = 5
        self.max_retries = 3
        self.retry_delay = 1
        
        # Configuración de TTL (Time To Live)
        self.ttl_sesiones = 3600  # 1 hora
        self.ttl_cache_sensores = 300  # 5 minutos
        self.ttl_cache_usuarios = 1800  # 30 minutos
        self.ttl_cache_alertas = 60  # 1 minuto
        
        # Configuración de claves
        self.prefijo_sesiones = "session:"
        self.prefijo_cache_sensores = "cache:sensors:"
        self.prefijo_cache_usuarios = "cache:users:"
        self.prefijo_cache_alertas = "cache:alerts:"
        self.prefijo_cache_mediciones = "cache:measurements:"
        
        # Configuración de pools
        self.max_connections = 20
        self.min_connections = 5
    
    def obtener_configuracion_local(self) -> Dict[str, Any]:
        """Obtener configuración para Redis local"""
        return {
            "host": self.redis_host,
            "port": self.redis_port,
            "password": self.redis_password,
            "db": self.redis_db,
            "timeout": self.timeout_conexion,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "max_connections": self.max_connections,
            "min_connections": self.min_connections
        }
    
    def obtener_configuracion_cloud(self) -> Dict[str, Any]:
        """Obtener configuración para Redis Cloud"""
        return {
            "host": self.redis_cloud_host,
            "port": self.redis_cloud_port,
            "password": self.redis_cloud_password,
            "db": self.redis_db,
            "timeout": self.timeout_conexion,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "max_connections": self.max_connections,
            "min_connections": self.min_connections
        }
    
    def obtener_configuracion_url(self) -> Dict[str, Any]:
        """Obtener configuración usando URL"""
        return {
            "url": self.redis_cloud_url,
            "timeout": self.timeout_conexion,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "max_connections": self.max_connections,
            "min_connections": self.min_connections
        }
    
    def validar_configuracion(self) -> bool:
        """Validar que la configuración esté completa"""
        return all([
            self.redis_host,
            self.redis_port,
            self.timeout_conexion
        ])

# Instancia global
config_redis = ConfiguracionRedis()
