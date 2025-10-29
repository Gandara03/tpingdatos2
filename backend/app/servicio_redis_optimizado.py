"""
Servicio Redis Optimizado - Especialista en Velocidad
Sistema de Gestión de Sensores - Trabajo Práctico Ingeniería de Datos II

ARQUITECTURA:
- MongoDB Atlas: Sensores, Mediciones, Usuarios, Transacciones ACID, Alertas
- Neo4j Aura: Mensajes, Grupos, Relaciones complejas (en creación)
- Redis: Sesiones, Cache, Datos en tiempo real
"""

import redis
import json
import pickle
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
import logging

class ServicioRedisOptimizado:
    """Servicio optimizado para Redis con arquitectura especializada en velocidad"""
    
    def __init__(self, host: str = "localhost", port: int = 6379, password: str = None, 
                 db: int = 0, timeout: int = 5, max_retries: int = 3):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.timeout = timeout
        self.max_retries = max_retries
        
        self.redis_client = None
        self.conectado = False
        
        # Configuración de TTL
        self.ttl_sesiones = 3600  # 1 hora
        self.ttl_cache_sensores = 300  # 5 minutos
        self.ttl_cache_usuarios = 1800  # 30 minutos
        self.ttl_cache_alertas = 60  # 1 minuto
        
        # Prefijos de claves
        self.prefijo_sesiones = "session:"
        self.prefijo_sesiones_cerradas = "session:closed:"
        self.prefijo_cache_sensores = "cache:sensors:"
        self.prefijo_cache_usuarios = "cache:users:"
        self.prefijo_cache_alertas = "cache:alerts:"
        self.prefijo_cache_mediciones = "cache:measurements:"
    
    def conectar(self) -> bool:
        """Conectar a Redis"""
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                socket_timeout=self.timeout,
                socket_connect_timeout=self.timeout,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # Probar conexión
            self.redis_client.ping()
            
            self.conectado = True
            return True
            
        except Exception as e:
            print(f"❌ Error conectando a Redis: {e}")
            self.conectado = False
            return False
    
    def desconectar(self):
        """Desconectar de Redis"""
        if self.redis_client:
            self.redis_client.close()
            self.conectado = False
            print("🔌 Desconectado de Redis")
    
    def crear_sesion(self, user_id: str, email: str, role: str, session_data: Dict[str, Any] = None) -> str:
        """Crear sesión de usuario"""
        if not self.conectado:
            return None
        
        try:
            session_id = f"{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            session_key = f"{self.prefijo_sesiones}{session_id}"
            
            session_info = {
                "session_id": session_id,
                "user_id": user_id,
                "email": email,
                "role": role,
                "created_at": datetime.now().isoformat(),
                "last_activity": datetime.now().isoformat(),
                "status": "active"
            }
            
            if session_data:
                session_info.update(session_data)
            
            # Guardar sesión con TTL
            self.redis_client.setex(
                session_key,
                self.ttl_sesiones,
                json.dumps(session_info)
            )
            
            print(f"✅ Sesión creada para {email} (TTL: {self.ttl_sesiones}s)")
            return session_id
            
        except Exception as e:
            print(f"❌ Error creando sesión: {e}")
            return None
    
    def validar_sesion(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Validar sesión de usuario"""
        if not self.conectado:
            return None
        
        try:
            session_key = f"{self.prefijo_sesiones}{session_id}"
            session_data = self.redis_client.get(session_key)
            
            if session_data:
                session_info = json.loads(session_data)
                
                # Actualizar última actividad
                session_info["last_activity"] = datetime.now().isoformat()
                self.redis_client.setex(
                    session_key,
                    self.ttl_sesiones,
                    json.dumps(session_info)
                )
                
                return session_info
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error validando sesión: {e}")
            return None
    
    def cerrar_sesion(self, session_id: str) -> bool:
        """Cerrar sesión de usuario"""
        if not self.conectado:
            return False
        
        try:
            session_key = f"{self.prefijo_sesiones}{session_id}"
            # Recuperar la sesión actual si existe
            session_data = self.redis_client.get(session_key)
            closed_at = datetime.now().isoformat()

            if session_data:
                try:
                    session_info = json.loads(session_data)
                except Exception:
                    # Si no es JSON, intentar pickle
                    try:
                        session_info = pickle.loads(session_data)
                    except Exception:
                        session_info = {"session_id": session_id}

                # Marcar cierre y estado
                session_info["closed_at"] = closed_at
                session_info["status"] = "closed"

                # Archivar sesión cerrada con TTL (mismo TTL que sesiones activas)
                closed_key = f"{self.prefijo_sesiones_cerradas}{session_id}"
                try:
                    self.redis_client.setex(closed_key, self.ttl_sesiones, json.dumps(session_info))
                except Exception:
                    # Fallback a set normal si falla setex
                    self.redis_client.set(closed_key, json.dumps(session_info))

            # Eliminar sesión activa
            result = self.redis_client.delete(session_key)

            if result:
                print(f"✅ Sesión {session_id} cerrada (closed_at={closed_at})")
                return True
            else:
                print(f"⚠️ Sesión {session_id} no encontrada para cierre")
                return False
                
        except Exception as e:
            print(f"❌ Error cerrando sesión: {e}")
            return False
    
    def cachear_sensores(self, sensores: List[Dict[str, Any]]) -> bool:
        """Cachear lista de sensores"""
        if not self.conectado:
            return False
        
        try:
            cache_key = f"{self.prefijo_cache_sensores}all"
            self.redis_client.setex(
                cache_key,
                self.ttl_cache_sensores,
                json.dumps(sensores)
            )
            
            print(f"✅ {len(sensores)} sensores cacheados (TTL: {self.ttl_cache_sensores}s)")
            return True
            
        except Exception as e:
            print(f"❌ Error cacheando sensores: {e}")
            return False
    
    def obtener_sensores_cache(self) -> Optional[List[Dict[str, Any]]]:
        """Obtener sensores del cache"""
        if not self.conectado:
            return None
        
        try:
            cache_key = f"{self.prefijo_cache_sensores}all"
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                sensores = json.loads(cached_data)
                print(f"✅ {len(sensores)} sensores obtenidos del cache")
                return sensores
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error obteniendo sensores del cache: {e}")
            return None
    
    def cachear_usuario(self, user_id: str, usuario_data: Dict[str, Any]) -> bool:
        """Cachear datos de usuario"""
        if not self.conectado:
            return False
        
        try:
            cache_key = f"{self.prefijo_cache_usuarios}{user_id}"
            self.redis_client.setex(
                cache_key,
                self.ttl_cache_usuarios,
                json.dumps(usuario_data)
            )
            
            print(f"✅ Usuario {user_id} cacheado (TTL: {self.ttl_cache_usuarios}s)")
            return True
            
        except Exception as e:
            print(f"❌ Error cacheando usuario: {e}")
            return False
    
    def obtener_usuario_cache(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Obtener usuario del cache"""
        if not self.conectado:
            return None
        
        try:
            cache_key = f"{self.prefijo_cache_usuarios}{user_id}"
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                usuario = json.loads(cached_data)
                print(f"✅ Usuario {user_id} obtenido del cache")
                return usuario
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error obteniendo usuario del cache: {e}")
            return None
    
    def cachear_alertas(self, alertas: List[Dict[str, Any]]) -> bool:
        """Cachear alertas"""
        if not self.conectado:
            return False
        
        try:
            cache_key = f"{self.prefijo_cache_alertas}recent"
            self.redis_client.setex(
                cache_key,
                self.ttl_cache_alertas,
                json.dumps(alertas)
            )
            
            print(f"✅ {len(alertas)} alertas cacheadas (TTL: {self.ttl_cache_alertas}s)")
            return True
            
        except Exception as e:
            print(f"❌ Error cacheando alertas: {e}")
            return False
    
    def obtener_alertas_cache(self) -> Optional[List[Dict[str, Any]]]:
        """Obtener alertas del cache"""
        if not self.conectado:
            return None
        
        try:
            cache_key = f"{self.prefijo_cache_alertas}recent"
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                alertas = json.loads(cached_data)
                print(f"✅ {len(alertas)} alertas obtenidas del cache")
                return alertas
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error obteniendo alertas del cache: {e}")
            return None
    
    def cachear_mediciones(self, sensor_id: str, mediciones: List[Dict[str, Any]]) -> bool:
        """Cachear mediciones de un sensor"""
        if not self.conectado:
            return False
        
        try:
            cache_key = f"{self.prefijo_cache_mediciones}{sensor_id}"
            self.redis_client.setex(
                cache_key,
                self.ttl_cache_sensores,  # Mismo TTL que sensores
                json.dumps(mediciones)
            )
            
            print(f"✅ {len(mediciones)} mediciones del sensor {sensor_id} cacheadas")
            return True
            
        except Exception as e:
            print(f"❌ Error cacheando mediciones: {e}")
            return False
    
    def obtener_mediciones_cache(self, sensor_id: str) -> Optional[List[Dict[str, Any]]]:
        """Obtener mediciones del cache"""
        if not self.conectado:
            return None
        
        try:
            cache_key = f"{self.prefijo_cache_mediciones}{sensor_id}"
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                mediciones = json.loads(cached_data)
                print(f"✅ {len(mediciones)} mediciones del sensor {sensor_id} obtenidas del cache")
                return mediciones
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error obteniendo mediciones del cache: {e}")
            return None
    
    def limpiar_cache(self, patron: str = None) -> int:
        """Limpiar cache"""
        if not self.conectado:
            return 0
        
        try:
            if patron:
                # Limpiar claves específicas
                keys = self.redis_client.keys(patron)
                if keys:
                    deleted = self.redis_client.delete(*keys)
                    print(f"✅ {deleted} claves eliminadas del cache")
                    return deleted
            else:
                # Limpiar todo el cache
                self.redis_client.flushdb()
                print("✅ Cache completamente limpiado")
                return 1
                
        except Exception as e:
            print(f"❌ Error limpiando cache: {e}")
            return 0
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtener estadísticas de Redis"""
        if not self.conectado:
            return {"error": "No conectado"}
        
        try:
            info = self.redis_client.info()
            
            # Contar claves por prefijo
            sesiones = len(self.redis_client.keys(f"{self.prefijo_sesiones}*"))
            cache_sensores = len(self.redis_client.keys(f"{self.prefijo_cache_sensores}*"))
            cache_usuarios = len(self.redis_client.keys(f"{self.prefijo_cache_usuarios}*"))
            cache_alertas = len(self.redis_client.keys(f"{self.prefijo_cache_alertas}*"))
            cache_mediciones = len(self.redis_client.keys(f"{self.prefijo_cache_mediciones}*"))
            
            return {
                "host": self.host,
                "port": self.port,
                "db": self.db,
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "sesiones_activas": sesiones,
                "cache_sensores": cache_sensores,
                "cache_usuarios": cache_usuarios,
                "cache_alertas": cache_alertas,
                "cache_mediciones": cache_mediciones,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def obtener_estado_conexion(self) -> Dict[str, Any]:
        """Obtener estado de la conexión"""
        return {
            "conectado": self.conectado,
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "timestamp": datetime.now().isoformat()
        }
    
    # Métodos genéricos para compatibilidad con la aplicación
    def set(self, key: str, value: str, ttl: int = None) -> bool:
        """Establecer valor con TTL opcional"""
        if not self.conectado:
            return False
        
        try:
            if ttl:
                self.redis_client.setex(key, ttl, value)
            else:
                self.redis_client.set(key, value)
            return True
        except Exception as e:
            print(f"❌ Error estableciendo {key}: {e}")
            return False
    
    def get(self, key: str) -> Optional[str]:
        """Obtener valor"""
        if not self.conectado:
            return None
        
        try:
            return self.redis_client.get(key)
        except Exception as e:
            print(f"❌ Error obteniendo {key}: {e}")
            return None
    
    def hset(self, key: str, data: Dict[str, Any], ttl: int = None) -> bool:
        """Establecer hash con TTL opcional"""
        if not self.conectado:
            return False
        
        try:
            # Convertir valores a strings para Redis
            converted_data = {}
            for k, v in data.items():
                if isinstance(v, list):
                    converted_data[k] = json.dumps(v)
                elif isinstance(v, dict):
                    converted_data[k] = json.dumps(v)
                else:
                    converted_data[k] = str(v)
            
            self.redis_client.hset(key, mapping=converted_data)
            if ttl:
                self.redis_client.expire(key, ttl)
            return True
        except Exception as e:
            print(f"❌ Error estableciendo hash {key}: {e}")
            return False
    
    def hgetall(self, key: str) -> Optional[Dict[str, Any]]:
        """Obtener hash completo"""
        if not self.conectado:
            return None
        
        try:
            return self.redis_client.hgetall(key)
        except Exception as e:
            print(f"❌ Error obteniendo hash {key}: {e}")
            return None
    
    def lpush(self, key: str, values: List[str]) -> bool:
        """Agregar valores a lista"""
        if not self.conectado:
            return False
        
        try:
            self.redis_client.lpush(key, *values)
            return True
        except Exception as e:
            print(f"❌ Error agregando a lista {key}: {e}")
            return False
    
    def lrange(self, key: str, start: int, end: int) -> Optional[List[str]]:
        """Obtener rango de lista"""
        if not self.conectado:
            return None
        
        try:
            return self.redis_client.lrange(key, start, end)
        except Exception as e:
            print(f"❌ Error obteniendo lista {key}: {e}")
            return None
    
    def sadd(self, key: str, values: List[str]) -> bool:
        """Agregar valores a set"""
        if not self.conectado:
            return False
        
        try:
            self.redis_client.sadd(key, *values)
            return True
        except Exception as e:
            print(f"❌ Error agregando a set {key}: {e}")
            return False
    
    def smembers(self, key: str) -> Optional[set]:
        """Obtener miembros de set"""
        if not self.conectado:
            return None
        
        try:
            return self.redis_client.smembers(key)
        except Exception as e:
            print(f"❌ Error obteniendo set {key}: {e}")
            return None
    
    def ttl(self, key: str) -> int:
        """Obtener TTL de clave"""
        if not self.conectado:
            return -1
        
        try:
            return self.redis_client.ttl(key)
        except Exception as e:
            print(f"❌ Error obteniendo TTL {key}: {e}")
            return -1
    
    def delete(self, *keys: str) -> int:
        """Eliminar claves"""
        if not self.conectado:
            return 0
        
        try:
            return self.redis_client.delete(*keys)
        except Exception as e:
            print(f"❌ Error eliminando claves: {e}")
            return 0
    
    def keys(self, pattern: str) -> List[str]:
        """Obtener claves que coincidan con patrón"""
        if not self.conectado:
            return []
        
        try:
            return self.redis_client.keys(pattern)
        except Exception as e:
            print(f"❌ Error obteniendo claves {pattern}: {e}")
            return []
    
    def info(self) -> Dict[str, Any]:
        """Obtener información del servidor"""
        if not self.conectado:
            return {}
        
        try:
            return self.redis_client.info()
        except Exception as e:
            print(f"❌ Error obteniendo info: {e}")
            return {}
