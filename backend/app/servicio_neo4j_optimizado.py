"""
Servicio Neo4j Optimizado - Especialista en Relaciones
Sistema de Gestión de Sensores - Trabajo Práctico Ingeniería de Datos II

ARQUITECTURA:
- MongoDB Atlas: Sensores, Mediciones, Usuarios, Transacciones ACID, Alertas
- Neo4j: Mensajes, Grupos, Relaciones complejas de comunicación
- Redis: Sesiones, Cache (pendiente)
"""

from neo4j import GraphDatabase
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

class ServicioNeo4jOptimizado:
    """Servicio optimizado para Neo4j con arquitectura especializada en relaciones"""
    
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None
        self.conectado = False
        
    def conectar(self) -> bool:
        """Conectar a Neo4j"""
        try:
            # Para Neo4j Aura (neo4j+s), no necesitamos parámetros de encriptación
            # El esquema neo4j+s maneja la encriptación automáticamente
            if self.uri.startswith("neo4j+s://"):
                self.driver = GraphDatabase.driver(
                    self.uri, 
                    auth=(self.user, self.password),
                    max_connection_lifetime=30 * 60,  # 30 minutos
                    max_connection_pool_size=50,
                    connection_acquisition_timeout=2 * 60  # 2 minutos
                )
            else:
                # Para conexiones locales (bolt://)
                self.driver = GraphDatabase.driver(
                    self.uri, 
                    auth=(self.user, self.password),
                    max_connection_lifetime=30 * 60,  # 30 minutos
                    max_connection_pool_size=50,
                    connection_acquisition_timeout=2 * 60,  # 2 minutos
                    encrypted=False  # Para desarrollo local
                )
            
            # Probar conexión
            with self.driver.session(database=self.database) as session:
                session.run("RETURN 1")
            
            self.conectado = True
            return True
            
        except Exception as e:
            print(f"❌ Error conectando a Neo4j: {e}")
            self.conectado = False
            return False
    
    def desconectar(self):
        """Desconectar de Neo4j"""
        if self.driver:
            self.driver.close()
            self.conectado = False
            print("🔌 Desconectado de Neo4j")
    
    def configurar_esquema(self):
        """Configurar esquema de Neo4j con índices y constraints"""
        if not self.conectado:
            return False
        
        try:
            print("🏗️ Configurando esquema de Neo4j...")
            
            with self.driver.session(database=self.database) as session:
                # Crear constraints
                constraints = [
                    "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
                    "CREATE CONSTRAINT message_id_unique IF NOT EXISTS FOR (m:Message) REQUIRE m.message_id IS UNIQUE",
                    "CREATE CONSTRAINT group_id_unique IF NOT EXISTS FOR (g:Group) REQUIRE g.group_id IS UNIQUE"
                ]
                
                for constraint in constraints:
                    try:
                        session.run(constraint)
                        print(f"   ✅ Constraint creado")
                    except Exception as e:
                        print(f"   ⚠️ Constraint ya existe o error: {e}")
                
                # Crear índices
                indices = [
                    "CREATE INDEX user_id_index IF NOT EXISTS FOR (u:User) ON (u.user_id)",
                    "CREATE INDEX message_id_index IF NOT EXISTS FOR (m:Message) ON (m.message_id)",
                    "CREATE INDEX group_id_index IF NOT EXISTS FOR (g:Group) ON (g.group_id)",
                    "CREATE INDEX created_at_index IF NOT EXISTS FOR (m:Message) ON (m.created_at)",
                    "CREATE INDEX message_type_index IF NOT EXISTS FOR (m:Message) ON (m.type)"
                ]
                
                for index in indices:
                    try:
                        session.run(index)
                        print(f"   ✅ Índice creado")
                    except Exception as e:
                        print(f"   ⚠️ Índice ya existe o error: {e}")
                
                print("✅ Esquema de Neo4j configurado")
                return True
                
        except Exception as e:
            print(f"❌ Error configurando esquema: {e}")
            return False
    
    def crear_usuario(self, user_id: str, email: str, full_name: str, role: str) -> bool:
        """Crear usuario en Neo4j"""
        if not self.conectado:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MERGE (u:User {user_id: $user_id})
                SET u.email = $email,
                    u.full_name = $full_name,
                    u.role = $role,
                    u.created_at = datetime(),
                    u.updated_at = datetime()
                RETURN u
                """
                
                result = session.run(query, {
                    "user_id": user_id,
                    "email": email,
                    "full_name": full_name,
                    "role": role
                })
                
                if result.single():
                    print(f"✅ Usuario {full_name} creado/actualizado en Neo4j")
                    return True
                return False
                
        except Exception as e:
            print(f"❌ Error creando usuario: {e}")
            return False
    
    def crear_mensaje(self, message_id: str, sender_id: str, recipient_id: str, 
                     subject: str, content: str, message_type: str = "private") -> bool:
        """Crear mensaje en Neo4j"""
        if not self.conectado:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (sender:User {user_id: $sender_id})
                MATCH (recipient:User {user_id: $recipient_id})
                CREATE (m:Message {
                    message_id: $message_id,
                    subject: $subject,
                    content: $content,
                    type: $message_type,
                    created_at: datetime(),
                    status: 'sent'
                })
                CREATE (sender)-[:SENT]->(m)
                CREATE (m)-[:SENT_TO]->(recipient)
                RETURN m
                """
                
                result = session.run(query, {
                    "message_id": message_id,
                    "sender_id": sender_id,
                    "recipient_id": recipient_id,
                    "subject": subject,
                    "content": content,
                    "message_type": message_type
                })
                
                if result.single():
                    print(f"✅ Mensaje '{subject}' creado en Neo4j")
                    return True
                return False
                
        except Exception as e:
            print(f"❌ Error creando mensaje: {e}")
            return False
    
    def crear_grupo(self, group_id: str, group_name: str, description: str, 
                   admin_id: str) -> bool:
        """Crear grupo en Neo4j"""
        if not self.conectado:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (admin:User {user_id: $admin_id})
                CREATE (g:Group {
                    group_id: $group_id,
                    name: $group_name,
                    description: $description,
                    created_at: datetime(),
                    updated_at: datetime()
                })
                CREATE (admin)-[:ADMIN_OF]->(g)
                CREATE (admin)-[:MEMBER_OF]->(g)
                RETURN g
                """
                
                result = session.run(query, {
                    "group_id": group_id,
                    "group_name": group_name,
                    "description": description,
                    "admin_id": admin_id
                })
                
                if result.single():
                    print(f"✅ Grupo '{group_name}' creado en Neo4j")
                    return True
                return False
                
        except Exception as e:
            print(f"❌ Error creando grupo: {e}")
            return False
    
    def agregar_miembro_grupo(self, group_id: str, user_id: str) -> bool:
        """Agregar miembro a grupo"""
        if not self.conectado:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (g:Group {group_id: $group_id})
                MATCH (u:User {user_id: $user_id})
                MERGE (u)-[:MEMBER_OF]->(g)
                RETURN g, u
                """
                
                result = session.run(query, {
                    "group_id": group_id,
                    "user_id": user_id
                })
                
                if result.single():
                    print(f"✅ Usuario {user_id} agregado al grupo {group_id}")
                    return True
                return False
                
        except Exception as e:
            print(f"❌ Error agregando miembro: {e}")
            return False
    
    def crear_mensaje_grupal(self, message_id: str, sender_id: str, group_id: str,
                            subject: str, content: str) -> bool:
        """Crear mensaje grupal"""
        if not self.conectado:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (sender:User {user_id: $sender_id})
                MATCH (g:Group {group_id: $group_id})
                CREATE (m:Message {
                    message_id: $message_id,
                    subject: $subject,
                    content: $content,
                    type: 'group',
                    created_at: datetime(),
                    status: 'sent'
                })
                CREATE (sender)-[:SENT]->(m)
                CREATE (m)-[:SENT_TO_GROUP]->(g)
                RETURN m
                """
                
                result = session.run(query, {
                    "message_id": message_id,
                    "sender_id": sender_id,
                    "group_id": group_id,
                    "subject": subject,
                    "content": content
                })
                
                if result.single():
                    print(f"✅ Mensaje grupal '{subject}' creado en Neo4j")
                    return True
                return False
                
        except Exception as e:
            print(f"❌ Error creando mensaje grupal: {e}")
            return False
    
    def obtener_mensajes_usuario(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Obtener mensajes de un usuario"""
        if not self.conectado:
            return []
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (u:User {user_id: $user_id})
                MATCH (sender:User)-[:SENT]->(m:Message)-[:SENT_TO]->(u)
                RETURN m.message_id as message_id,
                       m.subject as subject,
                       m.content as content,
                       m.type as type,
                       m.created_at as created_at,
                       sender.full_name as sender_name,
                       sender.user_id as sender_id
                ORDER BY m.created_at DESC
                LIMIT $limit
                """
                
                result = session.run(query, {"user_id": user_id, "limit": limit})
                mensajes = []
                
                for record in result:
                    mensajes.append({
                        "message_id": record["message_id"],
                        "subject": record["subject"],
                        "content": record["content"],
                        "type": record["type"],
                        "created_at": record["created_at"],
                        "sender_name": record["sender_name"],
                        "sender_id": record["sender_id"]
                    })
                
                return mensajes
                
        except Exception as e:
            print(f"❌ Error obteniendo mensajes: {e}")
            return []
    
    def obtener_mensajes_grupales_usuario(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Obtener mensajes grupales de un usuario"""
        if not self.conectado:
            return []
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (u:User {user_id: $user_id})-[:MEMBER_OF]->(g:Group)
                MATCH (sender:User)-[:SENT]->(m:Message)-[:SENT_TO_GROUP]->(g)
                RETURN m.message_id as message_id,
                       m.subject as subject,
                       m.content as content,
                       m.type as type,
                       m.created_at as created_at,
                       sender.full_name as sender_name,
                       sender.user_id as sender_id,
                       g.name as group_name,
                       g.group_id as group_id
                ORDER BY m.created_at DESC
                LIMIT $limit
                """
                
                result = session.run(query, {"user_id": user_id, "limit": limit})
                mensajes = []
                
                for record in result:
                    mensajes.append({
                        "message_id": record["message_id"],
                        "subject": record["subject"],
                        "content": record["content"],
                        "type": record["type"],
                        "created_at": record["created_at"],
                        "sender_name": record["sender_name"],
                        "sender_id": record["sender_id"],
                        "group_name": record["group_name"],
                        "group_id": record["group_id"]
                    })
                
                return mensajes
                
        except Exception as e:
            print(f"❌ Error obteniendo mensajes grupales del usuario {user_id}: {e}")
            return []
    
    def obtener_grupos_usuario(self, user_id: str) -> List[Dict[str, Any]]:
        """Obtener grupos de un usuario"""
        if not self.conectado:
            return []
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (u:User {user_id: $user_id})-[:MEMBER_OF]->(g:Group)
                RETURN g.group_id as group_id,
                       g.name as name,
                       g.description as description,
                       g.created_at as created_at
                ORDER BY g.created_at DESC
                """
                
                result = session.run(query, {"user_id": user_id})
                grupos = []
                
                for record in result:
                    grupos.append({
                        "group_id": record["group_id"],
                        "name": record["name"],
                        "description": record["description"],
                        "created_at": record["created_at"]
                    })
                
                return grupos
                
        except Exception as e:
            print(f"❌ Error obteniendo grupos: {e}")
            return []
    
    def obtener_miembros_grupo(self, group_id: str) -> List[Dict[str, Any]]:
        """Obtener miembros de un grupo específico"""
        if not self.conectado:
            return []
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (u:User)-[r:MEMBER_OF]->(g:Group {group_id: $group_id})
                RETURN u.user_id as user_id, u.full_name as full_name, u.email as email, 
                       r.role as role, r.joined_at as joined_at, r.status as status
                ORDER BY r.joined_at ASC
                """
                
                result = session.run(query, {"group_id": group_id})
                miembros = []
                
                for record in result:
                    miembros.append({
                        "user_id": record["user_id"],
                        "full_name": record["full_name"],
                        "email": record["email"],
                        "role": record["role"] or "member",
                        "joined_at": record["joined_at"],
                        "status": record["status"] or "active"
                    })
                
                return miembros
                
        except Exception as e:
            print(f"❌ Error obteniendo miembros del grupo: {e}")
            return []
    
    def agregar_miembro_grupo_real(self, group_id: str, user_id: str, role: str = "member") -> bool:
        """Agregar miembro a un grupo con datos reales"""
        if not self.conectado:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (u:User {user_id: $user_id})
                MATCH (g:Group {group_id: $group_id})
                MERGE (u)-[r:MEMBER_OF]->(g)
                SET r.role = $role, r.joined_at = datetime(), r.status = 'active'
                RETURN r
                """
                
                result = session.run(query, {
                    "user_id": user_id,
                    "group_id": group_id,
                    "role": role
                })
                
                if result.single():
                    print(f"✅ Usuario {user_id} agregado al grupo {group_id}")
                    return True
                return False
                
        except Exception as e:
            print(f"❌ Error agregando miembro al grupo: {e}")
            return False
    
    def remover_miembro_grupo(self, group_id: str, user_id: str) -> bool:
        """Remover miembro de un grupo"""
        if not self.conectado:
            return False
        
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (u:User {user_id: $user_id})-[r:MEMBER_OF]->(g:Group {group_id: $group_id})
                DELETE r
                RETURN count(r) as deleted
                """
                
                result = session.run(query, {
                    "user_id": user_id,
                    "group_id": group_id
                })
                
                record = result.single()
                if record and record["deleted"] > 0:
                    print(f"✅ Usuario {user_id} removido del grupo {group_id}")
                    return True
                return False
                
        except Exception as e:
            print(f"❌ Error removiendo miembro del grupo: {e}")
            return False
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtener estadísticas de Neo4j"""
        if not self.conectado:
            return {"error": "No conectado"}
        
        try:
            with self.driver.session(database=self.database) as session:
                # Contar nodos
                usuarios_query = "MATCH (u:User) RETURN count(u) as count"
                usuarios_result = session.run(usuarios_query)
                usuarios_count = usuarios_result.single()["count"]
                
                mensajes_query = "MATCH (m:Message) RETURN count(m) as count"
                mensajes_result = session.run(mensajes_query)
                mensajes_count = mensajes_result.single()["count"]
                
                grupos_query = "MATCH (g:Group) RETURN count(g) as count"
                grupos_result = session.run(grupos_query)
                grupos_count = grupos_result.single()["count"]
                
                # Contar relaciones
                relaciones_query = "MATCH ()-[r]->() RETURN count(r) as count"
                relaciones_result = session.run(relaciones_query)
                relaciones_count = relaciones_result.single()["count"]
                
                return {
                    "database": self.database,
                    "usuarios": usuarios_count,
                    "mensajes": mensajes_count,
                    "grupos": grupos_count,
                    "relaciones": relaciones_count,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def obtener_estado_conexion(self) -> Dict[str, Any]:
        """Obtener estado de la conexión"""
        return {
            "conectado": self.conectado,
            "uri": self.uri,
            "database": self.database,
            "timestamp": datetime.now().isoformat()
        }
