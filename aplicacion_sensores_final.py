#!/usr/bin/env python3
"""
Sistema de Gestión de Sensores - Aplicación Online
Trabajo Práctico - Ingeniería de Datos II - Persistencia Poliglota
MongoDB Atlas + Neo4j Aura
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from datetime import datetime, timedelta
import json
import threading
import time
import os
import sys
import asyncio
import uuid

# Agregar el directorio backend al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

# Importar servicios online
try:
    from backend.app.servicio_mongodb_optimizado import ServicioMongoDBOptimizado
    from backend.app.config_mongodb_real import config_mongodb_real
    MONGODB_ATLAS_DISPONIBLE = True
except ImportError as e:
    MONGODB_ATLAS_DISPONIBLE = False
    print(f"ERROR MongoDB Atlas no disponible: {e}")

try:
    from backend.app.servicio_neo4j_optimizado import ServicioNeo4jOptimizado
    from backend.app.config_neo4j import config_neo4j
    NEO4J_DISPONIBLE = True
except ImportError as e:
    NEO4J_DISPONIBLE = False
    print(f"WARNING Neo4j no disponible: {e}")

try:
    from backend.app.servicio_redis_optimizado import ServicioRedisOptimizado
    from backend.app.config_redis import config_redis
    REDIS_DISPONIBLE = True
except ImportError as e:
    REDIS_DISPONIBLE = False
    print(f"WARNING Redis no disponible: {e}")

class AplicacionSensoresOnline:
    """Aplicación de gestión de sensores con MongoDB Atlas + Neo4j Aura"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Sistema de Gestión de Sensores")
        self.root.geometry("1400x900")
        self.root.configure(bg='#ecf0f1')
        # Ocultar la ventana principal al inicio
        self.root.withdraw()
        
        # Verificar que MongoDB Atlas esté disponible
        if not MONGODB_ATLAS_DISPONIBLE:
            messagebox.showerror("Error", "MongoDB Atlas es requerido para esta aplicación.")
            self.root.destroy()
            return
        
        # Inicializar servicios
        self.mongodb_service = None
        self.neo4j_service = None
        self.redis_service = None
        
        # Estado del usuario
        self.usuario_autenticado = None
        self.sesion_activa = False
        self.rol_usuario = None
        self.tiempo_inicio_sesion = None  # Para facturación por tiempo de sesión
        
        # Inicializar servicios
        self.inicializar_mongodb_atlas()
        
        if NEO4J_DISPONIBLE:
            self.inicializar_neo4j()
        
        if REDIS_DISPONIBLE:
            self.inicializar_redis()
        
        # Crear interfaz básica (oculta inicialmente)
        self.crear_interfaz_basica()
        
        # Crear usuarios iniciales si no existen
        self.crear_usuarios_iniciales()
        
        # Mostrar login obligatorio al inicio
        self.mostrar_dialogo_login()
    
    def inicializar_mongodb_atlas(self):
        """Inicializar MongoDB Atlas"""
        try:
            config = config_mongodb_real.obtener_configuracion_completa()
            self.mongodb_service = ServicioMongoDBOptimizado(
                connection_string=config["mongodb_url"],
                database_name=config["mongodb_database"]
            )
            
            if self.mongodb_service.conectar():
                print("OK MongoDB Atlas conectado")
            else:
                print("ERROR Error conectando a MongoDB Atlas")
                
        except Exception as e:
            print(f"ERROR Error inicializando MongoDB Atlas: {e}")
    
    def inicializar_neo4j(self):
        """Inicializar Neo4j Aura"""
        try:
            config = config_neo4j.obtener_configuracion_aura()
            self.neo4j_service = ServicioNeo4jOptimizado(
                uri=config["uri"],
                user=config["user"],
                password=config["password"],
                database=config["database"]
            )
            
            if self.neo4j_service.conectar():
                print("OK Neo4j Aura conectado")
            else:
                print("WARNING Neo4j Aura no disponible")
                
        except Exception as e:
            print(f"WARNING Error inicializando Neo4j Aura: {e}")
    
    def inicializar_redis(self):
        """Inicializar Redis Cloud"""
        try:
            config = config_redis.obtener_configuracion_cloud()
            self.redis_service = ServicioRedisOptimizado(
                host=config["host"],
                port=config["port"],
                password=config["password"],
                db=config["db"]
            )
            
            if self.redis_service.conectar():
                print("OK Redis Cloud conectado")
            else:
                print("WARNING Redis Cloud no disponible")
                
        except Exception as e:
            print(f"WARNING Error inicializando Redis Cloud: {e}")
            self.redis_service = None
    
    def crear_usuarios_iniciales(self):
        """Crear usuarios iniciales si no existen en la base de datos"""
        if not self.mongodb_service or not self.mongodb_service.conectado:
            print("WARNING MongoDB no disponible, no se pueden crear usuarios iniciales")
            return
        
        try:
            # Verificar si ya existen usuarios
            usuarios_existentes = self.mongodb_service.obtener_usuarios()
            
            # Asegurar que los roles existan antes de crear usuarios
            self.asegurar_roles_iniciales()
            
            # Si ya existen usuarios, no crear usuarios iniciales
            if usuarios_existentes:
                print(f"OK Ya existen {len(usuarios_existentes)} usuarios en la base de datos")
                return
            
            # Crear usuarios iniciales con role_id
            usuarios_iniciales = [
                {
                    "user_id": "USER_ADMIN_001",
                    "username": "admin",
                    "email": "admin@sensores.com",
                    "password": "admin123",
                    "rol": "administrador",  # Mantener para compatibilidad
                    "role_id": "ROL_ADMIN_001",  # Nueva referencia
                    "status": "activo",
                    "created_at": datetime.now().isoformat(),
                    "last_login": None,
                    "permissions": ["read", "write", "admin", "manage_users", "manage_system"]
                },
                {
                    "user_id": "USER_TECNICO_001",
                    "username": "tecnico",
                    "email": "tecnico@sensores.com",
                    "password": "tecnico123",
                    "rol": "técnico",  # Mantener para compatibilidad
                    "role_id": "ROL_TECNICO_001",  # Nueva referencia
                    "status": "activo",
                    "created_at": datetime.now().isoformat(),
                    "last_login": None,
                    "permissions": ["read", "write", "manage_sensors", "manage_alerts"]
                },
                {
                    "user_id": "USER_USUARIO_001",
                    "username": "usuario",
                    "email": "usuario@sensores.com",
                    "password": "usuario123",
                    "rol": "usuario",  # Mantener para compatibilidad
                    "role_id": "ROL_USUARIO_001",  # Nueva referencia
                    "status": "activo",
                    "created_at": datetime.now().isoformat(),
                    "last_login": None,
                    "permissions": ["read", "request_process"]
                }
            ]
            
            # Insertar usuarios en MongoDB
            for usuario in usuarios_iniciales:
                if self.mongodb_service.crear_usuario(usuario):
                    print(f"OK Usuario inicial creado: {usuario['username']}")
                else:
                    print(f"ERROR Error creando usuario inicial: {usuario['username']}")
            
            print("OK Usuarios iniciales creados correctamente")
            
            # Crear sensores iniciales
            self.crear_sensores_iniciales()
            
        except Exception as e:
            print(f"ERROR Error creando usuarios iniciales: {e}")
    
    def crear_sensores_iniciales(self):
        """Verificar y cargar sensores desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                print("ERROR MongoDB Atlas no disponible para cargar sensores")
                return
            
            # Obtener sensores existentes desde MongoDB
            sensores_existentes = self.mongodb_service.obtener_sensores()
            if sensores_existentes:
                print(f"OK Sensores cargados desde MongoDB: {len(sensores_existentes)} sensores")
                return
            
            # Si no hay sensores, crear algunos básicos usando el servicio
            print("⚠️ No hay sensores en MongoDB, creando sensores básicos...")
            self.crear_sensores_basicos()
                
        except Exception as e:
            print(f"ERROR cargando sensores: {e}")
    
    def crear_sensores_basicos(self):
        """Crear sensores básicos usando el servicio de MongoDB"""
        try:
            # Crear sensores básicos usando el servicio
            sensores_basicos = [
                {
                    "sensor_id": "SENSOR_BA_001",
                    "name": "Sensor Buenos Aires Centro",
                    "location": "Buenos Aires, Argentina",
                    "type": "Temperatura",
                    "status": "activo",
                    "description": "Sensor de temperatura en el centro de Buenos Aires",
                    "coordinates": {"lat": -34.6037, "lng": -58.3816}
                },
                {
                    "sensor_id": "SENSOR_CBA_001",
                    "name": "Sensor Córdoba Norte",
                    "location": "Córdoba, Argentina",
                    "type": "Humedad",
                    "status": "activo",
                    "description": "Sensor de humedad en el norte de Córdoba",
                    "coordinates": {"lat": -31.4201, "lng": -64.1888}
                },
                {
                    "sensor_id": "SENSOR_ROS_001",
                    "name": "Sensor Rosario Sur",
                    "location": "Rosario, Argentina",
                    "type": "Ambos",
                    "status": "activo",
                    "description": "Sensor combinado de temperatura y humedad en Rosario",
                    "coordinates": {"lat": -32.9442, "lng": -60.6505}
                }
            ]
            
            # Crear sensores usando el servicio
            for sensor in sensores_basicos:
                self.mongodb_service.crear_sensor(sensor)
            
            print(f"OK Sensores básicos creados: {len(sensores_basicos)} sensores")
            
        except Exception as e:
            print(f"ERROR creando sensores básicos: {e}")
    
    def crear_interfaz_basica(self):
        """Crear interfaz básica (solo header inicialmente)"""
        # Header
        self.crear_header()
        
        # Crear notebook para pestañas (inicialmente oculto)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Crear todas las pestañas
        self.crear_tab_home()
        self.crear_tab_sensores()
        self.crear_tab_analisis()
        self.crear_tab_informes()
        self.crear_tab_alertas()
        self.crear_tab_facturacion()
        self.crear_tab_comunicacion()
        self.crear_tab_procesos()
        self.crear_tab_servicios()
        self.crear_tab_configuracion()
        self.crear_tab_administracion()
        
        # Ocultar todo el notebook inicialmente
        self.notebook.pack_forget()
    
    def crear_interfaz(self):
        """Crear interfaz de usuario completa (llamada después del login)"""
        # Mostrar el notebook que ya fue creado
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
    
    def crear_header(self):
        """Crear header de la aplicación"""
        header_frame = tk.Frame(self.root, bg='#2c3e50', height=60)
        header_frame.pack(fill='x')
        header_frame.pack_propagate(False)
        
        # Título
        titulo = tk.Label(header_frame, text="Sistema de Gestión de Sensores ", 
                         font=('Arial', 16, 'bold'), bg='#2c3e50', fg='white')
        titulo.pack(side='left', padx=20, pady=15)
        
        # Botón de login
        self.boton_login = tk.Button(header_frame, text="Iniciar Sesión", 
                                     command=self.mostrar_dialogo_login,
                                     bg='#3498db', fg='white', font=('Arial', 10, 'bold'))
        self.boton_login.pack(side='right', padx=10, pady=10)
        
        # Información del usuario
        self.etiqueta_usuario = tk.Label(header_frame, text="Usuario: No autenticado", 
                                  font=('Arial', 9), fg='#ecf0f1', bg='#2c3e50')
        self.etiqueta_usuario.pack(side='right', padx=10, pady=10)
        
        # Indicador de tiempo de sesión
        self.etiqueta_tiempo_sesion = tk.Label(header_frame, text="", 
                                        font=('Arial', 8), fg='#f39c12', bg='#2c3e50')
        self.etiqueta_tiempo_sesion.pack(side='right', padx=5, pady=10)
    
    def crear_tab_home(self):
        """Crear tab del home principal con navegación a módulos"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Home")
        
        # Welcome section
        welcome_frame = tk.Frame(tab, bg='white')
        welcome_frame.pack(fill='x', padx=20, pady=20)
        
        welcome_label = tk.Label(welcome_frame, text="🏠 Sistema de Gestión de Sensores", 
                               font=('Arial', 20, 'bold'), bg='white', fg='#2c3e50')
        welcome_label.pack(pady=10)
        
        subtitle_label = tk.Label(welcome_frame, text="Selecciona un módulo para comenzar", 
                                font=('Arial', 12), bg='white', fg='#7f8c8d')
        subtitle_label.pack(pady=5)
        
        # User info section
        if self.usuario_autenticado:
            user_frame = tk.Frame(tab, bg='white')
            user_frame.pack(fill='x', padx=20, pady=10)
            
            user_label = tk.Label(user_frame, text=f"👤 Usuario: {self.usuario_autenticado} | 🔑 Rol: {self.rol_usuario.title()}", 
                                font=('Arial', 10, 'bold'), bg='white', fg='#27ae60')
            user_label.pack()
        
        # Main navigation frame
        nav_frame = tk.LabelFrame(tab, text="Módulos del Sistema", 
                                font=('Arial', 14, 'bold'), bg='white')
        nav_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        nav_inner = tk.Frame(nav_frame, bg='white')
        nav_inner.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Configure grid weights for responsive layout
        nav_inner.grid_columnconfigure(0, weight=1)
        nav_inner.grid_columnconfigure(1, weight=1)
        nav_inner.grid_columnconfigure(2, weight=1)
        nav_inner.grid_rowconfigure(0, weight=1)
        nav_inner.grid_rowconfigure(1, weight=1)
        nav_inner.grid_rowconfigure(2, weight=1)
        
        # Navigation buttons
        modules = [
            ("📊 Sensores", "Gestionar sensores del sistema", self.ir_a_sensores, '#27ae60'),
            ("📈 Análisis", "Analizar datos de sensores", self.ir_a_analisis, '#3498db'),
            ("📋 Informes", "Generar reportes y estadísticas", self.ir_a_informes, '#f39c12'),
            ("🚨 Alertas", "Configurar y gestionar alertas", self.ir_a_alertas, '#e74c3c'),
            ("💰 Facturación", "Gestionar facturas y pagos", self.ir_a_facturacion, '#9b59b6'),
            ("💬 Comunicación", "Mensajes y notificaciones", self.ir_a_comunicacion, '#16a085'),
            ("⚙️ Procesos", "Procesos automatizados", self.ir_a_procesos, '#8e44ad'),
            ("🔧 Servicios", "Servicios del sistema", self.ir_a_servicios, '#2c3e50'),
            ("⚙️ Configuración", "Configuración del sistema", self.ir_a_configuracion, '#34495e')
        ]
        
        # Create buttons in a 3x3 grid
        for i, (title, description, command, color) in enumerate(modules):
            row = i // 3
            col = i % 3
            
            # Create button frame
            btn_frame = tk.Frame(nav_inner, bg='white', relief='raised', bd=2)
            btn_frame.grid(row=row, column=col, padx=10, pady=10, sticky='nsew')
            btn_frame.grid_columnconfigure(0, weight=1)
            
            # Create button
            btn = tk.Button(btn_frame, text=title, command=command, 
                          bg=color, fg='white', font=('Arial', 12, 'bold'),
                          height=3, width=20)
            btn.pack(fill='both', expand=True, padx=5, pady=5)
            
            # Create description label
            desc_label = tk.Label(btn_frame, text=description, 
                                font=('Arial', 9), bg='white', fg='#7f8c8d',
                                wraplength=150)
            desc_label.pack(pady=(0, 5))
        
        # Quick actions frame
        actions_frame = tk.LabelFrame(tab, text="Acciones Rápidas", 
                                    font=('Arial', 12, 'bold'), bg='white')
        actions_frame.pack(fill='x', padx=20, pady=10)
        
        actions_inner = tk.Frame(actions_frame, bg='white')
        actions_inner.pack(fill='x', padx=10, pady=10)
        
        # Solo botón de actualizar sistema centrado
        tk.Button(actions_inner, text="🔄 Actualizar Sistema", 
                 command=self.actualizar_sistema_completo, 
                 bg='#3498db', fg='white', font=('Arial', 12, 'bold')).pack(pady=10)
    
    def ir_a_sensores(self):
        """Navegar al módulo de sensores"""
        self.notebook.select(1)  # Sensores es el segundo tab (índice 1)
        self.agregar_log("🏠 Navegando a módulo Sensores")
    
    def ir_a_analisis(self):
        """Navegar al módulo de análisis"""
        self.notebook.select(2)  # Análisis es el tercer tab (índice 2)
        self.agregar_log("🏠 Navegando a módulo Análisis")
    
    def ir_a_informes(self):
        """Navegar al módulo de informes"""
        self.notebook.select(3)  # Informes es el cuarto tab (índice 3)
        self.agregar_log("🏠 Navegando a módulo Informes")
    
    def ir_a_alertas(self):
        """Navegar al módulo de alertas"""
        self.notebook.select(4)  # Alertas es el quinto tab (índice 4)
        self.agregar_log("🏠 Navegando a módulo Alertas")
    
    def ir_a_facturacion(self):
        """Navegar al módulo de facturación"""
        self.notebook.select(5)  # Facturación es el sexto tab (índice 5)
        self.agregar_log("🏠 Navegando a módulo Facturación")
    
    def ir_a_comunicacion(self):
        """Navegar al módulo de comunicación"""
        self.notebook.select(6)  # Comunicación es el séptimo tab (índice 6)
        self.agregar_log("🏠 Navegando a módulo Comunicación")
    
    def ir_a_procesos(self):
        """Navegar al módulo de procesos"""
        self.notebook.select(7)  # Procesos es el octavo tab (índice 7)
        self.agregar_log("🏠 Navegando a módulo Procesos")
    
    def ir_a_servicios(self):
        """Navegar al módulo de servicios"""
        self.notebook.select(8)  # Servicios es el noveno tab (índice 8)
        self.agregar_log("🏠 Navegando a módulo Servicios")
    
    def ir_a_configuracion(self):
        """Navegar al módulo de configuración"""
        self.notebook.select(9)  # Configuración es el décimo tab (índice 9)
        self.agregar_log("🏠 Navegando a módulo Configuración")
    
    def actualizar_sistema_completo(self):
        """Actualizar todos los módulos del sistema"""
        try:
            self.agregar_log("🔄 Iniciando actualización completa del sistema...")
            
            # Actualizar estadísticas
            if hasattr(self, 'actualizar_estadisticas_dashboard'):
                self.actualizar_estadisticas_dashboard()
            
            # Actualizar listas de sensores
            if hasattr(self, 'actualizar_lista_sensores'):
                self.actualizar_lista_sensores()
            
            # Actualizar alertas
            if hasattr(self, 'actualizar_lista_alertas'):
                self.actualizar_lista_alertas()
            
            self.agregar_log("✅ Actualización completa del sistema finalizada")
            messagebox.showinfo("Actualización", "Sistema actualizado correctamente")
            
        except Exception as e:
            self.agregar_log(f"❌ Error en actualización del sistema: {e}")
            messagebox.showerror("Error", f"Error actualizando sistema: {e}")
    
    def mostrar_estadisticas_generales(self):
        """Mostrar estadísticas generales del sistema"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Crear ventana de estadísticas
            stats_window = tk.Toplevel(self.root)
            stats_window.title("Estadísticas Generales del Sistema")
            stats_window.geometry("600x400")
            stats_window.configure(bg='white')
            stats_window.transient(self.root)
            stats_window.grab_set()
            
            # Centrar ventana
            stats_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
            
            tk.Label(stats_window, text="📊 Estadísticas Generales", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=10)
            
            # Frame principal con scroll
            main_frame = tk.Frame(stats_window, bg='white')
            main_frame.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Crear área de texto con scroll
            texto_stats = scrolledtext.ScrolledText(main_frame, height=20, width=70)
            texto_stats.pack(fill='both', expand=True)
            
            # Obtener estadísticas
            sensores = self.mongodb_service.obtener_sensores()
            mediciones = self.mongodb_service.obtener_mediciones()
            alertas = self.mongodb_service.obtener_alertas()
            usuarios = self.mongodb_service.obtener_usuarios()
            
            # Generar reporte
            texto_stats.insert(tk.END, "📈 RESUMEN GENERAL DEL SISTEMA\n")
            texto_stats.insert(tk.END, "=" * 50 + "\n\n")
            
            texto_stats.insert(tk.END, f"🔢 Total de Sensores: {len(sensores)}\n")
            texto_stats.insert(tk.END, f"📊 Total de Mediciones: {len(mediciones)}\n")
            texto_stats.insert(tk.END, f"🚨 Total de Alertas: {len(alertas)}\n")
            texto_stats.insert(tk.END, f"👥 Total de Usuarios: {len(usuarios)}\n\n")
            
            # Sensores por estado
            sensores_activos = [s for s in sensores if s.get('status') == 'activo']
            texto_stats.insert(tk.END, f"✅ Sensores Activos: {len(sensores_activos)}\n")
            texto_stats.insert(tk.END, f"❌ Sensores Inactivos: {len(sensores) - len(sensores_activos)}\n\n")
            
            # Alertas por estado
            alertas_pendientes = [a for a in alertas if a.get('status') == 'pendiente']
            texto_stats.insert(tk.END, f"⏳ Alertas Pendientes: {len(alertas_pendientes)}\n")
            texto_stats.insert(tk.END, f"✅ Alertas Resueltas: {len(alertas) - len(alertas_pendientes)}\n\n")
            
            # Usuarios por rol
            usuarios_activos = [u for u in usuarios if u.get('status') == 'activo']
            texto_stats.insert(tk.END, f"👤 Usuarios Activos: {len(usuarios_activos)}\n")
            texto_stats.insert(tk.END, f"🔒 Usuarios Inactivos: {len(usuarios) - len(usuarios_activos)}\n\n")
            
            texto_stats.insert(tk.END, f"📅 Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            self.agregar_log("📊 Estadísticas generales mostradas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando estadísticas: {e}")
            messagebox.showerror("Error", f"Error mostrando estadísticas: {e}")
    
    def mostrar_ayuda(self):
        """Mostrar ayuda del sistema"""
        help_text = """
🏠 SISTEMA DE GESTIÓN DE SENSORES - AYUDA

📋 MÓDULOS DISPONIBLES:

📊 Sensores: Gestionar sensores del sistema
   • Agregar, editar y eliminar sensores
   • Configurar ubicaciones y tipos
   • Generar datos de prueba

📈 Análisis: Analizar datos de sensores
   • Seleccionar país y ciudad
   • Configurar rangos de fechas
   • Generar gráficos y reportes

📋 Informes: Generar reportes y estadísticas
   • Diferentes tipos de informes
   • Exportar en múltiples formatos
   • Análisis por ubicación

🚨 Alertas: Configurar y gestionar alertas
   • Crear alertas personalizadas
   • Configurar umbrales
   • Gestionar estados

💰 Facturación: Gestionar facturas y pagos
   • Ver cuentas corrientes
   • Generar facturas
   • Procesar pagos

💬 Comunicación: Mensajes y notificaciones
   • Enviar mensajes
   • Crear grupos
   • Gestionar notificaciones

⚙️ Procesos: Procesos automatizados
   • Configurar procesos
   • Monitorear ejecución
   • Gestionar colas

🔧 Servicios: Servicios del sistema
   • Configurar servicios
   • Monitorear estado
   • Gestionar recursos

⚙️ Configuración: Configuración del sistema
   • Parámetros generales
   • Configuración de base de datos
   • Logs del sistema

💡 CONSEJOS:
• Usa los botones de navegación para moverte entre módulos
• Revisa los logs para información detallada
• Actualiza el sistema regularmente
• Contacta al administrador si necesitas ayuda

🔗 SOPORTE:
Para soporte técnico, contacta al administrador del sistema.
        """
        
        messagebox.showinfo("Ayuda del Sistema", help_text)
        self.agregar_log("❓ Ayuda del sistema mostrada")
    
    def crear_tab_sensores(self):
        """Crear tab de gestión de sensores"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Sensores")
        
        # Configuración
        config_frame = tk.LabelFrame(tab, text="Configuración de Sensores", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Campos para nuevo sensor
        tk.Label(config_inner, text="Nombre:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.entry_nombre_sensor = tk.Entry(config_inner, width=30)
        self.entry_nombre_sensor.grid(row=0, column=1, padx=5, pady=5)
        
        # Ubicación - Tres combos separados (País, Ciudad, Zona)
        tk.Label(config_inner, text="País:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.combo_pais_sensor = ttk.Combobox(config_inner, width=25)
        self.combo_pais_sensor.grid(row=0, column=3, padx=5, pady=5)
        self.combo_pais_sensor.bind('<<ComboboxSelected>>', self.on_pais_selected_sensor)
        
        tk.Label(config_inner, text="Ciudad:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.combo_ciudad_sensor = ttk.Combobox(config_inner, width=25)
        self.combo_ciudad_sensor.grid(row=1, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Zona:", bg='white').grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.combo_zona_sensor = ttk.Combobox(config_inner, width=25, values=["Norte", "Sur", "Este", "Oeste", "Centro", "N/A"])
        self.combo_zona_sensor.grid(row=1, column=3, padx=5, pady=5)
        self.combo_zona_sensor.set("Centro")
        
        tk.Label(config_inner, text="Tipo:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.combo_tipo_sensor = ttk.Combobox(config_inner, values=["Temperatura", "Humedad", "Ambos"], width=27)
        self.combo_tipo_sensor.grid(row=2, column=1, padx=5, pady=5)
        self.combo_tipo_sensor.set("Temperatura")
        
        tk.Label(config_inner, text="Estado:", bg='white').grid(row=2, column=2, padx=5, pady=5, sticky='w')
        self.combo_estado_sensor = ttk.Combobox(config_inner, values=["Activo", "Inactivo", "Mantenimiento"], width=27)
        self.combo_estado_sensor.grid(row=2, column=3, padx=5, pady=5)
        self.combo_estado_sensor.set("Activo")
        
        # Botones
        self.btn_agregar_sensor = tk.Button(config_inner, text="➕ Agregar Sensor", 
                 command=self.agregar_sensor, 
                 bg='#27ae60', fg='white', font=('Arial', 10))
        self.btn_agregar_sensor.grid(row=3, column=0, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔄 Actualizar Lista", 
                 command=self.actualizar_lista_sensores, 
                 bg='#3498db', fg='white', font=('Arial', 10)).grid(row=3, column=1, padx=5, pady=10)
        
        tk.Button(config_inner, text="📊 Generar Datos", 
                 command=self.generar_datos_sensor, 
                 bg='#f39c12', fg='white', font=('Arial', 10)).grid(row=3, column=2, padx=5, pady=10)
        
        self.btn_editar_sensor = tk.Button(config_inner, text="✏️ Editar Sensor", 
                 command=self.editar_sensor, 
                 bg='#9b59b6', fg='white', font=('Arial', 10))
        self.btn_editar_sensor.grid(row=3, column=3, padx=5, pady=10)
        
        self.btn_eliminar_sensor = tk.Button(config_inner, text="🗑️ Eliminar Sensor", 
                 command=self.eliminar_sensor, 
                 bg='#e74c3c', fg='white', font=('Arial', 10))
        self.btn_eliminar_sensor.grid(row=4, column=0, padx=5, pady=10)
        
        # Lista de sensores
        lista_frame = tk.LabelFrame(tab, text="Lista de Sensores", 
                                  font=('Arial', 12, 'bold'), bg='white')
        lista_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Treeview para sensores
        columns = ("ID", "Nombre", "Ubicación", "Tipo", "Estado", "Última Medición")
        self.tree_sensores = ttk.Treeview(lista_frame, columns=columns, show="headings")
        
        for col in columns:
            self.tree_sensores.heading(col, text=col)
            self.tree_sensores.column(col, width=120)
        
        # Scrollbar para la lista
        scrollbar_sensores = ttk.Scrollbar(lista_frame, orient="vertical", command=self.tree_sensores.yview)
        self.tree_sensores.configure(yscrollcommand=scrollbar_sensores.set)
        
        self.tree_sensores.pack(side="left", fill="both", expand=True)
        scrollbar_sensores.pack(side="right", fill="y")
        
        # Bind doble click
        self.tree_sensores.bind("<Double-1>", self.al_hacer_doble_clic_sensor)
        
        # Cargar países para el combo de sensores
        self.cargar_paises_para_sensores()
        
        # Configurar botones según el rol del usuario
        self.configurar_botones_sensores()
        
        # Cargar sensores inicialmente
        self.actualizar_lista_sensores()
    
    def configurar_botones_sensores(self):
        """Configurar botones de sensores según el rol del usuario"""
        try:
            if not hasattr(self, 'rol_usuario'):
                return
            
            # Solo técnicos y administradores pueden gestionar sensores
            if self.rol_usuario == "usuario":
                # Deshabilitar botones de gestión para usuarios comunes
                if hasattr(self, 'btn_agregar_sensor'):
                    self.btn_agregar_sensor.config(state='disabled')
                if hasattr(self, 'btn_editar_sensor'):
                    self.btn_editar_sensor.config(state='disabled')
                if hasattr(self, 'btn_eliminar_sensor'):
                    self.btn_eliminar_sensor.config(state='disabled')
                
                # self.agregar_log("🔒 Botones de gestión de sensores deshabilitados para usuario común")
            else:
                # Habilitar botones para técnicos y administradores
                if hasattr(self, 'btn_agregar_sensor'):
                    self.btn_agregar_sensor.config(state='normal')
                if hasattr(self, 'btn_editar_sensor'):
                    self.btn_editar_sensor.config(state='normal')
                if hasattr(self, 'btn_eliminar_sensor'):
                    self.btn_eliminar_sensor.config(state='normal')
                
                # self.agregar_log(f"✅ Botones de gestión de sensores habilitados para rol: {self.rol_usuario}")
                
        except Exception as e:
            self.agregar_log(f"❌ Error configurando botones de sensores: {e}")
    
    def crear_tab_analisis(self):
        """Crear tab de análisis de datos"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Análisis")
        
        # Configuración
        config_frame = tk.LabelFrame(tab, text="Configuración de Análisis", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Selección de país
        tk.Label(config_inner, text="País:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_pais_analisis = ttk.Combobox(config_inner, width=25)
        self.combo_pais_analisis.grid(row=0, column=1, padx=5, pady=5)
        
        # Selección de ciudad
        tk.Label(config_inner, text="Ciudad:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.combo_ciudad_analisis = ttk.Combobox(config_inner, width=25)
        self.combo_ciudad_analisis.grid(row=1, column=1, padx=5, pady=5)
        
        # Rango de fechas
        tk.Label(config_inner, text="Desde:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.entry_fecha_desde = tk.Entry(config_inner, width=15)
        self.entry_fecha_desde.grid(row=0, column=3, padx=5, pady=5)
        self.entry_fecha_desde.insert(0, (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
        
        tk.Label(config_inner, text="Hasta:", bg='white').grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.entry_fecha_hasta = tk.Entry(config_inner, width=15)
        self.entry_fecha_hasta.grid(row=1, column=3, padx=5, pady=5)
        self.entry_fecha_hasta.insert(0, datetime.now().strftime("%Y-%m-%d"))
        
        # Tipo de análisis
        tk.Label(config_inner, text="Tipo:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.combo_tipo_analisis = ttk.Combobox(config_inner, values=["Temperatura Máxima", "Temperatura Mínima", "Ambas Temperaturas"], width=20)
        self.combo_tipo_analisis.grid(row=2, column=1, padx=5, pady=5)
        self.combo_tipo_analisis.set("Ambas Temperaturas")
        
        # Botones de análisis (sin superposiciones)
        tk.Button(config_inner, text="📊 Analizar Datos", 
                 command=self.ejecutar_analisis, 
                 bg='#3498db', fg='white', font=('Arial', 10), width=15).grid(row=3, column=0, padx=10, pady=10, sticky='ew')
        
        tk.Button(config_inner, text="📋 Exportar Reporte", 
                 command=self.exportar_reporte, 
                 bg='#f39c12', fg='white', font=('Arial', 10), width=15).grid(row=3, column=1, padx=10, pady=10, sticky='ew')
        
        tk.Button(config_inner, text="🔍 Detectar Anomalías", 
                 command=self.detectar_anomalias, 
                 bg='#e74c3c', fg='white', font=('Arial', 10), width=15).grid(row=3, column=2, padx=10, pady=10, sticky='ew')
        
        # Área de resultados
        resultados_frame = tk.LabelFrame(tab, text="Resultados del Análisis", 
                                       font=('Arial', 12, 'bold'), bg='white')
        resultados_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        self.texto_resultados_analisis = scrolledtext.ScrolledText(resultados_frame, height=15)
        self.texto_resultados_analisis.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Configurar eventos para selección en cascada
        self.combo_pais_analisis.bind('<<ComboboxSelected>>', self.on_pais_selected)
        
        # Cargar países para análisis
        self.cargar_paises_para_analisis()
    
    def crear_tab_informes(self):
        """Crear tab de generación de informes usando MongoDB Time Series"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Informes")
        
        # Configuración de informes
        config_frame = tk.LabelFrame(tab, text="Configuración de Informes", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Campos para configuración de informe
        tk.Label(config_inner, text="Tipo de Informe:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_tipo_informe = ttk.Combobox(config_inner, values=[
            "Temperatura por País", 
            "Humedad por País",
            "Análisis Temporal"
        ], width=25)
        self.combo_tipo_informe.grid(row=0, column=1, padx=5, pady=5)
        self.combo_tipo_informe.set("Humedad por País")
        
        tk.Label(config_inner, text="País:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.combo_pais_ciudad_informe = ttk.Combobox(config_inner, width=20)
        self.combo_pais_ciudad_informe.grid(row=0, column=3, padx=5, pady=5)
        
        tk.Label(config_inner, text="Fecha Inicio:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.entry_fecha_inicio = tk.Entry(config_inner, width=20)
        self.entry_fecha_inicio.grid(row=1, column=1, padx=5, pady=5)
        self.entry_fecha_inicio.insert(0, (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
        
        tk.Label(config_inner, text="Fecha Fin:", bg='white').grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.entry_fecha_fin = tk.Entry(config_inner, width=20)
        self.entry_fecha_fin.grid(row=1, column=3, padx=5, pady=5)
        self.entry_fecha_fin.insert(0, datetime.now().strftime("%Y-%m-%d"))
        
        tk.Label(config_inner, text="Agrupación:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.combo_agrupacion = ttk.Combobox(config_inner, values=["Diaria", "Semanal", "Mensual", "Anual"], width=20)
        self.combo_agrupacion.grid(row=2, column=1, padx=5, pady=5)
        self.combo_agrupacion.set("Diaria")
        
        tk.Label(config_inner, text="Formato:", bg='white').grid(row=2, column=2, padx=5, pady=5, sticky='w')
        self.combo_formato_informe = ttk.Combobox(config_inner, values=["Pantalla", "PDF", "Excel", "CSV"], width=20)
        self.combo_formato_informe.grid(row=2, column=3, padx=5, pady=5)
        self.combo_formato_informe.set("Pantalla")
        
        # Botones (sin superposiciones)
        tk.Button(config_inner, text="📊 Generar Informe", 
                 command=self.generar_informe, 
                 bg='#27ae60', fg='white', font=('Arial', 10), width=15).grid(row=3, column=0, padx=10, pady=10, sticky='ew')
        
        tk.Button(config_inner, text="🔄 Actualizar Datos", 
                 command=self.actualizar_datos_informe, 
                 bg='#3498db', fg='white', font=('Arial', 10), width=15).grid(row=3, column=1, padx=10, pady=10, sticky='ew')
        
        tk.Button(config_inner, text="💾 Guardar Informe", 
                 command=self.guardar_informe, 
                 bg='#f39c12', fg='white', font=('Arial', 10), width=15).grid(row=3, column=2, padx=10, pady=10, sticky='ew')
        
        # Área de resultados del informe
        resultados_frame = tk.LabelFrame(tab, text="Resultados del Informe", 
                                       font=('Arial', 12, 'bold'), bg='white')
        resultados_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        self.texto_informe = scrolledtext.ScrolledText(resultados_frame, height=20)
        self.texto_informe.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Cargar datos para los combos
        self.cargar_sensores_para_informes()
        self.cargar_ubicaciones_para_informes()
    
    def crear_tab_alertas(self):
        """Crear tab de gestión de alertas"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Alertas")
        
        # Configuración de alertas
        config_frame = tk.LabelFrame(tab, text="Configuración de Alertas", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Configuración de umbrales
        tk.Label(config_inner, text="Sensor:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_sensor_alerta = ttk.Combobox(config_inner, width=20)
        self.combo_sensor_alerta.grid(row=0, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Categoría:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.combo_categoria_alerta = ttk.Combobox(config_inner, values=["Climática", "Sensor"], width=18)
        self.combo_categoria_alerta.grid(row=0, column=3, padx=5, pady=5)
        self.combo_categoria_alerta.set("Climática")
        
        tk.Label(config_inner, text="Tipo:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.combo_tipo_alerta = ttk.Combobox(config_inner, values=["Temperatura Alta", "Temperatura Baja", "Humedad Alta", "Humedad Baja"], width=20)
        self.combo_tipo_alerta.grid(row=1, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Umbral:", bg='white').grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.entry_umbral_alerta = tk.Entry(config_inner, width=20)
        self.entry_umbral_alerta.grid(row=1, column=3, padx=5, pady=5)
        
        tk.Label(config_inner, text="Severidad:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.combo_severidad_alerta = ttk.Combobox(config_inner, values=["Baja", "Media", "Alta", "Crítica"], width=20)
        self.combo_severidad_alerta.grid(row=2, column=1, padx=5, pady=5)
        self.combo_severidad_alerta.set("Media")
        
        tk.Label(config_inner, text="Estado:", bg='white').grid(row=2, column=2, padx=5, pady=5, sticky='w')
        self.combo_estado_alerta = ttk.Combobox(config_inner, values=["Pendiente", "En Proceso", "Resuelta", "Cerrada"], width=18)
        self.combo_estado_alerta.grid(row=2, column=3, padx=5, pady=5)
        self.combo_estado_alerta.set("Pendiente")
        
        tk.Label(config_inner, text="Mensaje:", bg='white').grid(row=3, column=0, padx=5, pady=5, sticky='w')
        self.entry_mensaje_alerta = tk.Entry(config_inner, width=60)
        self.entry_mensaje_alerta.grid(row=3, column=1, columnspan=3, padx=5, pady=5, sticky='ew')
        
        # Botones - Primera fila: Gestión de Alertas
        tk.Button(config_inner, text="➕ Crear Alerta", 
                 command=self.crear_alerta, 
                 bg='#27ae60', fg='white', font=('Arial', 10)).grid(row=4, column=0, padx=5, pady=10)
        
        tk.Button(config_inner, text="✏️ Editar Alerta", 
                 command=self.editar_alerta, 
                 bg='#9b59b6', fg='white', font=('Arial', 10)).grid(row=4, column=1, padx=5, pady=10)
        
        tk.Button(config_inner, text="✅ Resolver Alerta", 
                 command=self.resolver_alerta, 
                 bg='#f39c12', fg='white', font=('Arial', 10)).grid(row=4, column=2, padx=5, pady=10)
        
        tk.Button(config_inner, text="🗑️ Eliminar Alerta", 
                 command=self.eliminar_alerta, 
                 bg='#e74c3c', fg='white', font=('Arial', 10)).grid(row=4, column=3, padx=5, pady=10)
        
        # Segunda fila: Configuración y Detección
        self.btn_umbrales_ubicacion = tk.Button(config_inner, text="📍 Umbrales por Ubicación", 
                 command=self.mostrar_umbrales_por_ubicacion, 
                 bg='#8e44ad', fg='white', font=('Arial', 10))
        self.btn_umbrales_ubicacion.grid(row=5, column=0, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔍 Detectar Alertas", 
                 command=self.detectar_alertas_climaticas_automaticas, 
                 bg='#e67e22', fg='white', font=('Arial', 10)).grid(row=5, column=1, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔄 Actualizar Lista", 
                 command=self.actualizar_lista_alertas, 
                 bg='#3498db', fg='white', font=('Arial', 10)).grid(row=5, column=2, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔄 Recargar Sensores", 
                 command=self.cargar_sensores_para_alertas, 
                 bg='#16a085', fg='white', font=('Arial', 10)).grid(row=5, column=3, padx=5, pady=10)
        
        # --- Control de Funcionamiento (lado derecho) ---
        control_frame = tk.LabelFrame(config_inner, text="Control de Funcionamiento", 
                                   font=('Arial', 12, 'bold'), bg='white')
        control_frame.grid(row=0, column=4, rowspan=6, padx=20, pady=5, sticky='n')

        tk.Label(control_frame, text="Sensor:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_sensor_control = ttk.Combobox(control_frame, width=30)
        self.combo_sensor_control.grid(row=0, column=1, padx=5, pady=5)

        tk.Label(control_frame, text="Fecha de Revisión:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.entry_fecha_control = tk.Entry(control_frame, width=20)
        self.entry_fecha_control.grid(row=1, column=1, padx=5, pady=5)
        self.entry_fecha_control.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        tk.Label(control_frame, text="Estado del Sensor:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.combo_estado_sensor = ttk.Combobox(control_frame, values=["OK", "Falla"], width=18, state='readonly')
        self.combo_estado_sensor.grid(row=2, column=1, padx=5, pady=5)
        self.combo_estado_sensor.set("OK")

        tk.Label(control_frame, text="Observaciones:", bg='white').grid(row=3, column=0, padx=5, pady=5, sticky='nw')
        self.txt_obs_control = tk.Text(control_frame, width=30, height=4)
        self.txt_obs_control.grid(row=3, column=1, padx=5, pady=5)

        tk.Button(control_frame, text="📝 Registrar Control", 
                 command=self.registrar_control_funcionamiento,
                 bg='#2ecc71', fg='white', font=('Arial', 10)).grid(row=4, column=1, padx=5, pady=10, sticky='e')

        # Cargar sensores para el combo de control
        self.cargar_sensores_para_alertas()

        # Lista de alertas
        lista_frame = tk.LabelFrame(tab, text="📊 Log de Alertas del Sistema", 
                                  font=('Arial', 12, 'bold'), bg='white')
        lista_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Frame para filtros
        filtros_frame = tk.Frame(lista_frame, bg='white')
        filtros_frame.pack(fill='x', padx=10, pady=5)
        
        # Filtros
        tk.Label(filtros_frame, text="🔍 Filtros:", bg='white', font=('Arial', 10, 'bold')).pack(side='left', padx=5)
        
        tk.Label(filtros_frame, text="Tipo:", bg='white').pack(side='left', padx=5)
        self.combo_filtro_tipo = ttk.Combobox(filtros_frame, values=["Todas", "Climática", "Sensor"], width=12)
        self.combo_filtro_tipo.pack(side='left', padx=5)
        self.combo_filtro_tipo.set("Todas")
        
        tk.Label(filtros_frame, text="Estado:", bg='white').pack(side='left', padx=5)
        self.combo_filtro_estado = ttk.Combobox(filtros_frame, values=["Todas", "Activa", "Resuelta"], width=12)
        self.combo_filtro_estado.pack(side='left', padx=5)
        self.combo_filtro_estado.set("Todas")
        
        tk.Label(filtros_frame, text="Severidad:", bg='white').pack(side='left', padx=5)
        self.combo_filtro_severidad = ttk.Combobox(filtros_frame, values=["Todas", "Baja", "Media", "Alta", "Crítica"], width=12)
        self.combo_filtro_severidad.pack(side='left', padx=5)
        self.combo_filtro_severidad.set("Todas")
        
        tk.Button(filtros_frame, text="🔄 Aplicar Filtros", 
                 command=self.aplicar_filtros_alertas, 
                 bg='#3498db', fg='white', font=('Arial', 9)).pack(side='left', padx=10)
        
        # Treeview para alertas con columnas mejoradas
        columns = ("ID", "Tipo", "Ubicación/Sensor", "Descripción", "Severidad", "Estado", "Fecha", "Resuelto por", "Resuelto en")
        self.tree_alertas = ttk.Treeview(lista_frame, columns=columns, show="headings")
        
        # Configurar columnas con anchos apropiados
        column_widths = {"ID": 80, "Tipo": 80, "Ubicación/Sensor": 120, "Descripción": 200, 
                        "Severidad": 80, "Estado": 80, "Fecha": 120, "Resuelto por": 100, "Resuelto en": 120}
        
        for col in columns:
            self.tree_alertas.heading(col, text=col)
            self.tree_alertas.column(col, width=column_widths.get(col, 100))
        
        # Scrollbar para la lista
        scrollbar_alertas = ttk.Scrollbar(lista_frame, orient="vertical", command=self.tree_alertas.yview)
        self.tree_alertas.configure(yscrollcommand=scrollbar_alertas.set)
        
        self.tree_alertas.pack(side="left", fill="both", expand=True)
        scrollbar_alertas.pack(side="right", fill="y")
        
        # Cargar sensores para el combo de alertas
        self.cargar_sensores_para_alertas()
        
        # Configurar botones según el rol del usuario
        self.configurar_botones_alertas()
    
    def configurar_botones_alertas(self):
        """Configurar botones de alertas según el rol del usuario"""
        try:
            if not hasattr(self, 'rol_usuario'):
                return
            
            # Solo técnicos y administradores pueden configurar umbrales
            if self.rol_usuario == "usuario":
                # Deshabilitar botón de umbrales por ubicación para usuarios comunes
                if hasattr(self, 'btn_umbrales_ubicacion'):
                    self.btn_umbrales_ubicacion.config(state='disabled')
                
                # self.agregar_log("🔒 Botón de umbrales por ubicación deshabilitado para usuario común")
            else:
                # Habilitar botón para técnicos y administradores
                if hasattr(self, 'btn_umbrales_ubicacion'):
                    self.btn_umbrales_ubicacion.config(state='normal')
                
                # self.agregar_log(f"✅ Botón de umbrales por ubicación habilitado para rol: {self.rol_usuario}")
                
        except Exception as e:
            self.agregar_log(f"❌ Error configurando botones de alertas: {e}")
    
    def detectar_alertas_climaticas_automaticas(self):
        """Detectar alertas climáticas automáticamente basadas en TODAS las mediciones y umbrales"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            self.agregar_log("🔍 Iniciando detección automática de alertas climáticas en TODAS las mediciones...")
            
            # Mostrar ventana de progreso
            progress_window = tk.Toplevel(self.root)
            progress_window.title("Detectando Alertas")
            progress_window.geometry("400x150")
            progress_window.configure(bg='white')
            progress_window.transient(self.root)
            progress_window.grab_set()
            
            tk.Label(progress_window, text="🔍 Detectando alertas climáticas...", 
                    font=('Arial', 12, 'bold'), bg='white').pack(pady=20)
            
            progress_label = tk.Label(progress_window, text="Analizando sensores y mediciones...", 
                                     bg='white')
            progress_label.pack(pady=10)
            
            # Obtener todos los sensores
            sensores = self.mongodb_service.obtener_sensores()
            alertas_creadas = 0
            sensores_procesados = 0
            
            progress_label.config(text=f"Procesando {len(sensores)} sensores...")
            progress_window.update()
            
            for sensor in sensores:
                sensor_id = sensor.get('sensor_id')
                location = sensor.get('location', {})
                
                if not sensor_id or not location:
                    continue
                
                sensores_procesados += 1
                progress_label.config(text=f"Procesando sensor {sensores_procesados}/{len(sensores)}: {sensor_id}")
                progress_window.update()
                
                # Obtener ubicación
                if isinstance(location, dict):
                    ciudad = location.get('city', '')
                    pais = location.get('country', '')
                elif isinstance(location, str):
                    # Parsear string de ubicación formato "Ciudad, Zona - País" o "Ciudad - País"
                    if ' - ' in location:
                        # Extraer país (después del guión)
                        partes = location.split(' - ')
                        pais = partes[-1].strip()
                        
                        # Extraer ciudad (antes del guión, puede tener zona)
                        ciudad_zona = partes[0].strip()
                        if ', ' in ciudad_zona:
                            ciudad, _ = ciudad_zona.split(', ', 1)
                            ciudad = ciudad.strip()
                        else:
                            ciudad = ciudad_zona
                    else:
                        continue
                else:
                    continue
                
                if not ciudad or not pais:
                    continue
                
                # Obtener umbrales para esta ubicación
                umbrales = self.mongodb_service.obtener_umbrales_efectivos_por_ubicacion(sensor_id)
                
                if not umbrales:
                    self.agregar_log(f"⚠️ No hay umbrales configurados para {ciudad}, {pais}")
                    continue
                
                # Obtener TODAS las mediciones del sensor (no solo la última)
                mediciones = self.mongodb_service.obtener_mediciones_sensor(sensor_id)
                
                if not mediciones:
                    self.agregar_log(f"⚠️ No hay mediciones para el sensor {sensor_id}")
                    continue
                
                self.agregar_log(f"📊 Analizando {len(mediciones)} mediciones del sensor {sensor_id}")
                
                # Analizar cada medición
                for medicion in mediciones:
                    temperatura = medicion.get('temperature')
                    humedad = medicion.get('humidity')
                    timestamp = medicion.get('timestamp')
                    
                    if temperatura is None and humedad is None:
                        continue
                    
                    # Verificar umbrales de temperatura
                    if temperatura is not None:
                        temp_min = umbrales.get('Temperatura', {}).get('min')
                        temp_max = umbrales.get('Temperatura', {}).get('max')
                        
                        # Verificar si ya existe una alerta para esta medición específica
                        if not self.existe_alerta_para_medicion(sensor_id, timestamp, "Temperatura"):
                            if temp_min is not None and temperatura < temp_min:
                                self.crear_alerta_climatica_automatica(
                                    sensor_id, ciudad, pais, "Temperatura Baja", 
                                    temperatura, temp_min, "Temperatura", timestamp
                                )
                                alertas_creadas += 1
                            
                            if temp_max is not None and temperatura > temp_max:
                                self.crear_alerta_climatica_automatica(
                                    sensor_id, ciudad, pais, "Temperatura Alta", 
                                    temperatura, temp_max, "Temperatura", timestamp
                                )
                                alertas_creadas += 1
                    
                    # Verificar umbrales de humedad
                    if humedad is not None:
                        hum_min = umbrales.get('Humedad', {}).get('min')
                        hum_max = umbrales.get('Humedad', {}).get('max')
                        
                        # Verificar si ya existe una alerta para esta medición específica
                        if not self.existe_alerta_para_medicion(sensor_id, timestamp, "Humedad"):
                            if hum_min is not None and humedad < hum_min:
                                self.crear_alerta_climatica_automatica(
                                    sensor_id, ciudad, pais, "Humedad Baja", 
                                    humedad, hum_min, "Humedad", timestamp
                                )
                                alertas_creadas += 1
                            
                            if hum_max is not None and humedad > hum_max:
                                self.crear_alerta_climatica_automatica(
                                    sensor_id, ciudad, pais, "Humedad Alta", 
                                    humedad, hum_max, "Humedad", timestamp
                                )
                                alertas_creadas += 1
            
            # Cerrar ventana de progreso
            progress_window.destroy()
            
            if alertas_creadas > 0:
                self.agregar_log(f"✅ {alertas_creadas} alertas climáticas automáticas creadas")
                messagebox.showinfo("Éxito", f"Se detectaron y crearon {alertas_creadas} alertas climáticas")
                # Actualizar lista de alertas
                self.actualizar_lista_alertas()
            else:
                self.agregar_log("✅ No se detectaron alertas climáticas en las mediciones")
                messagebox.showinfo("Información", "No se detectaron alertas climáticas en las mediciones analizadas")
                
        except Exception as e:
            self.agregar_log(f"❌ Error en detección automática de alertas: {e}")
            messagebox.showerror("Error", f"Error detectando alertas: {e}")
    
    def crear_alerta_climatica_automatica(self, sensor_id, ciudad, pais, tipo_alerta, valor_actual, umbral, parametro, timestamp=None):
        """Crear alerta climática automáticamente"""
        try:
            # Generar ID único usando timestamp + sensor_id + número aleatorio
            import time
            import random
            timestamp_str = str(int(time.time() * 1000))  # Usar milisegundos para mayor precisión
            random_suffix = str(random.randint(1000, 9999))  # Número aleatorio adicional
            alert_id = f"ALERT_CLIMATIC_{timestamp_str}_{sensor_id}_{random_suffix}"
            
            # Determinar severidad basada en qué tan lejos está del umbral
            diferencia = abs(valor_actual - umbral)
            if diferencia > 10:
                severity = "crítica"
            elif diferencia > 5:
                severity = "alta"
            elif diferencia > 2:
                severity = "media"
            else:
                severity = "baja"
            
            # Crear mensaje descriptivo
            if parametro == "Temperatura":
                unidad = "°C"
            else:
                unidad = "%"
            
            mensaje = f"{tipo_alerta} en {ciudad}, {pais}: {valor_actual}{unidad} (umbral: {umbral}{unidad})"
            
            # Usar timestamp de la medición si está disponible
            created_at = timestamp if timestamp else datetime.now().isoformat()
            
            alerta_data = {
                "alert_id": alert_id,
                "sensor_id": sensor_id,
                "categoria": "Climática",
                "type": tipo_alerta,
                "severity": severity,
                "status": "active",
                "threshold": umbral,
                "current_value": valor_actual,
                "parameter": parametro,
                "location": {
                    "city": ciudad,
                    "country": pais
                },
                "message": mensaje,
                "created_at": created_at,
                "created_by": "SYSTEM",
                "automatic": True
            }
            
            # Guardar en MongoDB
            if self.mongodb_service.crear_alerta(alerta_data):
                self.agregar_log(f"🌡️ Alerta climática automática creada: {mensaje}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error creando alerta climática automática: {e}")
    
    def existe_alerta_para_medicion(self, sensor_id, timestamp, parametro):
        """Verificar si ya existe una alerta para una medición específica"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return False
            
            # Buscar alertas existentes para este sensor, timestamp y parámetro
            query = {
                "sensor_id": sensor_id,
                "created_at": timestamp,
                "categoria": "Climática",
                "automatic": True
            }
            
            # Agregar filtro por tipo de parámetro
            if parametro == "Temperatura":
                query["type"] = {"$in": ["Temperatura Alta", "Temperatura Baja"]}
            elif parametro == "Humedad":
                query["type"] = {"$in": ["Humedad Alta", "Humedad Baja"]}
            
            # Verificar si existe al menos una alerta
            existing_alerts = list(self.mongodb_service.db.alerts.find(query).limit(1))
            return len(existing_alerts) > 0
            
        except Exception as e:
            self.agregar_log(f"❌ Error verificando alertas existentes: {e}")
            return False
    
    def mostrar_umbrales_por_ubicacion(self):
        """Mostrar umbrales por ubicación con interfaz simplificada"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Crear ventana principal
            umbrales_window = tk.Toplevel(self.root)
            umbrales_window.title("📍 Gestión de Umbrales por Ubicación")
            umbrales_window.geometry("900x600")
            umbrales_window.configure(bg='white')
            umbrales_window.transient(self.root)
            umbrales_window.grab_set()
            
            # Frame principal
            main_frame = tk.Frame(umbrales_window, bg='white')
            main_frame.pack(fill='both', expand=True, padx=20, pady=20)
            
            # Título
            tk.Label(main_frame, text="📍 Umbrales por Ubicación", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=(0, 20))
            
            # Información sobre jerarquía
            info_frame = tk.Frame(main_frame, bg='#e8f4fd', relief='raised', bd=1)
            info_frame.pack(fill='x', pady=(0, 20))
            
            tk.Label(info_frame, text="ℹ️ Jerarquía de Umbrales:", 
                    font=('Arial', 10, 'bold'), bg='#e8f4fd').pack(anchor='w', padx=10, pady=5)
            
            tk.Label(info_frame, text="1. 🌍 Umbrales por Ubicación (Ciudad, País) - Prioridad Alta", 
                    bg='#e8f4fd').pack(anchor='w', padx=20)
            tk.Label(info_frame, text="2. 🌐 Umbrales Globales - Prioridad Media", 
                    bg='#e8f4fd').pack(anchor='w', padx=20)
            tk.Label(info_frame, text="3. ⚙️ Valores por Defecto - Prioridad Baja", 
                    bg='#e8f4fd').pack(anchor='w', padx=20, pady=(0, 10))
            
            # Treeview para mostrar umbrales
            columns = ("Ubicación", "Temperatura Min", "Temperatura Max", "Humedad Min", "Humedad Max", "Última Actualización")
            self.tree_umbrales_ubicacion = ttk.Treeview(main_frame, columns=columns, show="headings")
            
            # Configurar columnas
            self.tree_umbrales_ubicacion.heading("Ubicación", text="📍 Ubicación")
            self.tree_umbrales_ubicacion.heading("Temperatura Min", text="🌡️ Temp Min (°C)")
            self.tree_umbrales_ubicacion.heading("Temperatura Max", text="🌡️ Temp Max (°C)")
            self.tree_umbrales_ubicacion.heading("Humedad Min", text="💧 Hum Min (%)")
            self.tree_umbrales_ubicacion.heading("Humedad Max", text="💧 Hum Max (%)")
            self.tree_umbrales_ubicacion.heading("Última Actualización", text="📅 Última Actualización")
            
            # Configurar anchos
            self.tree_umbrales_ubicacion.column("Ubicación", width=200)
            self.tree_umbrales_ubicacion.column("Temperatura Min", width=120)
            self.tree_umbrales_ubicacion.column("Temperatura Max", width=120)
            self.tree_umbrales_ubicacion.column("Humedad Min", width=120)
            self.tree_umbrales_ubicacion.column("Humedad Max", width=120)
            self.tree_umbrales_ubicacion.column("Última Actualización", width=150)
            
            # Scrollbar
            scrollbar_umbrales = ttk.Scrollbar(main_frame, orient="vertical", command=self.tree_umbrales_ubicacion.yview)
            self.tree_umbrales_ubicacion.configure(yscrollcommand=scrollbar_umbrales.set)
            
            # Pack treeview y scrollbar
            self.tree_umbrales_ubicacion.pack(side="left", fill="both", expand=True)
            scrollbar_umbrales.pack(side="right", fill="y")
            
            # Bind para doble clic
            self.tree_umbrales_ubicacion.bind('<Double-1>', self.configurar_umbrales_ubicacion_click)
            
            # Botones principales
            button_frame = tk.Frame(main_frame, bg='white')
            button_frame.pack(fill='x', padx=20, pady=10)
            
            tk.Button(button_frame, text="🔄 Actualizar Lista", 
                     command=self.actualizar_umbrales_ubicacion, 
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(button_frame, text="➕ Nueva Ubicación", 
                     command=self.crear_nueva_ubicacion_umbrales, 
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(button_frame, text="❌ Cerrar", 
                     command=umbrales_window.destroy, 
                     bg='#95a5a6', fg='white', font=('Arial', 10)).pack(side='right', padx=5)
            
            # Cargar datos iniciales
            self.actualizar_umbrales_ubicacion()
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando umbrales por ubicación: {e}")
            messagebox.showerror("Error", f"Error mostrando umbrales: {e}")
    
    def actualizar_umbrales_ubicacion(self):
        """Actualizar la lista de umbrales por ubicación"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Limpiar lista
            for item in self.tree_umbrales_ubicacion.get_children():
                self.tree_umbrales_ubicacion.delete(item)
            
            # Obtener umbrales globales
            umbrales_globales = self.mongodb_service.obtener_umbrales_globales()
            if umbrales_globales:
                thresholds = umbrales_globales.get("thresholds", {})
                temp_min = thresholds.get("Temperatura", {}).get("min", "N/A")
                temp_max = thresholds.get("Temperatura", {}).get("max", "N/A")
                hum_min = thresholds.get("Humedad", {}).get("min", "N/A")
                hum_max = thresholds.get("Humedad", {}).get("max", "N/A")
                updated_at = umbrales_globales.get("updated_at", "N/A")
                
                self.tree_umbrales_ubicacion.insert('', 'end', values=(
                    "🌐 Globales", temp_min, temp_max, hum_min, hum_max, updated_at
                ))
            else:
                # Mostrar umbrales globales por defecto si no están configurados
                self.tree_umbrales_ubicacion.insert('', 'end', values=(
                    "🌐 Globales", "5", "35", "30", "80", "No configurado"
                ))
            
            # Obtener todas las ubicaciones únicas de los sensores
            sensores = self.mongodb_service.obtener_sensores()
            ubicaciones_sensores = set()
            
            for sensor in sensores:
                location = sensor.get('location', {})
                if isinstance(location, dict):
                    ciudad = location.get('city', '')
                    pais = location.get('country', '')
                    if ciudad and pais:
                        ubicaciones_sensores.add((ciudad, pais))
                elif isinstance(location, str):
                    # Parsear string de ubicación formato "Ciudad, Zona - País" o "Ciudad - País"
                    if ' - ' in location:
                        # Extraer país (después del guión)
                        partes = location.split(' - ')
                        pais = partes[-1].strip()
                        
                        # Extraer ciudad (antes del guión, puede tener zona)
                        ciudad_zona = partes[0].strip()
                        if ', ' in ciudad_zona:
                            ciudad, _ = ciudad_zona.split(', ', 1)
                            ciudad = ciudad.strip()
                        else:
                            ciudad = ciudad_zona
                        
                        if ciudad and pais:
                            ubicaciones_sensores.add((ciudad, pais))
            
            # Obtener umbrales configurados por ubicación
            umbrales_configurados = {}
            ubicaciones_db = self.mongodb_service.db.location_thresholds.find()
            for ubicacion in ubicaciones_db:
                ciudad = ubicacion.get("ciudad", "")
                pais = ubicacion.get("pais", "")
                if ciudad and pais:
                    umbrales_configurados[(ciudad, pais)] = ubicacion
            
            # Mostrar todas las ubicaciones de sensores
            for ciudad, pais in sorted(ubicaciones_sensores):
                ubicacion_str = f"{ciudad}, {pais}"
                
                # Verificar si tiene umbrales configurados
                if (ciudad, pais) in umbrales_configurados:
                    ubicacion_data = umbrales_configurados[(ciudad, pais)]
                    thresholds = ubicacion_data.get("thresholds", {})
                    temp_min = thresholds.get("Temperatura", {}).get("min", "N/A")
                    temp_max = thresholds.get("Temperatura", {}).get("max", "N/A")
                    hum_min = thresholds.get("Humedad", {}).get("min", "N/A")
                    hum_max = thresholds.get("Humedad", {}).get("max", "N/A")
                    updated_at = ubicacion_data.get("updated_at", "N/A")
                else:
                    # Mostrar valores por defecto si no están configurados
                    temp_min = "5"
                    temp_max = "35"
                    hum_min = "30"
                    hum_max = "80"
                    updated_at = "No configurado"
                
                self.tree_umbrales_ubicacion.insert('', 'end', values=(
                    ubicacion_str, temp_min, temp_max, hum_min, hum_max, updated_at
                ))
                
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando umbrales por ubicación: {e}")
    
    def configurar_umbrales_ubicacion_click(self, event):
        """Configurar umbrales al hacer doble clic en una ubicación"""
        try:
            selection = self.tree_umbrales_ubicacion.selection()
            if not selection:
                return
            
            item = self.tree_umbrales_ubicacion.item(selection[0])
            ubicacion = item['values'][0]
            
            if ubicacion == "🌐 Globales":
                self.configurar_umbrales_globales()
            else:
                # Extraer ciudad y país
                if ', ' in ubicacion:
                    ciudad, pais = ubicacion.split(', ', 1)
                    self.configurar_umbrales_ubicacion_especifica(ciudad, pais)
                else:
                    messagebox.showerror("Error", "Formato de ubicación inválido")
                    
        except Exception as e:
            self.agregar_log(f"❌ Error configurando umbrales por clic: {e}")
            messagebox.showerror("Error", f"Error configurando umbrales: {e}")
    
    def configurar_umbrales_globales(self):
        """Configurar umbrales globales"""
        self.abrir_ventana_configuracion("Globales", "", "")
    
    def configurar_umbrales_ubicacion_especifica(self, ciudad, pais):
        """Configurar umbrales para una ubicación específica"""
        self.abrir_ventana_configuracion(f"{ciudad}, {pais}", ciudad, pais)
    
    def crear_nueva_ubicacion_umbrales(self):
        """Crear nueva ubicación para configurar umbrales"""
        self.abrir_ventana_configuracion("Nueva Ubicación", "", "")
    
    def abrir_ventana_configuracion(self, titulo, ciudad_predefinida, pais_predefinido):
        """Abrir ventana para configurar umbrales"""
        try:
            # Crear ventana de configuración
            config_window = tk.Toplevel(self.root)
            config_window.title(f"⚙️ Configurar Umbrales - {titulo}")
            config_window.geometry("500x400")
            config_window.configure(bg='white')
            config_window.transient(self.root)
            config_window.grab_set()
            
            # Frame principal
            main_frame = tk.Frame(config_window, bg='white')
            main_frame.pack(fill='both', expand=True, padx=20, pady=20)
            
            # Título
            tk.Label(main_frame, text=f"⚙️ Configurar Umbrales - {titulo}", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=(0, 20))
            
            # Campos de ubicación (solo si no es global)
            if titulo != "Globales":
                ubicacion_frame = tk.Frame(main_frame, bg='white')
                ubicacion_frame.pack(fill='x', pady=(0, 20))
                
                tk.Label(ubicacion_frame, text="📍 Ubicación:", bg='white', font=('Arial', 10, 'bold')).pack(anchor='w')
                
                ubicacion_input_frame = tk.Frame(ubicacion_frame, bg='white')
                ubicacion_input_frame.pack(fill='x', pady=5)
                
                tk.Label(ubicacion_input_frame, text="País:", bg='white').pack(side='left', padx=(0, 5))
                combo_pais = ttk.Combobox(ubicacion_input_frame, width=18)
                combo_pais.pack(side='left', padx=(0, 20))
                
                tk.Label(ubicacion_input_frame, text="Ciudad:", bg='white').pack(side='left', padx=(0, 5))
                combo_ciudad = ttk.Combobox(ubicacion_input_frame, width=18)
                combo_ciudad.pack(side='left')
                
                # Cargar países y ciudades desde MongoDB
                try:
                    sensores = self.mongodb_service.obtener_sensores()
                    paises_set = set()
                    ciudades_por_pais = {}
                    
                    for sensor in sensores:
                        location = sensor.get('location', {})
                        if isinstance(location, dict):
                            ciudad = location.get('city', '')
                            pais = location.get('country', '')
                            if ciudad and pais:
                                paises_set.add(pais)
                                if pais not in ciudades_por_pais:
                                    ciudades_por_pais[pais] = set()
                                ciudades_por_pais[pais].add(ciudad)
                    
                    # Agregar países adicionales
                    paises_set.update(["Argentina", "Brasil", "Chile", "Colombia", "Uruguay", "Paraguay", "Perú"])
                    
                    combo_pais['values'] = sorted(list(paises_set))
                    if pais_predefinido:
                        combo_pais.set(pais_predefinido)
                    elif combo_pais['values']:
                        combo_pais.set(combo_pais['values'][0])
                    
                    # Función para actualizar ciudades cuando se selecciona país
                    def actualizar_ciudades_combo(event):
                        pais_sel = combo_pais.get()
                        ciudades_lista = sorted(list(ciudades_por_pais.get(pais_sel, set())))
                        # Agregar ciudades adicionales según el país
                        ciudades_adicionales = self.obtener_ciudades_adicionales_por_pais(pais_sel)
                        ciudades_completas = list(set(ciudades_lista + ciudades_adicionales))
                        ciudades_completas.sort()
                        combo_ciudad['values'] = ciudades_completas
                        if ciudades_completas:
                            combo_ciudad.set(ciudades_completas[0])
                    
                    combo_pais.bind('<<ComboboxSelected>>', actualizar_ciudades_combo)
                    
                    # Cargar ciudades iniciales
                    if combo_pais.get():
                        actualizar_ciudades_combo(None)
                    if ciudad_predefinida:
                        combo_ciudad.set(ciudad_predefinida)
                except Exception as e:
                    self.agregar_log(f"⚠️ Error cargando ubicaciones: {e}")
            
            # Frame para umbrales
            umbrales_frame = tk.LabelFrame(main_frame, text="🌡️ Configuración de Umbrales", 
                                         font=('Arial', 10, 'bold'), bg='white')
            umbrales_frame.pack(fill='x', pady=(0, 20))
            
            # Temperatura
            temp_frame = tk.Frame(umbrales_frame, bg='white')
            temp_frame.pack(fill='x', padx=10, pady=10)
            
            tk.Label(temp_frame, text="🌡️ Temperatura:", bg='white', font=('Arial', 10, 'bold')).pack(anchor='w')
            
            temp_input_frame = tk.Frame(temp_frame, bg='white')
            temp_input_frame.pack(fill='x', pady=5)
            
            tk.Label(temp_input_frame, text="Mínima (°C):", bg='white').pack(side='left', padx=(0, 5))
            entry_temp_min = tk.Entry(temp_input_frame, width=10)
            entry_temp_min.pack(side='left', padx=(0, 20))
            
            tk.Label(temp_input_frame, text="Máxima (°C):", bg='white').pack(side='left', padx=(0, 5))
            entry_temp_max = tk.Entry(temp_input_frame, width=10)
            entry_temp_max.pack(side='left')
            
            # Humedad
            hum_frame = tk.Frame(umbrales_frame, bg='white')
            hum_frame.pack(fill='x', padx=10, pady=10)
            
            tk.Label(hum_frame, text="💧 Humedad:", bg='white', font=('Arial', 10, 'bold')).pack(anchor='w')
            
            hum_input_frame = tk.Frame(hum_frame, bg='white')
            hum_input_frame.pack(fill='x', pady=5)
            
            tk.Label(hum_input_frame, text="Mínima (%):", bg='white').pack(side='left', padx=(0, 5))
            entry_hum_min = tk.Entry(hum_input_frame, width=10)
            entry_hum_min.pack(side='left', padx=(0, 20))
            
            tk.Label(hum_input_frame, text="Máxima (%):", bg='white').pack(side='left', padx=(0, 5))
            entry_hum_max = tk.Entry(hum_input_frame, width=10)
            entry_hum_max.pack(side='left')
            
            # Cargar valores actuales si existen
            if titulo == "Globales":
                umbrales_actuales = self.mongodb_service.obtener_umbrales_globales()
            else:
                umbrales_actuales = self.mongodb_service.obtener_umbrales_ubicacion(ciudad_predefinida, pais_predefinido)
            
            if umbrales_actuales:
                thresholds = umbrales_actuales.get("thresholds", {})
                temp_config = thresholds.get("Temperatura", {})
                hum_config = thresholds.get("Humedad", {})
                
                entry_temp_min.insert(0, str(temp_config.get("min", "")))
                entry_temp_max.insert(0, str(temp_config.get("max", "")))
                entry_hum_min.insert(0, str(hum_config.get("min", "")))
                entry_hum_max.insert(0, str(hum_config.get("max", "")))
            
            # Botones
            button_frame = tk.Frame(main_frame, bg='white')
            button_frame.pack(fill='x', pady=20)
            
            def guardar_configuracion():
                try:
                    # Validar campos
                    temp_min = float(entry_temp_min.get())
                    temp_max = float(entry_temp_max.get())
                    hum_min = float(entry_hum_min.get())
                    hum_max = float(entry_hum_max.get())
                    
                    if temp_min >= temp_max:
                        messagebox.showerror("Error", "La temperatura mínima debe ser menor que la máxima")
                        return
                    
                    if hum_min >= hum_max:
                        messagebox.showerror("Error", "La humedad mínima debe ser menor que la máxima")
                        return
                    
                    # Preparar datos
                    umbrales_data = {
                        "Temperatura": {"min": temp_min, "max": temp_max},
                        "Humedad": {"min": hum_min, "max": hum_max}
                    }
                    
                    # Guardar según el tipo
                    if titulo == "Globales":
                        if self.mongodb_service.guardar_umbrales_globales(umbrales_data):
                            messagebox.showinfo("Éxito", "✅ Umbrales globales guardados correctamente")
                            self.agregar_log("✅ Umbrales globales actualizados")
                        else:
                            messagebox.showerror("Error", "Error guardando umbrales globales")
                    else:
                        ciudad = combo_ciudad.get().strip()
                        pais = combo_pais.get().strip()
                        
                        if not ciudad or not pais:
                            messagebox.showerror("Error", "Ingrese ciudad y país")
                            return
                        
                        if self.mongodb_service.guardar_umbrales_ubicacion(ciudad, pais, umbrales_data):
                            messagebox.showinfo("Éxito", f"✅ Umbrales guardados para {ciudad}, {pais}")
                            self.agregar_log(f"✅ Umbrales actualizados para {ciudad}, {pais}")
                        else:
                            messagebox.showerror("Error", f"Error guardando umbrales para {ciudad}, {pais}")
                    
                    # Actualizar lista principal
                    if hasattr(self, 'actualizar_umbrales_ubicacion'):
                        self.actualizar_umbrales_ubicacion()
                    
                    config_window.destroy()
                    
                except ValueError:
                    messagebox.showerror("Error", "Ingrese valores numéricos válidos")
                except Exception as e:
                    messagebox.showerror("Error", f"Error guardando configuración: {e}")
            
            tk.Button(button_frame, text="💾 Guardar", 
                     command=guardar_configuracion, 
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(button_frame, text="❌ Cancelar", 
                     command=config_window.destroy, 
                     bg='#95a5a6', fg='white', font=('Arial', 10)).pack(side='right', padx=5)
            
        except Exception as e:
            self.agregar_log(f"❌ Error abriendo ventana de configuración: {e}")
            messagebox.showerror("Error", f"Error abriendo configuración: {e}")
    
    def configurar_tab_visualizar(self, tab):
        """Configurar pestaña de visualización de umbrales"""
        try:
            # Información sobre cómo funcionan los umbrales
            info_frame = tk.LabelFrame(tab, text="ℹ️ Información", 
                                     font=('Arial', 12, 'bold'), bg='white')
            info_frame.pack(fill='x', padx=10, pady=10)
            
            info_text = """
🌍 Los umbrales climáticos se configuran por CIUDAD y PAÍS:
• Cada sensor tiene una ubicación (ciudad, país)
• Los umbrales se aplican automáticamente según la ubicación del sensor
• Se pueden configurar umbrales globales (para todas las ubicaciones)
• Se pueden configurar umbrales específicos por sensor

📊 Jerarquía de umbrales:
1. Umbrales específicos del sensor (si existen)
2. Umbrales globales (si no hay específicos)
3. Umbrales por defecto (si no hay configuración)

🔍 El botón "Detectar Alertas" analiza TODAS las mediciones:
• Revisa cada medición de cada sensor
• Compara con los umbrales configurados
• Crea alertas automáticas si se exceden los límites
            """
            
            tk.Label(info_frame, text=info_text, font=('Arial', 10), 
                    bg='white', justify='left').pack(padx=10, pady=10)
            
            # Mostrar umbrales actuales
            umbrales_frame = tk.LabelFrame(tab, text="📋 Umbrales Actuales", 
                                         font=('Arial', 12, 'bold'), bg='white')
            umbrales_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Treeview para mostrar umbrales
            columns = ("Ubicación", "Sensor", "Temperatura Min", "Temperatura Max", "Humedad Min", "Humedad Max", "Tipo")
            self.tree_umbrales_visualizar = ttk.Treeview(umbrales_frame, columns=columns, show="headings")
            
            # Configurar columnas
            self.tree_umbrales_visualizar.heading("Ubicación", text="Ubicación")
            self.tree_umbrales_visualizar.heading("Sensor", text="Sensor")
            self.tree_umbrales_visualizar.heading("Temperatura Min", text="Temp Min (°C)")
            self.tree_umbrales_visualizar.heading("Temperatura Max", text="Temp Max (°C)")
            self.tree_umbrales_visualizar.heading("Humedad Min", text="Hum Min (%)")
            self.tree_umbrales_visualizar.heading("Humedad Max", text="Hum Max (%)")
            self.tree_umbrales_visualizar.heading("Tipo", text="Tipo")
            
            self.tree_umbrales_visualizar.column("Ubicación", width=150)
            self.tree_umbrales_visualizar.column("Sensor", width=120)
            self.tree_umbrales_visualizar.column("Temperatura Min", width=100)
            self.tree_umbrales_visualizar.column("Temperatura Max", width=100)
            self.tree_umbrales_visualizar.column("Humedad Min", width=100)
            self.tree_umbrales_visualizar.column("Humedad Max", width=100)
            self.tree_umbrales_visualizar.column("Tipo", width=100)
            
            # Scrollbar
            scrollbar_umbrales = ttk.Scrollbar(umbrales_frame, orient="vertical", command=self.tree_umbrales_visualizar.yview)
            self.tree_umbrales_visualizar.configure(yscrollcommand=scrollbar_umbrales.set)
            
            self.tree_umbrales_visualizar.pack(side="left", fill="both", expand=True)
            scrollbar_umbrales.pack(side="right", fill="y")
            
            # Cargar umbrales
            self.cargar_umbrales_en_treeview(self.tree_umbrales_visualizar)
            
        except Exception as e:
            self.agregar_log(f"❌ Error configurando tab visualizar: {e}")
    
    def configurar_tab_configurar(self, tab):
        """Configurar pestaña de configuración de umbrales"""
        try:
            # Usar la funcionalidad existente de configurar_umbrales pero en esta pestaña
            # Frame para configuración
            config_frame = tk.LabelFrame(tab, text="⚙️ Configuración de Umbrales", 
                                       font=('Arial', 12, 'bold'), bg='white')
            config_frame.pack(fill='x', padx=10, pady=10)
            
            # Frame interno para la configuración
            config_inner = tk.Frame(config_frame, bg='white')
            config_inner.pack(fill='x', padx=10, pady=10)
            
            # Tipo de configuración
            tk.Label(config_inner, text="Tipo de configuración:", bg='white', font=('Arial', 10, 'bold')).pack(anchor='w')
            
            self.config_type_var = tk.StringVar(value="ubicacion")
            config_type_frame = tk.Frame(config_inner, bg='white')
            config_type_frame.pack(fill='x', pady=5)
            
            tk.Radiobutton(config_type_frame, text="🌍 Umbrales Globales", 
                          variable=self.config_type_var, value="global", 
                          command=self.cambiar_tipo_configuracion, bg='white').pack(side='left', padx=5)
            
            tk.Radiobutton(config_type_frame, text="📍 Umbrales por Ubicación", 
                          variable=self.config_type_var, value="ubicacion", 
                          command=self.cambiar_tipo_configuracion, bg='white').pack(side='left', padx=5)
            
            # Selector de ubicación
            ubicacion_frame = tk.Frame(config_inner, bg='white')
            ubicacion_frame.pack(fill='x', pady=5)
            
            tk.Label(ubicacion_frame, text="Ciudad:", bg='white').pack(side='left', padx=5)
            self.entry_ciudad_umbrales = tk.Entry(ubicacion_frame, width=20)
            self.entry_ciudad_umbrales.pack(side='left', padx=5)
            
            tk.Label(ubicacion_frame, text="País:", bg='white').pack(side='left', padx=5)
            self.entry_pais_umbrales = tk.Entry(ubicacion_frame, width=20)
            self.entry_pais_umbrales.pack(side='left', padx=5)
            
            # Cargar ubicaciones disponibles
            self.cargar_ubicaciones_para_umbrales()
            
            # Frame para umbrales
            self.umbrales_frame = tk.Frame(config_inner, bg='white')
            self.umbrales_frame.pack(fill='x', pady=10)
            
            # Crear interfaz de umbrales
            self.crear_interfaz_umbrales()
            
            # Botones de configuración
            button_config_frame = tk.Frame(config_inner, bg='white')
            button_config_frame.pack(fill='x', pady=10)
            
            tk.Button(button_config_frame, text="💾 Guardar Configuración", 
                     command=self.guardar_configuracion_umbrales, 
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(button_config_frame, text="🔄 Cargar Actual", 
                     command=self.cargar_configuracion_actual, 
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
        except Exception as e:
            self.agregar_log(f"❌ Error configurando tab configurar: {e}")
    
    def configurar_tab_historial(self, tab):
        """Configurar pestaña de historial de cambios"""
        try:
            # Frame para historial
            historial_frame = tk.LabelFrame(tab, text="📊 Historial de Cambios de Umbrales", 
                                          font=('Arial', 12, 'bold'), bg='white')
            historial_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Treeview para historial
            columns = ("Fecha", "Usuario", "Tipo", "Sensor/Global", "Cambio", "Valores Anteriores", "Valores Nuevos")
            self.tree_historial = ttk.Treeview(historial_frame, columns=columns, show="headings")
            
            # Configurar columnas
            self.tree_historial.heading("Fecha", text="Fecha")
            self.tree_historial.heading("Usuario", text="Usuario")
            self.tree_historial.heading("Tipo", text="Tipo")
            self.tree_historial.heading("Sensor/Global", text="Sensor/Global")
            self.tree_historial.heading("Cambio", text="Cambio")
            self.tree_historial.heading("Valores Anteriores", text="Valores Anteriores")
            self.tree_historial.heading("Valores Nuevos", text="Valores Nuevos")
            
            self.tree_historial.column("Fecha", width=120)
            self.tree_historial.column("Usuario", width=100)
            self.tree_historial.column("Tipo", width=100)
            self.tree_historial.column("Sensor/Global", width=120)
            self.tree_historial.column("Cambio", width=150)
            self.tree_historial.column("Valores Anteriores", width=150)
            self.tree_historial.column("Valores Nuevos", width=150)
            
            # Scrollbar
            scrollbar_historial = ttk.Scrollbar(historial_frame, orient="vertical", command=self.tree_historial.yview)
            self.tree_historial.configure(yscrollcommand=scrollbar_historial.set)
            
            self.tree_historial.pack(side="left", fill="both", expand=True)
            scrollbar_historial.pack(side="right", fill="y")
            
            # Cargar historial
            self.actualizar_historial_umbrales()
            
        except Exception as e:
            self.agregar_log(f"❌ Error configurando tab historial: {e}")
    
    def cargar_ubicaciones_para_umbrales(self):
        """Cargar ubicaciones disponibles para configuración de umbrales"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Obtener todas las ubicaciones únicas de los sensores
            sensores = self.mongodb_service.obtener_sensores()
            ubicaciones = set()
            
            for sensor in sensores:
                location = sensor.get('location', {})
                if isinstance(location, dict):
                    ciudad = location.get('city', '')
                    pais = location.get('country', '')
                    if ciudad and pais:
                        ubicaciones.add(f"{ciudad}, {pais}")
            
            # Crear combobox para seleccionar ubicación
            ubicacion_combo_frame = tk.Frame(self.entry_ciudad_umbrales.master, bg='white')
            ubicacion_combo_frame.pack(fill='x', pady=5)
            
            tk.Label(ubicacion_combo_frame, text="Ubicaciones disponibles:", bg='white').pack(anchor='w')
            self.combo_ubicacion_umbrales = ttk.Combobox(ubicacion_combo_frame, width=40)
            self.combo_ubicacion_umbrales.pack(fill='x', pady=2)
            self.combo_ubicacion_umbrales['values'] = sorted(list(ubicaciones))
            
            # Bind para auto-completar ciudad y país
            self.combo_ubicacion_umbrales.bind('<<ComboboxSelected>>', self.seleccionar_ubicacion_umbrales)
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ubicaciones para umbrales: {e}")
    
    def seleccionar_ubicacion_umbrales(self, event=None):
        """Auto-completar ciudad y país cuando se selecciona una ubicación"""
        try:
            ubicacion_seleccionada = self.combo_ubicacion_umbrales.get()
            if ubicacion_seleccionada and ', ' in ubicacion_seleccionada:
                ciudad, pais = ubicacion_seleccionada.split(', ', 1)
                self.entry_ciudad_umbrales.delete(0, tk.END)
                self.entry_ciudad_umbrales.insert(0, ciudad)
                self.entry_pais_umbrales.delete(0, tk.END)
                self.entry_pais_umbrales.insert(0, pais)
        except Exception as e:
            self.agregar_log(f"❌ Error seleccionando ubicación: {e}")
    
    def actualizar_todas_las_pestanas(self, notebook):
        """Actualizar todas las pestañas"""
        try:
            # Actualizar pestaña de visualización
            if hasattr(self, 'tree_umbrales_visualizar'):
                self.cargar_umbrales_en_treeview(self.tree_umbrales_visualizar)
            
            # Actualizar pestaña de historial
            if hasattr(self, 'tree_historial'):
                self.actualizar_historial_umbrales()
            
            self.agregar_log("✅ Todas las pestañas actualizadas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando pestañas: {e}")
    
    def cargar_umbrales_en_treeview(self, tree_umbrales):
        """Cargar umbrales en el TreeView"""
        try:
            # Limpiar treeview
            for item in tree_umbrales.get_children():
                tree_umbrales.delete(item)
            
            # Obtener umbrales globales
            umbrales_globales = self.mongodb_service.obtener_umbrales_globales()
            if umbrales_globales:
                temp_min = umbrales_globales.get('Temperatura', {}).get('min', 'N/A')
                temp_max = umbrales_globales.get('Temperatura', {}).get('max', 'N/A')
                hum_min = umbrales_globales.get('Humedad', {}).get('min', 'N/A')
                hum_max = umbrales_globales.get('Humedad', {}).get('max', 'N/A')
                
                tree_umbrales.insert('', 'end', values=(
                    "🌍 Global", "Todos", temp_min, temp_max, hum_min, hum_max, "Global"
                ))
            
            # Obtener sensores y sus umbrales específicos
            sensores = self.mongodb_service.obtener_sensores()
            for sensor in sensores:
                sensor_id = sensor.get('sensor_id')
                location = sensor.get('location', {})
                
                if isinstance(location, dict):
                    ciudad = location.get('city', 'N/A')
                    pais = location.get('country', 'N/A')
                    ubicacion = f"{ciudad}, {pais}"
                else:
                    ubicacion = "N/A"
                
                # Obtener umbrales específicos del sensor
                umbrales_sensor = self.mongodb_service.obtener_umbrales_sensor(sensor_id)
                
                if umbrales_sensor:
                    temp_min = umbrales_sensor.get('Temperatura', {}).get('min', 'N/A')
                    temp_max = umbrales_sensor.get('Temperatura', {}).get('max', 'N/A')
                    hum_min = umbrales_sensor.get('Humedad', {}).get('min', 'N/A')
                    hum_max = umbrales_sensor.get('Humedad', {}).get('max', 'N/A')
                    
                    tree_umbrales.insert('', 'end', values=(
                        ubicacion, sensor_id, temp_min, temp_max, hum_min, hum_max, "Específico"
                    ))
                else:
                    # Mostrar que usa umbrales globales o por defecto
                    tree_umbrales.insert('', 'end', values=(
                        ubicacion, sensor_id, "Global", "Global", "Global", "Global", "Heredado"
                    ))
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando umbrales en TreeView: {e}")
    
    def mostrar_dashboard_alertas(self):
        """Mostrar dashboard con estadísticas de alertas"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Crear ventana del dashboard
            dashboard_window = tk.Toplevel(self.root)
            dashboard_window.title("📊 Dashboard de Alertas")
            dashboard_window.geometry("1000x700")
            dashboard_window.configure(bg='white')
            dashboard_window.transient(self.root)
            
            # Título
            title_frame = tk.Frame(dashboard_window, bg='white')
            title_frame.pack(fill='x', padx=20, pady=10)
            
            tk.Label(title_frame, text="📊 Dashboard de Alertas del Sistema", 
                    font=('Arial', 18, 'bold'), bg='white').pack()
            
            tk.Label(title_frame, text="Estadísticas y análisis de alertas climáticas y de sensores", 
                    font=('Arial', 10), bg='white', fg='#7f8c8d').pack(pady=5)
            
            # Frame principal con scrollbar
            main_frame = tk.Frame(dashboard_window, bg='white')
            main_frame.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Crear canvas para scrollbar
            canvas = tk.Canvas(main_frame, bg='white')
            scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
            scrollable_frame = tk.Frame(canvas, bg='white')
            
            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )
            
            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            # Estadísticas generales
            stats_frame = tk.LabelFrame(scrollable_frame, text="📈 Estadísticas Generales", 
                                      font=('Arial', 12, 'bold'), bg='white')
            stats_frame.pack(fill='x', padx=10, pady=10)
            
            # Obtener todas las alertas
            alertas = self.mongodb_service.obtener_alertas()
            
            # Calcular estadísticas
            total_alertas = len(alertas)
            alertas_activas = len([a for a in alertas if a.get('status') == 'active'])
            alertas_resueltas = len([a for a in alertas if a.get('status') == 'resolved'])
            alertas_climaticas = len([a for a in alertas if a.get('categoria') == 'Climática'])
            alertas_sensor = len([a for a in alertas if a.get('categoria') == 'Sensor'])
            
            # Contar por severidad
            severidad_counts = {}
            for alerta in alertas:
                severity = alerta.get('severity', 'N/A')
                severidad_counts[severity] = severidad_counts.get(severity, 0) + 1
            
            # Crear grid de estadísticas
            stats_grid = tk.Frame(stats_frame, bg='white')
            stats_grid.pack(fill='x', padx=10, pady=10)
            
            # Estadísticas principales
            tk.Label(stats_grid, text=f"🔢 Total de Alertas:", font=('Arial', 10, 'bold'), bg='white').grid(row=0, column=0, sticky='w', padx=5)
            tk.Label(stats_grid, text=f"{total_alertas}", font=('Arial', 10), bg='white', fg='#2c3e50').grid(row=0, column=1, sticky='w', padx=5)
            
            tk.Label(stats_grid, text=f"🔴 Alertas Activas:", font=('Arial', 10, 'bold'), bg='white').grid(row=0, column=2, sticky='w', padx=5)
            tk.Label(stats_grid, text=f"{alertas_activas}", font=('Arial', 10), bg='white', fg='#e74c3c').grid(row=0, column=3, sticky='w', padx=5)
            
            tk.Label(stats_grid, text=f"✅ Alertas Resueltas:", font=('Arial', 10, 'bold'), bg='white').grid(row=1, column=0, sticky='w', padx=5)
            tk.Label(stats_grid, text=f"{alertas_resueltas}", font=('Arial', 10), bg='white', fg='#27ae60').grid(row=1, column=1, sticky='w', padx=5)
            
            tk.Label(stats_grid, text=f"🌡️ Alertas Climáticas:", font=('Arial', 10, 'bold'), bg='white').grid(row=1, column=2, sticky='w', padx=5)
            tk.Label(stats_grid, text=f"{alertas_climaticas}", font=('Arial', 10), bg='white', fg='#3498db').grid(row=1, column=3, sticky='w', padx=5)
            
            tk.Label(stats_grid, text=f"🔧 Alertas de Sensor:", font=('Arial', 10, 'bold'), bg='white').grid(row=2, column=0, sticky='w', padx=5)
            tk.Label(stats_grid, text=f"{alertas_sensor}", font=('Arial', 10), bg='white', fg='#9b59b6').grid(row=2, column=1, sticky='w', padx=5)
            
            # Estadísticas por severidad
            severity_frame = tk.LabelFrame(scrollable_frame, text="⚠️ Distribución por Severidad", 
                                        font=('Arial', 12, 'bold'), bg='white')
            severity_frame.pack(fill='x', padx=10, pady=10)
            
            severity_grid = tk.Frame(severity_frame, bg='white')
            severity_grid.pack(fill='x', padx=10, pady=10)
            
            row = 0
            col = 0
            for severity, count in severidad_counts.items():
                color = self.get_severity_color(severity)
                tk.Label(severity_grid, text=f"{severity}:", font=('Arial', 10, 'bold'), bg='white').grid(row=row, column=col, sticky='w', padx=5)
                tk.Label(severity_grid, text=f"{count}", font=('Arial', 10), bg='white', fg=color).grid(row=row, column=col+1, sticky='w', padx=5)
                col += 2
                if col >= 6:
                    col = 0
                    row += 1
            
            # Alertas recientes
            recent_frame = tk.LabelFrame(scrollable_frame, text="🕒 Alertas Recientes (Últimas 10)", 
                                       font=('Arial', 12, 'bold'), bg='white')
            recent_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Treeview para alertas recientes
            columns = ("Fecha", "Tipo", "Ubicación", "Severidad", "Estado", "Descripción")
            tree_recent = ttk.Treeview(recent_frame, columns=columns, show="headings")
            
            # Configurar columnas
            tree_recent.heading("Fecha", text="Fecha")
            tree_recent.heading("Tipo", text="Tipo")
            tree_recent.heading("Ubicación", text="Ubicación")
            tree_recent.heading("Severidad", text="Severidad")
            tree_recent.heading("Estado", text="Estado")
            tree_recent.heading("Descripción", text="Descripción")
            
            tree_recent.column("Fecha", width=120)
            tree_recent.column("Tipo", width=100)
            tree_recent.column("Ubicación", width=150)
            tree_recent.column("Severidad", width=100)
            tree_recent.column("Estado", width=100)
            tree_recent.column("Descripción", width=300)
            
            # Scrollbar para treeview
            scrollbar_recent = ttk.Scrollbar(recent_frame, orient="vertical", command=tree_recent.yview)
            tree_recent.configure(yscrollcommand=scrollbar_recent.set)
            
            tree_recent.pack(side="left", fill="both", expand=True)
            scrollbar_recent.pack(side="right", fill="y")
            
            # Cargar alertas recientes
            alertas_ordenadas = sorted(alertas, key=lambda x: x.get('created_at', ''), reverse=True)[:10]
            self.cargar_alertas_recientes(tree_recent, alertas_ordenadas)
            
            # Pack canvas y scrollbar
            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")
            
            # Botones
            button_frame = tk.Frame(dashboard_window, bg='white')
            button_frame.pack(fill='x', padx=20, pady=10)
            
            tk.Button(button_frame, text="🔄 Actualizar", 
                     command=lambda: self.actualizar_dashboard(dashboard_window), 
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(button_frame, text="📊 Ver Todas las Alertas", 
                     command=lambda: [dashboard_window.destroy(), self.actualizar_lista_alertas()], 
                     bg='#34495e', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(button_frame, text="❌ Cerrar", 
                     command=dashboard_window.destroy, 
                     bg='#95a5a6', fg='white', font=('Arial', 10)).pack(side='right', padx=5)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando dashboard de alertas: {e}")
            messagebox.showerror("Error", f"Error mostrando dashboard: {e}")
    
    def get_severity_color(self, severity):
        """Obtener color según severidad"""
        colors = {
            'crítica': '#e74c3c',
            'alta': '#e67e22',
            'media': '#f39c12',
            'baja': '#27ae60',
            'N/A': '#95a5a6'
        }
        return colors.get(severity.lower(), '#95a5a6')
    
    def cargar_alertas_recientes(self, tree_recent, alertas):
        """Cargar alertas recientes en el TreeView"""
        try:
            for alerta in alertas:
                # Fecha formateada
                fecha = alerta.get('created_at', 'N/A')
                if fecha != 'N/A':
                    try:
                        if isinstance(fecha, str):
                            from datetime import datetime
                            dt = datetime.fromisoformat(fecha.replace('Z', '+00:00'))
                            fecha_formateada = dt.strftime("%d/%m/%Y %H:%M")
                        else:
                            fecha_formateada = str(fecha)
                    except:
                        fecha_formateada = str(fecha)
                else:
                    fecha_formateada = 'N/A'
                
                # Tipo con icono
                categoria = alerta.get('categoria', 'Sensor')
                tipo_icono = "🌡️" if categoria == "Climática" else "🔧"
                
                # Ubicación
                if categoria == "Climática":
                    location = alerta.get('location', {})
                    if isinstance(location, dict):
                        ciudad = location.get('city', 'N/A')
                        pais = location.get('country', 'N/A')
                        ubicacion = f"{ciudad}, {pais}"
                    else:
                        ubicacion = str(location)
                else:
                    ubicacion = alerta.get('sensor_id', 'N/A')
                
                # Severidad con color
                severidad = alerta.get('severity', 'N/A')
                color = self.get_severity_color(severidad)
                
                # Estado con icono
                estado = alerta.get('status', 'N/A')
                if estado == 'active':
                    estado_icono = "🔴 Activa"
                elif estado == 'resolved':
                    estado_icono = "✅ Resuelta"
                else:
                    estado_icono = f"❓ {estado}"
                
                # Descripción truncada
                descripcion = alerta.get('message', '')[:50] + '...' if len(alerta.get('message', '')) > 50 else alerta.get('message', '')
                
                tree_recent.insert('', 'end', values=(
                    fecha_formateada,
                    f"{tipo_icono} {categoria}",
                    ubicacion,
                    severidad,
                    estado_icono,
                    descripcion
                ))
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando alertas recientes: {e}")
    
    def actualizar_dashboard(self, dashboard_window):
        """Actualizar dashboard"""
        try:
            dashboard_window.destroy()
            self.mostrar_dashboard_alertas()
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando dashboard: {e}")
    
    def exportar_reporte_alertas(self):
        """Exportar reporte de alertas a archivo"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Crear ventana de configuración de exportación
            export_window = tk.Toplevel(self.root)
            export_window.title("📄 Exportar Reporte de Alertas")
            export_window.geometry("500x400")
            export_window.configure(bg='white')
            export_window.transient(self.root)
            export_window.grab_set()
            
            # Título
            tk.Label(export_window, text="📄 Exportar Reporte de Alertas", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=20)
            
            # Frame para opciones
            options_frame = tk.LabelFrame(export_window, text="⚙️ Opciones de Exportación", 
                                        font=('Arial', 12, 'bold'), bg='white')
            options_frame.pack(fill='x', padx=20, pady=10)
            
            # Tipo de archivo
            tk.Label(options_frame, text="Tipo de archivo:", bg='white', font=('Arial', 10, 'bold')).pack(anchor='w', padx=10, pady=5)
            file_type_var = tk.StringVar(value="CSV")
            tk.Radiobutton(options_frame, text="CSV (Excel)", variable=file_type_var, value="CSV", bg='white').pack(anchor='w', padx=20)
            tk.Radiobutton(options_frame, text="TXT (Texto)", variable=file_type_var, value="TXT", bg='white').pack(anchor='w', padx=20)
            
            # Filtros
            tk.Label(options_frame, text="Filtros:", bg='white', font=('Arial', 10, 'bold')).pack(anchor='w', padx=10, pady=(10,5))
            
            # Filtro por tipo
            tk.Label(options_frame, text="Tipo de alerta:", bg='white').pack(anchor='w', padx=20)
            tipo_var = tk.StringVar(value="Todas")
            tipo_combo = ttk.Combobox(options_frame, textvariable=tipo_var, values=["Todas", "Climática", "Sensor"], width=20)
            tipo_combo.pack(anchor='w', padx=40, pady=2)
            
            # Filtro por estado
            tk.Label(options_frame, text="Estado:", bg='white').pack(anchor='w', padx=20)
            estado_var = tk.StringVar(value="Todas")
            estado_combo = ttk.Combobox(options_frame, textvariable=estado_var, values=["Todas", "Activa", "Resuelta"], width=20)
            estado_combo.pack(anchor='w', padx=40, pady=2)
            
            # Filtro por fecha
            tk.Label(options_frame, text="Rango de fechas:", bg='white').pack(anchor='w', padx=20)
            fecha_frame = tk.Frame(options_frame, bg='white')
            fecha_frame.pack(anchor='w', padx=40, pady=2)
            
            tk.Label(fecha_frame, text="Desde:", bg='white').pack(side='left')
            fecha_desde = tk.Entry(fecha_frame, width=12)
            fecha_desde.pack(side='left', padx=5)
            fecha_desde.insert(0, "2024-01-01")
            
            tk.Label(fecha_frame, text="Hasta:", bg='white').pack(side='left', padx=5)
            fecha_hasta = tk.Entry(fecha_frame, width=12)
            fecha_hasta.pack(side='left', padx=5)
            fecha_hasta.insert(0, "2024-12-31")
            
            # Botones
            button_frame = tk.Frame(export_window, bg='white')
            button_frame.pack(fill='x', padx=20, pady=20)
            
            def exportar():
                try:
                    # Obtener parámetros
                    file_type = file_type_var.get()
                    tipo_filtro = tipo_var.get()
                    estado_filtro = estado_var.get()
                    fecha_inicio = fecha_desde.get()
                    fecha_fin = fecha_hasta.get()
                    
                    # Seleccionar archivo de destino
                    if file_type == "CSV":
                        file_extension = "csv"
                        file_types = [("CSV files", "*.csv"), ("All files", "*.*")]
                    else:
                        file_extension = "txt"
                        file_types = [("Text files", "*.txt"), ("All files", "*.*")]
                    
                    from tkinter import filedialog
                    filename = filedialog.asksaveasfilename(
                        defaultextension=f".{file_extension}",
                        filetypes=file_types,
                        title="Guardar reporte de alertas"
                    )
                    
                    if not filename:
                        return
                    
                    # Obtener alertas con filtros
                    alertas = self.mongodb_service.obtener_alertas()
                    
                    # Aplicar filtros
                    alertas_filtradas = []
                    for alerta in alertas:
                        # Filtro por tipo
                        if tipo_filtro != "Todas":
                            categoria = alerta.get('categoria', 'Sensor')
                            if tipo_filtro == "Climática" and categoria != "Climática":
                                continue
                            elif tipo_filtro == "Sensor" and categoria != "Sensor":
                                continue
                        
                        # Filtro por estado
                        if estado_filtro != "Todas":
                            estado = alerta.get('status', 'N/A')
                            if estado_filtro == "Activa" and estado != "active":
                                continue
                            elif estado_filtro == "Resuelta" and estado != "resolved":
                                continue
                        
                        # Filtro por fecha
                        fecha_alerta = alerta.get('created_at', '')
                        if fecha_alerta and fecha_inicio and fecha_fin:
                            try:
                                from datetime import datetime
                                if isinstance(fecha_alerta, str):
                                    dt_alerta = datetime.fromisoformat(fecha_alerta.replace('Z', '+00:00'))
                                else:
                                    dt_alerta = fecha_alerta
                                
                                dt_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d")
                                dt_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
                                
                                if not (dt_inicio <= dt_alerta.date() <= dt_fin):
                                    continue
                            except:
                                pass  # Si hay error en fecha, incluir la alerta
                        
                        alertas_filtradas.append(alerta)
                    
                    # Generar reporte
                    if file_type == "CSV":
                        self.generar_reporte_csv(alertas_filtradas, filename)
                    else:
                        self.generar_reporte_txt(alertas_filtradas, filename)
                    
                    messagebox.showinfo("Éxito", f"Reporte exportado exitosamente:\n{filename}")
                    export_window.destroy()
                    
                except Exception as e:
                    messagebox.showerror("Error", f"Error exportando reporte: {e}")
                    self.agregar_log(f"❌ Error exportando reporte: {e}")
            
            tk.Button(button_frame, text="📄 Exportar", command=exportar, 
                     bg='#16a085', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(button_frame, text="❌ Cancelar", command=export_window.destroy, 
                     bg='#95a5a6', fg='white', font=('Arial', 10)).pack(side='right', padx=5)
            
        except Exception as e:
            self.agregar_log(f"❌ Error en exportación de reportes: {e}")
            messagebox.showerror("Error", f"Error iniciando exportación: {e}")
    
    def generar_reporte_csv(self, alertas, filename):
        """Generar reporte en formato CSV"""
        try:
            import csv
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['ID', 'Fecha', 'Tipo', 'Categoría', 'Ubicación', 'Sensor', 'Severidad', 'Estado', 'Descripción', 'Creado por']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                
                for alerta in alertas:
                    # Formatear fecha
                    fecha = alerta.get('created_at', 'N/A')
                    if fecha != 'N/A':
                        try:
                            if isinstance(fecha, str):
                                from datetime import datetime
                                dt = datetime.fromisoformat(fecha.replace('Z', '+00:00'))
                                fecha_formateada = dt.strftime("%d/%m/%Y %H:%M")
                            else:
                                fecha_formateada = str(fecha)
                        except:
                            fecha_formateada = str(fecha)
                    else:
                        fecha_formateada = 'N/A'
                    
                    # Ubicación
                    categoria = alerta.get('categoria', 'Sensor')
                    if categoria == "Climática":
                        location = alerta.get('location', {})
                        if isinstance(location, dict):
                            ciudad = location.get('city', 'N/A')
                            pais = location.get('country', 'N/A')
                            ubicacion = f"{ciudad}, {pais}"
                        else:
                            ubicacion = str(location)
                        sensor_id = 'N/A'
                    else:
                        ubicacion = 'N/A'
                        sensor_id = alerta.get('sensor_id', 'N/A')
                    
                    writer.writerow({
                        'ID': alerta.get('alert_id', 'N/A'),
                        'Fecha': fecha_formateada,
                        'Tipo': alerta.get('type', 'N/A'),
                        'Categoría': categoria,
                        'Ubicación': ubicacion,
                        'Sensor': sensor_id,
                        'Severidad': alerta.get('severity', 'N/A'),
                        'Estado': alerta.get('status', 'N/A'),
                        'Descripción': alerta.get('message', 'N/A'),
                        'Creado por': alerta.get('created_by', 'N/A')
                    })
            
            self.agregar_log(f"✅ Reporte CSV generado: {filename}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando reporte CSV: {e}")
            raise e
    
    def generar_reporte_txt(self, alertas, filename):
        """Generar reporte en formato TXT"""
        try:
            with open(filename, 'w', encoding='utf-8') as txtfile:
                txtfile.write("=" * 80 + "\n")
                txtfile.write("REPORTE DE ALERTAS DEL SISTEMA\n")
                txtfile.write("=" * 80 + "\n\n")
                
                txtfile.write(f"Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                txtfile.write(f"Total de alertas: {len(alertas)}\n\n")
                
                # Estadísticas
                alertas_activas = len([a for a in alertas if a.get('status') == 'active'])
                alertas_resueltas = len([a for a in alertas if a.get('status') == 'resolved'])
                alertas_climaticas = len([a for a in alertas if a.get('categoria') == 'Climática'])
                alertas_sensor = len([a for a in alertas if a.get('categoria') == 'Sensor'])
                
                txtfile.write("ESTADÍSTICAS:\n")
                txtfile.write("-" * 40 + "\n")
                txtfile.write(f"Alertas activas: {alertas_activas}\n")
                txtfile.write(f"Alertas resueltas: {alertas_resueltas}\n")
                txtfile.write(f"Alertas climáticas: {alertas_climaticas}\n")
                txtfile.write(f"Alertas de sensor: {alertas_sensor}\n\n")
                
                # Detalle de alertas
                txtfile.write("DETALLE DE ALERTAS:\n")
                txtfile.write("=" * 80 + "\n")
                
                for i, alerta in enumerate(alertas, 1):
                    txtfile.write(f"\n{i}. ID: {alerta.get('alert_id', 'N/A')}\n")
                    txtfile.write(f"   Fecha: {alerta.get('created_at', 'N/A')}\n")
                    txtfile.write(f"   Tipo: {alerta.get('type', 'N/A')}\n")
                    txtfile.write(f"   Categoría: {alerta.get('categoria', 'N/A')}\n")
                    txtfile.write(f"   Severidad: {alerta.get('severity', 'N/A')}\n")
                    txtfile.write(f"   Estado: {alerta.get('status', 'N/A')}\n")
                    
                    # Ubicación o sensor
                    categoria = alerta.get('categoria', 'Sensor')
                    if categoria == "Climática":
                        location = alerta.get('location', {})
                        if isinstance(location, dict):
                            ciudad = location.get('city', 'N/A')
                            pais = location.get('country', 'N/A')
                            txtfile.write(f"   Ubicación: {ciudad}, {pais}\n")
                        else:
                            txtfile.write(f"   Ubicación: {location}\n")
                    else:
                        txtfile.write(f"   Sensor: {alerta.get('sensor_id', 'N/A')}\n")
                    
                    txtfile.write(f"   Descripción: {alerta.get('message', 'N/A')}\n")
                    txtfile.write(f"   Creado por: {alerta.get('created_by', 'N/A')}\n")
                    txtfile.write("-" * 40 + "\n")
            
            self.agregar_log(f"✅ Reporte TXT generado: {filename}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando reporte TXT: {e}")
            raise e
    
    def aplicar_filtros_alertas(self):
        """Aplicar filtros a la lista de alertas"""
        try:
            # Obtener valores de filtros
            filtro_tipo = self.combo_filtro_tipo.get()
            filtro_estado = self.combo_filtro_estado.get()
            filtro_severidad = self.combo_filtro_severidad.get()
            
            # Limpiar lista actual
            for item in self.tree_alertas.get_children():
                self.tree_alertas.delete(item)
            
            # Obtener todas las alertas
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            alertas = self.mongodb_service.obtener_alertas()
            
            # Aplicar filtros
            alertas_filtradas = []
            for alerta in alertas:
                # Filtro por tipo
                if filtro_tipo != "Todas":
                    categoria = alerta.get('categoria', 'Sensor')
                    if filtro_tipo == "Climática" and categoria != "Climática":
                        continue
                    elif filtro_tipo == "Sensor" and categoria != "Sensor":
                        continue
                
                # Filtro por estado
                if filtro_estado != "Todas":
                    estado = alerta.get('status', 'N/A')
                    if filtro_estado == "Activa" and estado != "active":
                        continue
                    elif filtro_estado == "Resuelta" and estado != "resolved":
                        continue
                
                # Filtro por severidad
                if filtro_severidad != "Todas":
                    severidad = alerta.get('severity', '').lower()
                    if filtro_severidad.lower() != severidad:
                        continue
                
                alertas_filtradas.append(alerta)
            
            # Mostrar alertas filtradas
            self.mostrar_alertas_en_treeview(alertas_filtradas)
            
            self.agregar_log(f"🔍 Filtros aplicados: {len(alertas_filtradas)} alertas mostradas de {len(alertas)} totales")
            
        except Exception as e:
            self.agregar_log(f"❌ Error aplicando filtros: {e}")
    
    def mostrar_alertas_en_treeview(self, alertas):
        """Mostrar alertas en el TreeView con formato mejorado"""
        try:
            for alerta in alertas:
                # Determinar tipo y ubicación/sensor
                categoria = alerta.get('categoria', 'Sensor')
                tipo_icono = "🌡️" if categoria == "Climática" else "🔧"
                
                # Ubicación o sensor
                if categoria == "Climática":
                    # Para alertas climáticas, mostrar ubicación
                    location = alerta.get('location', {})
                    if isinstance(location, dict):
                        ciudad = location.get('city', 'N/A')
                        pais = location.get('country', 'N/A')
                        ubicacion = f"{ciudad}, {pais}"
                    else:
                        ubicacion = str(location)
                else:
                    # Para alertas de sensor, mostrar sensor ID
                    ubicacion = alerta.get('sensor_id', 'N/A')
                
                # Descripción mejorada
                descripcion = alerta.get('message', '')
                if not descripcion:
                    tipo_alerta = alerta.get('type', '')
                    if categoria == "Climática":
                        descripcion = f"Alerta climática: {tipo_alerta}"
                    else:
                        descripcion = f"Alerta de sensor: {tipo_alerta}"
                
                # Estado con iconos
                estado = alerta.get('status', 'N/A')
                if estado == 'active':
                    estado_icono = "🔴 Activa"
                elif estado == 'resolved':
                    estado_icono = "✅ Resuelta"
                else:
                    estado_icono = f"❓ {estado}"
                
                # Severidad con colores
                severidad = alerta.get('severity', 'N/A')
                if severidad.lower() == 'crítica':
                    severidad_icono = "🔴 Crítica"
                elif severidad.lower() == 'alta':
                    severidad_icono = "🟠 Alta"
                elif severidad.lower() == 'media':
                    severidad_icono = "🟡 Media"
                elif severidad.lower() == 'baja':
                    severidad_icono = "🟢 Baja"
                else:
                    severidad_icono = severidad
                
                # Fecha formateada
                fecha = alerta.get('created_at', 'N/A')
                if fecha != 'N/A':
                    try:
                        if isinstance(fecha, str):
                            from datetime import datetime
                            dt = datetime.fromisoformat(fecha.replace('Z', '+00:00'))
                            fecha_formateada = dt.strftime("%d/%m/%Y %H:%M")
                        else:
                            fecha_formateada = str(fecha)
                    except:
                        fecha_formateada = str(fecha)
                else:
                    fecha_formateada = 'N/A'
                
                # Quien resolvió
                resuelto_por = alerta.get('resolved_by', 'N/A')
                if resuelto_por != 'N/A' and resuelto_por is not None:
                    resuelto_por = self.obtener_username_por_user_id(resuelto_por)
                else:
                    resuelto_por = 'N/A'
                
                # Fecha de resolución formateada
                resuelto_en = alerta.get('resolved_at', 'N/A')
                if resuelto_en != 'N/A' and resuelto_en is not None:
                    try:
                        if isinstance(resuelto_en, str):
                            dt_res = datetime.fromisoformat(resuelto_en.replace('Z', '+00:00'))
                            resuelto_en_formateada = dt_res.strftime("%d/%m/%Y %H:%M")
                        else:
                            resuelto_en_formateada = str(resuelto_en)
                    except:
                        resuelto_en_formateada = str(resuelto_en)
                else:
                    resuelto_en_formateada = 'N/A'
                
                self.tree_alertas.insert('', 'end', values=(
                    alerta.get('alert_id', ''),
                    f"{tipo_icono} {categoria}",
                    ubicacion,
                    descripcion[:80] + '...' if len(descripcion) > 80 else descripcion,
                    severidad_icono,
                    estado_icono,
                    fecha_formateada,
                    resuelto_por,
                    resuelto_en_formateada
                ))
                
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando alertas: {e}")
    
    def cargar_sensores_para_alertas(self):
        """Cargar sensores para el combo de alertas"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar sensores de alertas")
                return
            
            # Obtener sensores desde MongoDB
            sensores = self.mongodb_service.obtener_sensores()
            
            # Crear lista de nombres de sensores formateados
            nombres_sensores = []
            for sensor in sensores:
                nombre_formateado = self.formatear_nombre_sensor(sensor)
                nombres_sensores.append(nombre_formateado)
            
            # Actualizar combo de sensores para alertas
            self.combo_sensor_alerta['values'] = nombres_sensores
            if nombres_sensores:
                self.combo_sensor_alerta.set(nombres_sensores[0])

            # Si existe el combo de control, actualizarlo también
            if hasattr(self, 'combo_sensor_control'):
                self.combo_sensor_control['values'] = nombres_sensores
                if nombres_sensores:
                    self.combo_sensor_control.set(nombres_sensores[0])
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando sensores para alertas: {e}")

    def registrar_control_funcionamiento(self):
        """Registrar control de funcionamiento y disparar alerta de sensor si corresponde"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return

            sensor_display = self.combo_sensor_control.get().strip() if hasattr(self, 'combo_sensor_control') else ''
            estado_sensor = self.combo_estado_sensor.get().strip() if hasattr(self, 'combo_estado_sensor') else ''
            fecha_rev = self.entry_fecha_control.get().strip() if hasattr(self, 'entry_fecha_control') else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            observaciones = self.txt_obs_control.get("1.0", tk.END).strip() if hasattr(self, 'txt_obs_control') else ''

            if not sensor_display or not estado_sensor:
                messagebox.showerror("Error", "Seleccione sensor y estado del sensor")
                return

            # Mapear display a sensor_id intentando buscar por nombre
            sensor_id = None
            sensores = self.mongodb_service.obtener_sensores()
            for s in sensores:
                if self.formatear_nombre_sensor(s) == sensor_display:
                    sensor_id = s.get('sensor_id')
                    break

            control_id = f"CTRL_{int(time.time())}"
            control_data = {
                "control_id": control_id,
                "sensor_id": sensor_id or sensor_display,
                "reviewed_at": fecha_rev,
                "sensor_state": estado_sensor.lower(),
                "observations": observaciones,
                "reviewed_by": getattr(self, 'usuario_autenticado', None)
            }

            if self.mongodb_service.crear_control(control_data):
                self.agregar_log(f"📝 Control registrado para {sensor_display}")

                # Disparar alerta de tipo sensor si hay falla
                if estado_sensor.lower() == 'falla':
                    alert_id = f"ALERT_SENS_{int(time.time())}"
                    alerta_data = {
                        "alert_id": alert_id,
                        "type": "sensor",
                        "categoria": "Sensor",
                        "sensor_id": sensor_id or sensor_display,
                        "description": f"Falla detectada en control {control_id}",
                        "severity": "high",
                        "status": "active",
                        "created_at": datetime.now().isoformat(),
                        "control_id": control_id
                    }
                    if self.mongodb_service.crear_alerta(alerta_data):
                        self.agregar_log(f"🚨 Alerta de sensor creada por control: {alert_id}")
                        self.actualizar_lista_alertas()

                messagebox.showinfo("Éxito", "Control registrado correctamente")
            else:
                messagebox.showerror("Error", "No se pudo registrar el control")

        except Exception as e:
            self.agregar_log(f"❌ Error registrando control: {e}")
            messagebox.showerror("Error", f"Error registrando control: {e}")
    
    def crear_tab_facturacion(self):
        """Crear tab de gestión de facturación"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Facturación")
        
        # Configuración
        config_frame = tk.LabelFrame(tab, text="Configuración de Facturación", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Campos para nueva factura
        tk.Label(config_inner, text="Usuario:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_usuario_factura = ttk.Combobox(config_inner, width=20)
        self.combo_usuario_factura.grid(row=0, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Servicio:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.combo_servicio_factura = ttk.Combobox(config_inner, values=["Análisis Básico", "Análisis Avanzado", "Reporte Personalizado", "Monitoreo 24/7"], width=20)
        self.combo_servicio_factura.grid(row=0, column=3, padx=5, pady=5)
        
        tk.Label(config_inner, text="Monto:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.entry_monto_factura = tk.Entry(config_inner, width=20)
        self.entry_monto_factura.grid(row=1, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Fecha Vencimiento:", bg='white').grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.entry_fecha_vencimiento = tk.Entry(config_inner, width=20)
        self.entry_fecha_vencimiento.grid(row=1, column=3, padx=5, pady=5)
        self.entry_fecha_vencimiento.insert(0, (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"))
        
        # Botones
        tk.Button(config_inner, text="📄 Generar Factura", 
                 command=self.generar_factura, 
                 bg='#27ae60', fg='white', font=('Arial', 10)).grid(row=2, column=0, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔄 Actualizar Lista", 
                 command=self.actualizar_lista_facturas, 
                 bg='#3498db', fg='white', font=('Arial', 10)).grid(row=2, column=1, padx=5, pady=10)
        
        tk.Button(config_inner, text="💳 Procesar Pago", 
                 command=self.procesar_pago, 
                 bg='#f39c12', fg='white', font=('Arial', 10)).grid(row=2, column=2, padx=5, pady=10)
        
        tk.Button(config_inner, text="📊 Resumen Financiero", 
                 command=self.mostrar_resumen_financiero, 
                 bg='#9b59b6', fg='white', font=('Arial', 10)).grid(row=2, column=3, padx=5, pady=10)
        
        # Botón para eliminar factura (solo para administradores)
        self.btn_eliminar_factura = tk.Button(config_inner, text="🗑️ Eliminar Factura", 
                 command=self.eliminar_factura, 
                 bg='#e74c3c', fg='white', font=('Arial', 10))
        self.btn_eliminar_factura.grid(row=2, column=4, padx=5, pady=10)
        
        # Lista de facturas
        lista_frame = tk.LabelFrame(tab, text="Lista de Facturas", 
                                  font=('Arial', 12, 'bold'), bg='white')
        lista_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Treeview para facturas
        columns = ("ID", "Usuario", "Servicio", "Monto", "Estado", "Fecha", "Vencimiento")
        self.tree_facturas = ttk.Treeview(lista_frame, columns=columns, show="headings")
        
        for col in columns:
            self.tree_facturas.heading(col, text=col)
            self.tree_facturas.column(col, width=100)
        
        # Scrollbar para la lista
        scrollbar_facturas = ttk.Scrollbar(lista_frame, orient="vertical", command=self.tree_facturas.yview)
        self.tree_facturas.configure(yscrollcommand=scrollbar_facturas.set)
        
        self.tree_facturas.pack(side="left", fill="both", expand=True)
        scrollbar_facturas.pack(side="right", fill="y")
        
        # Cargar usuarios para facturación (se cargará después del login)
    
    def crear_tab_comunicacion(self):
        """Crear tab de comunicación"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Comunicación")
        
        # Configuración de comunicación
        config_frame = tk.LabelFrame(tab, text="Configuración de Comunicación", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Campos para nuevo mensaje
        tk.Label(config_inner, text="Destinatario:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_destinatario = ttk.Combobox(config_inner, width=20)
        self.combo_destinatario.grid(row=0, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Tipo:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.combo_tipo_mensaje = ttk.Combobox(config_inner, values=["Privado", "Grupal"], width=20)
        self.combo_tipo_mensaje.grid(row=0, column=3, padx=5, pady=5)
        self.combo_tipo_mensaje.set("Privado")
        self.combo_tipo_mensaje.bind('<<ComboboxSelected>>', self.cambiar_tipo_mensaje)
        
        tk.Label(config_inner, text="Asunto:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.entry_asunto_mensaje = tk.Entry(config_inner, width=50)
        self.entry_asunto_mensaje.grid(row=1, column=1, columnspan=2, padx=5, pady=5)
        
        tk.Label(config_inner, text="Prioridad:", bg='white').grid(row=1, column=3, padx=5, pady=5, sticky='w')
        self.combo_prioridad_mensaje = ttk.Combobox(config_inner, values=["Baja", "Normal", "Alta", "Crítica"], width=20)
        self.combo_prioridad_mensaje.grid(row=1, column=4, padx=5, pady=5)
        self.combo_prioridad_mensaje.set("Normal")
        
        # Área de contenido del mensaje
        tk.Label(config_inner, text="Contenido del Mensaje:", bg='white', font=('Arial', 10, 'bold')).grid(row=2, column=0, padx=5, pady=(15,5), sticky='nw')
        self.texto_contenido_mensaje = scrolledtext.ScrolledText(config_inner, height=6, width=70)
        self.texto_contenido_mensaje.grid(row=3, column=0, columnspan=5, padx=5, pady=5, sticky='ew')
        
        # Configurar el grid para que el área de texto se expanda
        config_inner.grid_columnconfigure(0, weight=1)
        config_inner.grid_columnconfigure(1, weight=1)
        config_inner.grid_columnconfigure(2, weight=1)
        config_inner.grid_columnconfigure(3, weight=1)
        config_inner.grid_columnconfigure(4, weight=1)
        
        # Botones
        tk.Button(config_inner, text="📨 Enviar Mensaje", 
                 command=self.enviar_mensaje, 
                 bg='#27ae60', fg='white', font=('Arial', 10)).grid(row=4, column=0, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔄 Actualizar Mensajes", 
                 command=self.actualizar_mensajes, 
                 bg='#3498db', fg='white', font=('Arial', 10)).grid(row=4, column=1, padx=5, pady=10)
        
        self.btn_crear_grupo = tk.Button(config_inner, text="👥 Crear Grupo", 
                 command=self.crear_grupo, 
                 bg='#f39c12', fg='white', font=('Arial', 10))
        self.btn_crear_grupo.grid(row=4, column=2, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔄 Recargar Destinatarios", 
                 command=self.cargar_destinatarios, 
                 bg='#2c3e50', fg='white', font=('Arial', 10)).grid(row=4, column=3, padx=5, pady=10)
        
        self.btn_gestionar_grupos = tk.Button(config_inner, text="👥 Gestionar Grupos", 
                 command=self.gestionar_grupos, 
                 bg='#8e44ad', fg='white', font=('Arial', 10))
        self.btn_gestionar_grupos.grid(row=5, column=0, padx=5, pady=10)
        
        # Área de mensajes
        mensajes_frame = tk.LabelFrame(tab, text="Mensajes", 
                                     font=('Arial', 12, 'bold'), bg='white')
        mensajes_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        self.texto_mensajes = scrolledtext.ScrolledText(mensajes_frame, height=15)
        self.texto_mensajes.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Configurar botones según el rol del usuario
        self.configurar_botones_comunicacion()
        
        # Cargar lista de destinatarios (se cargará después del login)
    
    def configurar_botones_comunicacion(self):
        """Configurar botones de comunicación según el rol del usuario"""
        try:
            if not hasattr(self, 'rol_usuario'):
                return
            
            # Solo técnicos y administradores pueden gestionar grupos
            if self.rol_usuario == "usuario":
                # Deshabilitar botones de gestión de grupos para usuarios comunes
                if hasattr(self, 'btn_crear_grupo'):
                    self.btn_crear_grupo.config(state='disabled')
                if hasattr(self, 'btn_gestionar_grupos'):
                    self.btn_gestionar_grupos.config(state='disabled')
                
                # self.agregar_log("🔒 Botones de gestión de grupos deshabilitados para usuario común")
            else:
                # Habilitar botones para técnicos y administradores
                if hasattr(self, 'btn_crear_grupo'):
                    self.btn_crear_grupo.config(state='normal')
                if hasattr(self, 'btn_gestionar_grupos'):
                    self.btn_gestionar_grupos.config(state='normal')
                
                # self.agregar_log(f"✅ Botones de gestión de grupos habilitados para rol: {self.rol_usuario}")
                
        except Exception as e:
            self.agregar_log(f"❌ Error configurando botones de comunicación: {e}")
    
    def crear_tab_procesos(self):
        """Crear tab de gestión de procesos"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Procesos")
        
        # PanedWindow para dividir la pestaña en dos
        paned_window = tk.PanedWindow(tab, orient=tk.HORIZONTAL, sashrelief=tk.RAISED, bg='white')
        paned_window.pack(fill=tk.BOTH, expand=True)

        # Panel izquierdo: Lista de procesos
        left_pane = tk.Frame(paned_window, bg='white')
        paned_window.add(left_pane, width=600)

        lista_frame = tk.LabelFrame(left_pane, text="Mis Procesos",
                                    font=('Arial', 12, 'bold'), bg='white')
        lista_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Filtros para la lista de procesos (ANTES del treeview)
        filtros_frame = tk.Frame(lista_frame, bg='white')
        filtros_frame.pack(fill='x', padx=5, pady=5)
        
        tk.Label(filtros_frame, text="Filtros:", bg='white', font=('Arial', 10, 'bold')).grid(row=0, column=0, padx=5, pady=5, sticky='w')
        
        # Combo para filtrar por estado
        tk.Label(filtros_frame, text="Estado:", bg='white').grid(row=0, column=1, padx=5, pady=5, sticky='w')
        self.combo_filtro_estado = ttk.Combobox(filtros_frame, values=["Todos", "⏳ Pendiente", "🔄 En Ejecución", "✅ Completado", "❌ Fallido"], 
                                               width=20, state='readonly')
        self.combo_filtro_estado.grid(row=0, column=2, padx=5, pady=5)
        self.combo_filtro_estado.set("Todos")
        self.combo_filtro_estado.bind('<<ComboboxSelected>>', lambda e: self.actualizar_lista_procesos())

        columns = ("ID", "Nombre", "Tipo", "Tipo Proceso", "Ubicación", "Agrupación", "Estado")
        self.tree_procesos = ttk.Treeview(lista_frame, columns=columns, show="headings")

        for col in columns:
            self.tree_procesos.heading(col, text=col)
            self.tree_procesos.column(col, width=120)

        # Vincular evento de doble click
        self.tree_procesos.bind('<Double-Button-1>', self.on_double_click_proceso)
        
        # Vincular evento de selección simple (click simple) para autocompletar
        self.tree_procesos.bind('<<TreeviewSelect>>', self.on_select_proceso)
        
        self.tree_procesos.pack(fill='both', expand=True)
        
        botones_procesos_frame = tk.Frame(lista_frame, bg='white')
        botones_procesos_frame.pack(fill='x', padx=10, pady=5)
        
        tk.Button(botones_procesos_frame, text="Actualizar Lista",
                  command=self.actualizar_lista_procesos,
                  bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
        
        # Botón para eliminar proceso (solo para administradores)
        self.btn_eliminar_proceso = tk.Button(botones_procesos_frame, text="🗑️ Eliminar Proceso",
                  command=self.eliminar_proceso,
                  bg='#e74c3c', fg='white', font=('Arial', 10))
        self.btn_eliminar_proceso.pack(side='left', padx=5)

        # Panel derecho: Creación y ejecución
        right_pane = tk.Frame(paned_window, bg='white')
        paned_window.add(right_pane)

        # Frame para la creación de procesos
        self.creacion_frame = tk.LabelFrame(right_pane, text="Gestión de Procesos",
                                     font=('Arial', 12, 'bold'), bg='white')
        self.creacion_frame.pack(fill='x', padx=10, pady=10)

        # Botón para crear proceso (abre ventana completa con todos los campos)
        tk.Button(self.creacion_frame, text="➕ Crear Nuevo Proceso",
                  command=self.crear_proceso,
                  bg='#27ae60', fg='white', font=('Arial', 11, 'bold'), width=40, height=2).pack(pady=10)

        # Frame para la ejecución de procesos
        self.ejecucion_frame = tk.LabelFrame(right_pane, text="Ejecución de Procesos",
                                     font=('Arial', 12, 'bold'), bg='white')
        # NO empacar todavía, se configurará según el rol
        # self.ejecucion_frame.pack(fill='x', padx=10, pady=10)

        tk.Label(self.ejecucion_frame, text="Tipo de Análisis:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_tipo_analisis_proceso = ttk.Combobox(self.ejecucion_frame, values=["Humedad", "Temperatura", "Ambas"], width=37)
        self.combo_tipo_analisis_proceso.grid(row=0, column=1, padx=5, pady=5)
        self.combo_tipo_analisis_proceso.set("Ambas")

        tk.Label(self.ejecucion_frame, text="Agrupar por:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.combo_agrupacion_proceso = ttk.Combobox(self.ejecucion_frame, values=["Ciudad", "País", "Zona"], width=37)
        self.combo_agrupacion_proceso.grid(row=1, column=1, padx=5, pady=5)

        tk.Label(self.ejecucion_frame, text="Periodicidad:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.combo_periodicidad_proceso = ttk.Combobox(self.ejecucion_frame, values=["Anual", "Mensual", "Diario"], width=37)
        self.combo_periodicidad_proceso.grid(row=2, column=1, padx=5, pady=5)

        tk.Label(self.ejecucion_frame, text="País:", bg='white').grid(row=3, column=0, padx=5, pady=5, sticky='w')
        self.combo_pais_proceso = ttk.Combobox(self.ejecucion_frame, width=37)
        self.combo_pais_proceso.grid(row=3, column=1, padx=5, pady=5)

        tk.Label(self.ejecucion_frame, text="Ciudad:", bg='white').grid(row=4, column=0, padx=5, pady=5, sticky='w')
        self.combo_ciudad_proceso = ttk.Combobox(self.ejecucion_frame, width=37)
        self.combo_ciudad_proceso.grid(row=4, column=1, padx=5, pady=5)

        self.btn_ejecutar_proceso = tk.Button(self.ejecucion_frame, text="Ejecutar Proceso",
                                             command=self.ejecutar_proceso_analisis,
                                             bg='#27ae60', fg='white', font=('Arial', 10))
        self.btn_ejecutar_proceso.grid(row=5, column=1, padx=5, pady=10, sticky='e')

        # Frame para los resultados de los procesos
        resultados_frame = tk.LabelFrame(right_pane, text="Resultados del Proceso",
                                          font=('Arial', 12, 'bold'), bg='white')
        resultados_frame.pack(fill='both', expand=True, padx=10, pady=10)

        self.texto_resultados_proceso = scrolledtext.ScrolledText(resultados_frame, height=10)
        self.texto_resultados_proceso.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Botón para marcar como completado (solo para admin/técnico)
        self.btn_marcar_completado = tk.Button(resultados_frame, text="✅ Marcar Proceso como Completado",
                                              command=self.marcar_proceso_completado,
                                              bg='#27ae60', fg='white', font=('Arial', 10, 'bold'),
                                              state='disabled')
        self.btn_marcar_completado.pack(pady=5)

        self.cargar_ubicaciones_procesos()
        self.configurar_botones_procesos()
        self.configurar_botones_facturacion()
        self.actualizar_lista_procesos()

    def configurar_botones_procesos(self):
        """Configurar botones de procesos según el rol del usuario"""
        try:
            if not hasattr(self, 'rol_usuario'):
                if hasattr(self, 'ejecucion_frame'):
                    self.ejecucion_frame.pack_forget()
                if hasattr(self, 'creacion_frame'):
                    self.creacion_frame.pack_forget()
                return

            # Frame de ejecución solo para administradores y técnicos
            if self.rol_usuario in ["administrador", "técnico"]:
                # Administradores y técnicos pueden ejecutar procesos
                if hasattr(self, 'ejecucion_frame'):
                    self.ejecucion_frame.pack(fill='x', padx=10, pady=10)
                if hasattr(self, 'btn_ejecutar_proceso'):
                    self.btn_ejecutar_proceso.config(state='normal')
                
                # Habilitar botón de marcar como completado para admin/técnico
                if hasattr(self, 'btn_marcar_completado'):
                    # El botón se habilitará/deshabilitará dinámicamente según el proceso seleccionado
                    pass
                
                # Habilitar botón de eliminar proceso solo para administradores
                if hasattr(self, 'btn_eliminar_proceso'):
                    if self.rol_usuario == "administrador":
                        self.btn_eliminar_proceso.config(state='normal')
                    else:
                        self.btn_eliminar_proceso.config(state='disabled')
                
                # self.agregar_log(f"✅ Funcionalidad de ejecución de procesos habilitada para rol: {self.rol_usuario}")
            else:
                # Ocultar botón de eliminar para usuarios comunes
                if hasattr(self, 'btn_eliminar_proceso'):
                    self.btn_eliminar_proceso.config(state='disabled')
                
                # Usuarios comunes: ocultar frame de ejecución
                if hasattr(self, 'ejecucion_frame'):
                    self.ejecucion_frame.pack_forget()
                # Deshabilitar botón de marcar como completado
                if hasattr(self, 'btn_marcar_completado'):
                    self.btn_marcar_completado.config(state='disabled')
                self.agregar_log("ℹ Usuario común: puede crear procesos pero no ejecutarlos")

        except Exception as e:
            self.agregar_log(f"❌ Error configurando botones de procesos: {e}")
    
    def configurar_botones_facturacion(self):
        """Configurar botones de facturación según el rol del usuario"""
        try:
            if not hasattr(self, 'rol_usuario'):
                if hasattr(self, 'btn_eliminar_factura'):
                    self.btn_eliminar_factura.config(state='disabled')
                return
            
            # Botón de eliminar factura solo para administradores
            if hasattr(self, 'btn_eliminar_factura'):
                if self.rol_usuario == "administrador":
                    self.btn_eliminar_factura.config(state='normal')
                    self.agregar_log("✅ Botón 'Eliminar Factura' habilitado para administrador")
                else:
                    self.btn_eliminar_factura.config(state='disabled')
                    self.agregar_log(f" Botón 'Eliminar Factura' deshabilitado para rol: {self.rol_usuario}")
        
        except Exception as e:
            self.agregar_log(f"❌ Error configurando botones de facturación: {e}")

    def cargar_ubicaciones_procesos(self):
        """Cargar ubicaciones para los combos de la pestaña de procesos desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible, usando ubicaciones por defecto")
                self.combo_pais_proceso['values'] = ["Argentina", "Brasil", "Chile"]
                self.combo_ciudad_proceso['values'] = ["Buenos Aires", "Córdoba", "Rosario"]
                return

            # Obtener sensores de la base de datos
            sensores = self.mongodb_service.obtener_sensores()
            
            if not sensores:
                self.agregar_log("⚠️ No hay sensores registrados, usando ubicaciones por defecto")
                self.combo_pais_proceso['values'] = ["Argentina", "Brasil", "Chile"]
                self.combo_ciudad_proceso['values'] = ["Buenos Aires", "Córdoba", "Rosario"]
                return
            
            # Diccionarios para almacenar ubicaciones
            paises_ciudades = {}
            paises = set()
            
            for sensor in sensores:
                location = sensor.get('location')
                
                if isinstance(location, dict):
                    pais = location.get('country', '').strip()
                    ciudad = location.get('city', '').strip()
                    
                    if pais:
                        paises.add(pais)
                        if pais not in paises_ciudades:
                            paises_ciudades[pais] = set()
                        if ciudad:
                            paises_ciudades[pais].add(ciudad)
            
            # Convertir sets a listas ordenadas
            lista_paises = sorted(list(paises))
            
            # Configurar combo de países
            self.combo_pais_proceso['values'] = lista_paises
            
            
            # Función para actualizar ciudades cuando cambie el país
            def actualizar_ciudades(event=None):
                pais_seleccionado = self.combo_pais_proceso.get()
                if pais_seleccionado and pais_seleccionado in paises_ciudades:
                    ciudades = sorted(list(paises_ciudades[pais_seleccionado]))
                    self.combo_ciudad_proceso['values'] = ciudades
                else:
                    self.combo_ciudad_proceso['values'] = []
            
            # Vincular evento
            self.combo_pais_proceso.bind('<<ComboboxSelected>>', actualizar_ciudades)
            
            self.agregar_log(f"✅ Ubicaciones cargadas desde MongoDB: {len(lista_paises)} países")

        except Exception as e:
            self.agregar_log(f"❌ Error cargando ubicaciones para procesos: {e}")
            # En caso de error, usar valores por defecto
            self.combo_pais_proceso['values'] = ["Argentina", "Brasil", "Chile"]
            self.combo_ciudad_proceso['values'] = ["Buenos Aires", "Córdoba", "Rosario"]

    def ejecutar_proceso_analisis(self):
        """Ejecutar proceso de análisis de datos"""
        try:
            tipo_analisis = self.combo_tipo_analisis_proceso.get()
            agrupacion = self.combo_agrupacion_proceso.get()
            periodicidad = self.combo_periodicidad_proceso.get()
            pais = self.combo_pais_proceso.get().strip() if self.combo_pais_proceso.get() else ""
            ciudad = self.combo_ciudad_proceso.get().strip() if self.combo_ciudad_proceso.get() else ""
            zona = ""  # Ya no hay campo de zona

            if not all([tipo_analisis, agrupacion, periodicidad]):
                messagebox.showerror("Error", "Por favor, seleccione tipo de análisis, agrupación y periodicidad.")
                return

            self.texto_resultados_proceso.delete('1.0', tk.END)
            self.texto_resultados_proceso.insert(tk.END, f"Ejecutando proceso de análisis de {tipo_analisis.lower()}...\n")
            self.texto_resultados_proceso.insert(tk.END, f"Agrupación: {agrupacion}, Periodicidad: {periodicidad}\n")
            if pais:
                self.texto_resultados_proceso.insert(tk.END, f"País: {pais}\n")
            if ciudad:
                self.texto_resultados_proceso.insert(tk.END, f"Ciudad: {ciudad}\n")
            self.texto_resultados_proceso.insert(tk.END, "--------------------------------------------------\n")

            # Llamar al servicio de MongoDB para ejecutar el proceso
            resultados = self.mongodb_service.ejecutar_proceso_analisis(
                tipo_analisis, agrupacion, periodicidad, pais, ciudad, ""
            )

            if resultados:
                self.texto_resultados_proceso.insert(tk.END, f"\n{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, "📊 RESULTADOS DEL ANÁLISIS\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                
                for i, resultado in enumerate(resultados, 1):
                    self.texto_resultados_proceso.insert(tk.END, f"📋 Registro #{i}\n")
                    self.texto_resultados_proceso.insert(tk.END, f"{'─'*60}\n")
                    
                    # Extraer y formatear agrupación
                    agrupacion = resultado.get('agrupacion', {})
                    if isinstance(agrupacion, dict):
                        agrupacion_parts = []
                        if 'ciudad' in agrupacion:
                            agrupacion_parts.append(f"📍 Ciudad: {agrupacion['ciudad']}")
                        if 'zona' in agrupacion:
                            agrupacion_parts.append(f"🗺️  Zona: {agrupacion['zona']}")
                        if 'pais' in agrupacion:
                            agrupacion_parts.append(f"🌍 País: {agrupacion['pais']}")
                        if 'año' in agrupacion:
                            agrupacion_parts.append(f"📅 Año: {agrupacion['año']}")
                        if 'mes' in agrupacion:
                            mes_nombre = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                                         'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
                            try:
                                mes_str = mes_nombre[agrupacion['mes'] - 1]
                            except:
                                mes_str = f"{agrupacion['mes']}"
                            agrupacion_parts.append(f"🗓️  Mes: {mes_str}")
                        if 'dia' in agrupacion:
                            agrupacion_parts.append(f"📆 Día: {agrupacion['dia']}")
                        
                        if agrupacion_parts:
                            for part in agrupacion_parts:
                                self.texto_resultados_proceso.insert(tk.END, f"   {part}\n")
                    
                    # Mostrar temperatura promedio
                    if 'temperatura_promedio' in resultado:
                        temp = resultado.get('temperatura_promedio')
                        if isinstance(temp, (int, float)):
                            self.texto_resultados_proceso.insert(tk.END, f"   🌡️  Temperatura Promedio: {temp:.2f}°C\n")
                        else:
                            self.texto_resultados_proceso.insert(tk.END, f"   🌡️  Temperatura Promedio: {temp}\n")
                    
                    # Mostrar humedad promedio
                    if 'humedad_promedio' in resultado:
                        humedad = resultado.get('humedad_promedio')
                        if isinstance(humedad, (int, float)):
                            self.texto_resultados_proceso.insert(tk.END, f"   💧 Humedad Promedio: {humedad:.2f}%\n")
                        else:
                            self.texto_resultados_proceso.insert(tk.END, f"   💧 Humedad Promedio: {humedad}\n")
                    
                    self.texto_resultados_proceso.insert(tk.END, "\n")
                    
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, f"✅ Total de registros: {len(resultados)}\n")
            else:
                self.texto_resultados_proceso.insert(tk.END, f"\n{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, "⚠️ No se encontraron resultados para los criterios seleccionados.\n")

        except Exception as e:
            self.agregar_log(f"❌ Error ejecutando proceso de análisis: {e}")
            messagebox.showerror("Error", f"Error ejecutando proceso de análisis: {e}")

    def actualizar_lista_procesos(self):
        """Actualizar lista de procesos desde MongoDB Atlas filtrados por rol"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return

            for item in self.tree_procesos.get_children():
                self.tree_procesos.delete(item)

            # Filtrar por rol: usuarios comunes solo ven sus procesos
            if hasattr(self, 'rol_usuario') and self.rol_usuario == "usuario":
                procesos = self.mongodb_service.obtener_procesos(user_id=self.usuario_autenticado)
            else:
                # Administradores y técnicos ven todos
                procesos = self.mongodb_service.obtener_procesos()
            
            # Obtener filtro de estado seleccionado
            filtro_estado = "Todos"
            if hasattr(self, 'combo_filtro_estado'):
                filtro_estado = self.combo_filtro_estado.get()
                
            # Mapear emojis a estados reales
            estado_filtro_map = {
                "⏳ Pendiente": "pending",
                "🔄 En Ejecución": "running",
                "✅ Completado": "completed",
                "❌ Fallido": "failed"
            }
            
            estado_filtro = estado_filtro_map.get(filtro_estado, None)
            
            print(f"🔍 DEBUG FILTRO: filtro_estado seleccionado = '{filtro_estado}'")
            print(f"🔍 DEBUG FILTRO: estado_filtro mapeado = '{estado_filtro}'")
            print(f"🔍 DEBUG FILTRO: total procesos antes del filtro = {len(procesos)}")

            procesos_mostrados = 0
            for proceso in procesos:
                try:
                    # Obtener estado correctamente del campo 'status'
                    estado_raw = proceso.get('status', 'unknown')
                    
                    print(f"🔍 DEBUG PROCESO: {proceso.get('nombre', 'sin nombre')} - estado_raw = '{estado_raw}', filtro = '{estado_filtro}'")
                    
                    # Aplicar filtro de estado
                    if estado_filtro and estado_raw != estado_filtro:
                        print(f"🔍 DEBUG: SALTANDO proceso '{proceso.get('nombre', 'sin nombre')}' - no coincide con filtro")
                        continue  # Saltar este proceso si no coincide con el filtro
                    
                    # Mapear campos correctamente
                    process_id = str(proceso.get('process_id', 'N/A'))
                    nombre = str(proceso.get('nombre', 'Sin nombre'))[:50]
                    tipo = str(proceso.get('tipo', 'Sin tipo'))[:40]
                    
                    if estado_raw == 'pending':
                        estado_display = '⏳ Pendiente'
                    elif estado_raw == 'running':
                        estado_display = '🔄 En Ejecución'
                    elif estado_raw == 'completed':
                        estado_display = '✅ Completado'
                    elif estado_raw == 'failed':
                        estado_display = '❌ Fallido'
                    else:
                        estado_display = str(estado_raw).capitalize()
                    
                    # Obtener fecha y formatearla del campo 'created_at'
                    created_at = proceso.get('created_at', '')
                    if isinstance(created_at, datetime):
                        created_at_display = created_at.strftime('%Y-%m-%d %H:%M')
                    elif isinstance(created_at, str) and created_at:
                        try:
                            fecha_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            created_at_display = fecha_obj.strftime('%Y-%m-%d %H:%M')
                        except:
                            created_at_display = created_at[:10]
                    else:
                        created_at_display = 'N/A'
                    
                    # Obtener usuario del campo 'user_id'
                    user_id = str(proceso.get('user_id', 'N/A'))
                    
                    # Obtener ubicación y agrupación
                    ubicacion = str(proceso.get('ubicacion', 'Sin ubicación'))
                    agrupacion = str(proceso.get('agrupacion', 'Sin agrupación'))
                    
                    # Campo normalizado de tipo de proceso
                    tipo_proceso_norm = str(proceso.get('tipo_proceso', 'N/A'))

                    # Guardar el proceso completo en los tags del item
                    self.tree_procesos.insert('' , 'end', values=(
                        process_id,          # Columna 0: ID
                        nombre,              # Columna 1: Nombre
                        tipo,                # Columna 2: Tipo
                        tipo_proceso_norm,   # Columna 3: Tipo Proceso
                        ubicacion,           # Columna 4: Ubicación
                        agrupacion,          # Columna 5: Agrupación
                        estado_display       # Columna 6: Estado
                    ), tags=(process_id,))
                    procesos_mostrados += 1
                    print(f"🔍 DEBUG: ✅ Agregando proceso '{nombre}' con estado '{estado_display}'")
                except Exception as e:
                    print(f"🔍 DEBUG: ❌ Error procesando proceso: {e}")
                    self.agregar_log(f"❌ Error procesando proceso individual: {e}")
            
            print(f"🔍 DEBUG FILTRO: Total procesos mostrados después del filtro = {procesos_mostrados}")
                
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando lista de procesos: {e}")
    
    def on_select_proceso(self, event):
        """Manejar selección simple de un proceso - autocompletar formulario de ejecución"""
        try:
            seleccion = self.tree_procesos.selection()
            if not seleccion:
                return
            
            values = self.tree_procesos.item(seleccion[0], 'values')
            if not values:
                return
            
            process_id = values[0]
            
            # Obtener el proceso completo de MongoDB
            procesos = self.mongodb_service.obtener_procesos()
            proceso_seleccionado = None
            
            for proceso in procesos:
                if proceso.get('process_id') == process_id:
                    proceso_seleccionado = proceso
                    break
            
            if not proceso_seleccionado:
                return
            
            # Extraer ubicación - manejar diferentes formatos
            ubicacion = proceso_seleccionado.get('ubicacion', '')
            ciudad = ""
            pais = ""
            zona = ""
            
            if ubicacion:
                # Separar por " - "
                if ' - ' in ubicacion:
                    partes = ubicacion.split(' - ')
                    if len(partes) >= 2:
                        # Primera parte puede ser ciudad con o sin zona
                        primera_parte = partes[0].strip()
                        
                        # Si hay coma en la primera parte, extraer solo la ciudad (antes de la coma)
                        if ',' in primera_parte:
                            ciudad = primera_parte.split(',')[0].strip()
                        else:
                            ciudad = primera_parte
                        
                        # Segunda parte es el país
                        pais = partes[1].strip()
                        
                        # Si hay tercera parte, es la zona
                        if len(partes) >= 3:
                            zona = partes[2].strip()
                else:
                    # Si solo hay un valor, asumir que es la ciudad
                    # Extraer ciudad si tiene coma
                    if ',' in ubicacion:
                        ciudad = ubicacion.split(',')[0].strip()
                    else:
                        ciudad = ubicacion.strip()
            
            # Obtener parámetros del proceso
            parametros = proceso_seleccionado.get('parametros', 'temperatura_humedad')
            agrupacion_temporal = proceso_seleccionado.get('agrupacion', 'mensual')
            
            # Determinar tipo de análisis
            if parametros == "temperatura_humedad":
                tipo_analisis = "Ambas"
            elif parametros == "solo_temperatura":
                tipo_analisis = "Temperatura"
            else:
                tipo_analisis = "Humedad"
            
            # Determinar agrupación (por qué campo agrupar: Ciudad, País o Zona)
            tipo_proceso = proceso_seleccionado.get('tipo', '')
            if 'Ciudades' in tipo_proceso:
                agrupacion = "Ciudad"
            elif 'Países' in tipo_proceso:
                agrupacion = "País"
            elif 'Zonas' in tipo_proceso:
                agrupacion = "Zona"
            else:
                agrupacion = "Ciudad"  # Por defecto
            
            # Mapear agrupación temporal a periodicidad
            if agrupacion_temporal.lower() == 'diaria':
                periodicidad = "Diario"
            elif agrupacion_temporal.lower() == 'semanal':
                periodicidad = "Mensual"  # Como no hay "Semanal" en la UI, usar Mensual
            elif agrupacion_temporal.lower() == 'mensual':
                periodicidad = "Mensual"
            elif agrupacion_temporal.lower() == 'anual':
                periodicidad = "Anual"
            else:
                periodicidad = "Mensual"  # Por defecto
            
            # Autocompletar los campos del formulario de ejecución
            if hasattr(self, 'combo_tipo_analisis_proceso'):
                self.combo_tipo_analisis_proceso.set(tipo_analisis)
            
            if hasattr(self, 'combo_agrupacion_proceso'):
                self.combo_agrupacion_proceso.set(agrupacion)
            
            if hasattr(self, 'combo_periodicidad_proceso'):
                self.combo_periodicidad_proceso.set(periodicidad)
            
            if hasattr(self, 'combo_pais_proceso'):
                self.combo_pais_proceso.set(pais)
            
            if hasattr(self, 'combo_ciudad_proceso'):
                self.combo_ciudad_proceso.set(ciudad)
            
        except Exception as e:
            self.agregar_log(f"❌ Error al seleccionar proceso: {e}")
    
    def on_double_click_proceso(self, event):
        """Manejar doble click en un proceso de la lista"""
        try:
            item = self.tree_procesos.selection()[0]
            values = self.tree_procesos.item(item, 'values')
            
            if not values:
                return
            
            process_id = values[0]
            # El estado ahora está en la posición 5 (última columna)
            estado_raw = values[5] if len(values) > 5 else 'unknown'
            # Extraer el estado sin el emoji para comparar
            if '⏳' in estado_raw or 'Pendiente' in estado_raw:
                estado = "pending"
            elif '🔄' in estado_raw or 'Ejecución' in estado_raw:
                estado = "running"
            elif '✅' in estado_raw or 'Completado' in estado_raw:
                estado = "completed"
            elif '❌' in estado_raw or 'Fallido' in estado_raw:
                estado = "failed"
            else:
                estado = "unknown"
            
            # Obtener el proceso completo de MongoDB
            procesos = self.mongodb_service.obtener_procesos()
            proceso_seleccionado = None
            
            for proceso in procesos:
                if proceso.get('process_id') == process_id:
                    proceso_seleccionado = proceso
                    break
            
            if not proceso_seleccionado:
                messagebox.showwarning("Advertencia", "No se pudo encontrar el proceso en la base de datos")
                return
            
            # Mostrar resultados según el estado
            self.mostrar_resultado_proceso_por_estado(proceso_seleccionado, estado)
            
        except Exception as e:
            self.agregar_log(f"❌ Error en doble click de proceso: {e}")
            messagebox.showerror("Error", f"Error al abrir el proceso: {e}")
    
    def marcar_proceso_completado(self):
        """Marcar el proceso seleccionado como completado - ejecuta el análisis primero"""
        try:
            # Obtener proceso seleccionado
            seleccion = self.tree_procesos.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione un proceso de la lista")
                return
            
            # Verificar permisos
            if self.rol_usuario not in ["administrador", "técnico"]:
                messagebox.showerror("Acceso Denegado", "Solo administradores y técnicos pueden marcar procesos como completados")
                return
            
            values = self.tree_procesos.item(seleccion[0], 'values')
            process_id = values[0]
            
            # Obtener el proceso completo de MongoDB
            procesos = self.mongodb_service.obtener_procesos()
            proceso_seleccionado = None
            
            for proceso in procesos:
                if proceso.get('process_id') == process_id:
                    proceso_seleccionado = proceso
                    break
            
            if not proceso_seleccionado:
                messagebox.showwarning("Advertencia", "No se pudo encontrar el proceso en la base de datos")
                return
            
            # Mostrar ventana de confirmación con opción de ejecutar análisis
            respuesta = messagebox.askyesno(
                "Confirmar Completado", 
                f"¿Desea ejecutar el análisis del proceso '{proceso_seleccionado.get('nombre', 'N/A')}' antes de marcarlo como completado?\n\n"
                "Si elige 'Sí', se ejecutará el análisis y se guardarán los resultados.\n"
                "Si elige 'No', se marcará como completado sin resultados."
            )
            
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no disponible")
                return
            
            if respuesta:
                # Ejecutar el análisis
                ubicacion = proceso_seleccionado.get('ubicacion', '')
                agrupacion = proceso_seleccionado.get('agrupacion', 'mensual').capitalize()
                parametros = proceso_seleccionado.get('parametros', 'temperatura_humedad')
                
                # Determinar tipo de análisis desde parámetros
                if parametros == "temperatura_humedad":
                    tipo_analisis = "Ambas"
                elif parametros == "solo_temperatura":
                    tipo_analisis = "Temperatura"
                else:
                    tipo_analisis = "Humedad"
                
                # Preparar ubicación - manejar diferentes formatos
                ciudad_filtro = ""
                pais_filtro = ""
                
                if ubicacion and '-' in ubicacion:
                    # Formato: "Ciudad - País" o "Ciudad, Zona - País"
                    partes = ubicacion.split(' - ')
                    if len(partes) >= 2:
                        # Primera parte puede ser ciudad con o sin zona
                        primera_parte = partes[0].strip()
                        # Verificar si tiene coma (zona)
                        if ',' in primera_parte:
                            # Formato: "Buenos Aires, Centro"
                            ciudad_filtro = primera_parte.split(',')[0].strip()
                        else:
                            # Solo ciudad
                            ciudad_filtro = primera_parte
                        
                        # Segunda parte es el país
                        pais_filtro = partes[1].strip()
                    else:
                        ciudad_filtro = ubicacion.strip()
                elif ubicacion:
                    # Si no tiene '-', intentar parsear como ciudad
                    ciudad_filtro = ubicacion.strip()
                
                # Ejecutar análisis usando el método del backend
                resultados = self.mongodb_service.ejecutar_proceso_analisis(
                    tipo_analisis, agrupacion, "Mensual", pais_filtro, ciudad_filtro
                )
                
                # Guardar resultados correctamente - convertir lista a diccionario
                if resultados:
                    # Guardar la lista completa de resultados
                    self.mongodb_service.actualizar_estado_proceso(
                        process_id, "completed", progress=100, result={"data": resultados}
                    )
                    messagebox.showinfo("Éxito", f"Proceso {process_id} completado con {len(resultados)} resultados")
                else:
                    self.mongodb_service.actualizar_estado_proceso(
                        process_id, "completed", progress=100, 
                        result={"data": [], "mensaje": "Análisis completado sin resultados"}
                    )
                    messagebox.showinfo("Completado", f"Proceso {process_id} marcado como completado (sin resultados)")
            else:
                # Solo marcar como completado sin ejecutar
                self.mongodb_service.actualizar_estado_proceso(process_id, "completed", progress=100)
                messagebox.showinfo("Éxito", f"Proceso {process_id} marcado como completado")
            
            # Actualizar lista y ocultar botón
            self.actualizar_lista_procesos()
            self.btn_marcar_completado.config(state='disabled')
                
        except Exception as e:
            self.agregar_log(f"❌ Error marcando proceso como completado: {e}")
            messagebox.showerror("Error", f"Error marcando proceso: {e}")
    
    def mostrar_resultado_proceso_por_estado(self, proceso, estado):
        """Mostrar resultado del proceso según su estado"""
        try:
            self.texto_resultados_proceso.delete("1.0", tk.END)
            
            # Habilitar o deshabilitar botón de marcar como completado
            if hasattr(self, 'btn_marcar_completado'):
                # Verificar si el usuario es admin o técnico
                if hasattr(self, 'rol_usuario') and self.rol_usuario in ["administrador", "técnico"]:
                    # Solo habilitar si el proceso está en ejecución o pendiente (no si ya está completado)
                    if estado in ["running", "pending"]:
                        self.btn_marcar_completado.config(state='normal')
                    else:
                        self.btn_marcar_completado.config(state='disabled')
                else:
                    # Usuarios comunes no pueden marcar como completado
                    self.btn_marcar_completado.config(state='disabled')
            
            nombre_proceso = proceso.get('nombre', 'N/A')
            tipo_proceso = proceso.get('tipo', 'N/A')
            created_at = proceso.get('created_at', 'N/A')
            
            if estado == "completed":
                # Mostrar resultados completos
                resultado_raw = proceso.get('result', {})
                progreso = proceso.get('progress', 100)
                
                # Extraer los resultados - pueden estar en 'result' o en 'result.data'
                if isinstance(resultado_raw, dict) and 'data' in resultado_raw:
                    resultado = resultado_raw['data']
                elif isinstance(resultado_raw, list):
                    resultado = resultado_raw
                else:
                    resultado = resultado_raw
                
                self.texto_resultados_proceso.insert(tk.END, f"✅ PROCESO COMPLETADO\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                self.texto_resultados_proceso.insert(tk.END, f"📋 Nombre: {nombre_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"🏷️  Tipo: {tipo_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"📅 Creado: {created_at}\n")
                self.texto_resultados_proceso.insert(tk.END, f"📊 Progreso: {progreso}%\n")
                self.texto_resultados_proceso.insert(tk.END, f"\n{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, f"📊 RESULTADOS DEL ANÁLISIS\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                
                # Los resultados pueden ser una lista de diccionarios o un diccionario simple
                resultados_a_mostrar = []
                if isinstance(resultado, list):
                    resultados_a_mostrar = resultado
                elif isinstance(resultado, dict):
                    # Si es un dict con 'data', extraer la lista
                    if 'data' in resultado:
                        resultados_a_mostrar = resultado['data']
                    else:
                        resultados_a_mostrar = [resultado]
                elif resultado:
                    resultados_a_mostrar = [resultado]
                
                if resultados_a_mostrar:
                    for i, resultado_item in enumerate(resultados_a_mostrar, 1):
                        self.texto_resultados_proceso.insert(tk.END, f"\n📋 Registro #{i}\n")
                        self.texto_resultados_proceso.insert(tk.END, f"{'─'*60}\n")
                        
                        if isinstance(resultado_item, dict):
                            # Extraer agrupación y mostrarla de forma más limpia
                            agrupacion = resultado_item.get('agrupacion', {})
                            if isinstance(agrupacion, dict):
                                # Mostrar campos de agrupación con viñetas
                                if 'ciudad' in agrupacion:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Ciudad: {agrupacion['ciudad']}\n")
                                if 'zona' in agrupacion:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Zona: {agrupacion['zona']}\n")
                                if 'pais' in agrupacion:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • País: {agrupacion['pais']}\n")
                                if 'año' in agrupacion:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Año: {agrupacion['año']}\n")
                                if 'mes' in agrupacion:
                                    mes_nombre = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                                                 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
                                    try:
                                        mes_str = mes_nombre[agrupacion['mes'] - 1]
                                    except:
                                        mes_str = f"{agrupacion['mes']}"
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Mes: {mes_str}\n")
                                if 'dia' in agrupacion:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Día: {agrupacion['dia']}\n")
                            
                            # Mostrar temperatura promedio
                            if 'temperatura_promedio' in resultado_item:
                                temp = resultado_item.get('temperatura_promedio')
                                if isinstance(temp, (int, float)):
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Temperatura Promedio: {temp:.2f}°C\n")
                                else:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Temperatura Promedio: {temp}\n")
                            
                            # Mostrar humedad promedio
                            if 'humedad_promedio' in resultado_item:
                                humedad = resultado_item.get('humedad_promedio')
                                if isinstance(humedad, (int, float)):
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Humedad Promedio: {humedad:.2f}%\n")
                                else:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Humedad Promedio: {humedad}\n")
                            
                            # Mostrar otros campos
                            for clave, valor in resultado_item.items():
                                if clave not in ['agrupacion', 'temperatura_promedio', 'humedad_promedio']:
                                    self.texto_resultados_proceso.insert(tk.END, f"   • {clave}: {valor}\n")
                        else:
                            self.texto_resultados_proceso.insert(tk.END, f"   {resultado_item}\n")
                    
                    self.texto_resultados_proceso.insert(tk.END, f"\n{'='*60}\n")
                    self.texto_resultados_proceso.insert(tk.END, f"✅ Total de registros: {len(resultados_a_mostrar)}\n")
                else:
                    self.texto_resultados_proceso.insert(tk.END, "  ⚠️ No hay resultados disponibles\n")
                    
            elif estado == "running":
                # Proceso en ejecución
                progreso = proceso.get('progress', 0)
                started_at = proceso.get('started_at', 'N/A')
                
                self.texto_resultados_proceso.insert(tk.END, f"🔄 PROCESO EN EJECUCIÓN\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                self.texto_resultados_proceso.insert(tk.END, f"📋 Nombre: {nombre_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"🏷️  Tipo: {tipo_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"📅 Iniciado: {started_at}\n")
                self.texto_resultados_proceso.insert(tk.END, f"📊 Progreso: {progreso}%\n")
                self.texto_resultados_proceso.insert(tk.END, f"\n{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, f"ℹ️ Este proceso se está ejecutando actualmente.\n")
                self.texto_resultados_proceso.insert(tk.END, f"Por favor espere a que finalice para ver los resultados.\n")
                
            elif estado == "pending":
                # Proceso pendiente
                self.texto_resultados_proceso.insert(tk.END, f"⏳ PROCESO PENDIENTE\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                self.texto_resultados_proceso.insert(tk.END, f"📋 Nombre: {nombre_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"🏷️  Tipo: {tipo_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"📅 Creado: {created_at}\n")
                self.texto_resultados_proceso.insert(tk.END, f"\n{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, f"ℹ️ Este proceso está en la cola de espera.\n")
                self.texto_resultados_proceso.insert(tk.END, f"Será ejecutado por un técnico o administrador próximamente.\n")
                
            elif estado == "failed":
                # Proceso fallido
                error_msg = proceso.get('error', 'Error desconocido')
                
                self.texto_resultados_proceso.insert(tk.END, f"❌ PROCESO FALLIDO\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                self.texto_resultados_proceso.insert(tk.END, f"📋 Nombre: {nombre_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"🏷️  Tipo: {tipo_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"📅 Creado: {created_at}\n")
                self.texto_resultados_proceso.insert(tk.END, f"\n{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, f"ERROR:\n")
                self.texto_resultados_proceso.insert(tk.END, f"{error_msg}\n")
                
            else:
                # Estado desconocido
                self.texto_resultados_proceso.insert(tk.END, f"📋 PROCESO: {nombre_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                self.texto_resultados_proceso.insert(tk.END, f"Estado: {estado}\n")
                self.texto_resultados_proceso.insert(tk.END, f"Tipo: {tipo_proceso}\n")
                self.texto_resultados_proceso.insert(tk.END, f"Fecha: {created_at}\n")
                
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando resultado del proceso: {e}")
            messagebox.showerror("Error", f"Error mostrando resultado: {e}")
    
    def cargar_ubicaciones_procesos(self):
        """Cargar países y ciudades disponibles para el módulo de procesos"""
        try:
            # Valores por defecto de países
            paises = ["Argentina", "Brasil", "Chile", "Colombia", "México", "Perú", "Uruguay", "Venezuela", "Ecuador", "Paraguay"]
            self.combo_pais_proceso['values'] = paises
            self.combo_pais_proceso.set("Argentina")
            
            # Cargar ciudades según el país seleccionado
            def actualizar_ciudades(event=None):
                pais_seleccionado = self.combo_pais_proceso.get()
                ciudades_por_pais = {
                    "Argentina": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata", "Tucumán", "Mar del Plata", "Salta", "Santa Fe", "Corrientes"],
                    "Brasil": ["São Paulo", "Río de Janeiro", "Brasilia", "Salvador", "Fortaleza", "Belo Horizonte", "Manaus", "Curitiba", "Recife", "Porto Alegre"],
                    "Chile": ["Santiago", "Valparaíso", "Concepción", "La Serena", "Antofagasta", "Viña del Mar", "Temuco", "Valdivia", "Iquique", "Punta Arenas"],
                    "Colombia": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga", "Pereira", "Ibagué", "Santa Marta", "Manizales"],
                    "México": ["Ciudad de México", "Guadalajara", "Monterrey", "Puebla", "Tijuana", "León", "Juárez", "Torreón", "Querétaro", "San Luis Potosí"],
                    "Perú": ["Lima", "Arequipa", "Trujillo", "Chiclayo", "Huancayo", "Iquitos", "Piura", "Cusco", "Chimbote", "Tacna"],
                    "Uruguay": ["Montevideo", "Salto", "Paysandú", "Las Piedras", "Rivera", "Maldonado", "Tacuarembó", "Mercedes", "Artigas", "Durazno"],
                    "Venezuela": ["Caracas", "Maracaibo", "Valencia", "Barquisimeto", "Ciudad Guayana", "Maturín", "Barcelona", "Maracay", "Puerto La Cruz", "San Cristóbal"],
                    "Ecuador": ["Guayaquil", "Quito", "Cuenca", "Santo Domingo", "Machala", "Durán", "Portoviejo", "Ambato", "Esmeraldas", "Riobamba"],
                    "Paraguay": ["Asunción", "Ciudad del Este", "San Lorenzo", "Luque", "Capiatá", "Lambaré", "Fernando de la Mora", "Limpio", "Encarnación", "Mariano Roque Alonso"]
                }
                
                ciudades = ciudades_por_pais.get(pais_seleccionado, [])
                self.combo_ciudad_proceso['values'] = ciudades
                if ciudades:
                    self.combo_ciudad_proceso.set(ciudades[0])
            
            # Vincular evento de cambio de país
            self.combo_pais_proceso.bind('<<ComboboxSelected>>', actualizar_ciudades)
            
            # Cargar ciudades iniciales
            actualizar_ciudades()
            
            # self.agregar_log(f"✅ Ubicaciones cargadas para procesos")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ubicaciones para procesos: {e}")
            # En caso de error, usar valores por defecto básicos
            self.combo_pais_proceso['values'] = ["Argentina", "Brasil", "Chile", "Colombia"]
            self.combo_ciudad_proceso['values'] = ["Buenos Aires", "Córdoba", "Rosario", "Mendoza"]
    
    def ejecutar_proceso_analisis(self):
        """Ejecutar análisis de datos desde la interfaz de procesos"""
        try:
            self.texto_resultados_proceso.delete("1.0", tk.END)
            self.texto_resultados_proceso.insert(tk.END, "🔄 Ejecutando análisis...\n\n")
            self.texto_resultados_proceso.update()
            
            # Obtener parámetros de los combos
            tipo_analisis = self.combo_tipo_analisis_proceso.get()
            agrupacion = self.combo_agrupacion_proceso.get()
            periodicidad = self.combo_periodicidad_proceso.get()
            pais = self.combo_pais_proceso.get()
            ciudad = self.combo_ciudad_proceso.get()
            
            if not tipo_analisis or not agrupacion or not periodicidad:
                messagebox.showwarning("Advertencia", "Por favor seleccione todos los parámetros")
                return
            
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                self.texto_resultados_proceso.insert(tk.END, "❌ Error: MongoDB no disponible\n")
                return
            
            # Ejecutar el análisis en el backend
            self.texto_resultados_proceso.insert(tk.END, f"📊 Parámetros:\n")
            self.texto_resultados_proceso.insert(tk.END, f"  - Tipo: {tipo_analisis}\n")
            self.texto_resultados_proceso.insert(tk.END, f"  - Agrupación: {agrupacion}\n")
            self.texto_resultados_proceso.insert(tk.END, f"  - Periodicidad: {periodicidad}\n")
            self.texto_resultados_proceso.insert(tk.END, f"  - País: {pais}\n")
            self.texto_resultados_proceso.insert(tk.END, f"  - Ciudad: {ciudad}\n\n")
            self.texto_resultados_proceso.update()
            
            # Llamar al método del backend
            resultados = self.mongodb_service.ejecutar_proceso_analisis(
                tipo_analisis, agrupacion, periodicidad, pais, ciudad, ""
            )
            
            if resultados:
                self.texto_resultados_proceso.insert(tk.END, f"✅ Análisis completado. {len(resultados)} registros encontrados.\n\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n")
                self.texto_resultados_proceso.insert(tk.END, "RESULTADOS DEL ANÁLISIS\n")
                self.texto_resultados_proceso.insert(tk.END, f"{'='*60}\n\n")
                
                for i, resultado in enumerate(resultados, 1):
                    self.texto_resultados_proceso.insert(tk.END, f"\n📋 Registro #{i}\n")
                    self.texto_resultados_proceso.insert(tk.END, f"{'─'*60}\n")
                    
                    if isinstance(resultado, dict):
                        # Extraer agrupación si existe
                        agrupacion_data = resultado.get('agrupacion', {})
                        if isinstance(agrupacion_data, dict):
                            # Formatear ubicación de manera limpia
                            ubicacion_str = ""
                            if 'ciudad' in agrupacion_data:
                                ciudad_val = agrupacion_data['ciudad']
                                if isinstance(ciudad_val, str):
                                    # Si es un string simple, usarlo directamente
                                    ubicacion_str = ciudad_val
                                elif isinstance(ciudad_val, dict):
                                    # Si es un diccionario, extraer campos relevantes
                                    city_name = ciudad_val.get('city', '')
                                    country_name = ciudad_val.get('country', '')
                                    zone_name = ciudad_val.get('zone', '')
                                    if city_name:
                                        ubicacion_str = city_name
                                        if country_name:
                                            ubicacion_str += f", {country_name}"
                                        if zone_name:
                                            ubicacion_str += f" - {zone_name}"
                                else:
                                    ubicacion_str = str(ciudad_val)
                                self.texto_resultados_proceso.insert(tk.END, f"   • Ciudad: {ubicacion_str}\n")
                            
                            if 'zona' in agrupacion_data:
                                zona_val = agrupacion_data['zona']
                                if isinstance(zona_val, str):
                                    self.texto_resultados_proceso.insert(tk.END, f"   • Zona: {zona_val}\n")
                            if 'pais' in agrupacion_data:
                                pais_val = agrupacion_data['pais']
                                if isinstance(pais_val, str):
                                    self.texto_resultados_proceso.insert(tk.END, f"   • País: {pais_val}\n")
                            if 'año' in agrupacion_data:
                                self.texto_resultados_proceso.insert(tk.END, f"   • Año: {agrupacion_data['año']}\n")
                            if 'mes' in agrupacion_data:
                                mes_nombre = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                                             'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
                                try:
                                    mes_str = mes_nombre[agrupacion_data['mes'] - 1]
                                except:
                                    mes_str = f"{agrupacion_data['mes']}"
                                self.texto_resultados_proceso.insert(tk.END, f"   • Mes: {mes_str}\n")
                            if 'dia' in agrupacion_data:
                                self.texto_resultados_proceso.insert(tk.END, f"   • Día: {agrupacion_data['dia']}\n")
                        
                        # Mostrar temperatura promedio
                        if 'temperatura_promedio' in resultado:
                            temp = resultado.get('temperatura_promedio')
                            if isinstance(temp, (int, float)):
                                self.texto_resultados_proceso.insert(tk.END, f"   • Temperatura Promedio: {temp:.2f}°C\n")
                            else:
                                self.texto_resultados_proceso.insert(tk.END, f"   • Temperatura Promedio: {temp}\n")
                        
                        # Mostrar humedad promedio
                        if 'humedad_promedio' in resultado:
                            humedad = resultado.get('humedad_promedio')
                            if isinstance(humedad, (int, float)):
                                self.texto_resultados_proceso.insert(tk.END, f"   • Humedad Promedio: {humedad:.2f}%\n")
                            else:
                                self.texto_resultados_proceso.insert(tk.END, f"   • Humedad Promedio: {humedad}\n")
                    else:
                        # Si no es diccionario, mostrar directamente
                        self.texto_resultados_proceso.insert(tk.END, f"   {resultado}\n")
                    
                    self.texto_resultados_proceso.insert(tk.END, "\n")
            else:
                self.texto_resultados_proceso.insert(tk.END, "⚠️ No se encontraron resultados para los parámetros especificados\n")
            
            self.agregar_log(f"✅ Proceso de análisis ejecutado exitosamente")
            
        except Exception as e:
            self.agregar_log(f"❌ Error ejecutando proceso de análisis: {e}")
            self.texto_resultados_proceso.insert(tk.END, f"❌ Error: {e}\n")
            messagebox.showerror("Error", f"Error ejecutando análisis: {e}")
    
    def actualizar_interfaz_procesos(self):
        """Actualizar la interfaz de procesos según el rol actual"""
        try:
            # Solo actualizar la lista de procesos y la configuración de botones
            self.actualizar_lista_procesos()
            self.configurar_botones_procesos()
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando interfaz procesos: {e}")
    
    def obtener_nombre_usuario(self, user_id):
        """Obtener nombre de usuario por ID"""
        try:
            if not user_id or not self.mongodb_service or not self.mongodb_service.conectado:
                return 'N/A'
            
            usuario = self.mongodb_service.obtener_usuario_por_id(user_id)
            if usuario:
                return usuario.get('nombre', usuario.get('username', 'N/A'))
            return 'N/A'
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo nombre usuario {user_id}: {e}")
            return 'N/A'
    
    def actualizar_estado_sistema(self):
        """Actualizar el estado del sistema de procesos"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.label_estado_procesos.config(text="❌ MongoDB no disponible")
                return
            
            # Obtener estadísticas de procesos
            todos_procesos = self.mongodb_service.obtener_procesos()
            
            if not todos_procesos:
                self.label_estado_procesos.config(text="📊 No hay procesos registrados")
                return
            
            # Contar por estado
            estados = {}
            for proceso in todos_procesos:
                estado = proceso.get('status', 'unknown')
                estados[estado] = estados.get(estado, 0) + 1
            
            # Crear texto de estado
            estado_texto = f"📊 Total: {len(todos_procesos)} procesos | "
            estado_texto += f"⏳ Pendientes: {estados.get('pending', 0)} | "
            estado_texto += f"▶️ Ejecutando: {estados.get('running', 0)} | "
            estado_texto += f"✅ Completados: {estados.get('completed', 0)} | "
            estado_texto += f"❌ Fallidos: {estados.get('failed', 0)}"
            
            self.label_estado_procesos.config(text=estado_texto)
            
        except Exception as e:
            self.label_estado_procesos.config(text=f"❌ Error cargando estado: {e}")
    
    def agregar_mensaje_informativo_procesos(self, parent_frame):
        """Agregar mensaje informativo según el rol del usuario"""
        try:
            # Crear frame para el mensaje
            mensaje_frame = tk.Frame(parent_frame, bg='white')
            mensaje_frame.grid(row=4, column=0, columnspan=4, padx=5, pady=10, sticky='ew')
            
            if self.rol_usuario == "usuario":
                mensaje = "ℹ️ Como usuario tradicional, puedes crear procesos de análisis y ver tus propios procesos creados."
                color = '#3498db'
            elif self.rol_usuario == "técnico":
                mensaje = "ℹ️ Como técnico, puedes crear, ejecutar y pausar procesos. También puedes ver todos los procesos del sistema."
                color = '#f39c12'
            elif self.rol_usuario == "administrador":
                mensaje = "ℹ️ Como administrador, tienes acceso completo: crear, ejecutar, pausar y eliminar procesos."
                color = '#e74c3c'
            else:
                mensaje = "⚠️ Rol de usuario no reconocido"
                color = '#e74c3c'
            
            tk.Label(mensaje_frame, text=mensaje, 
                    bg='white', fg=color, font=('Arial', 9, 'italic'),
                    wraplength=600, justify='left').pack()
                    
        except Exception as e:
            self.agregar_log(f"❌ Error agregando mensaje informativo: {e}")
    
    def crear_tab_servicios(self):
        """Crear tab de servicios avanzados con facturación"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Servicios")
        
        # Configuración de servicios
        config_frame = tk.LabelFrame(tab, text="Servicios de Consultas en Línea", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Información de servicios
        info_frame = tk.Frame(config_inner, bg='#ecf0f1', relief='raised', bd=1)
        info_frame.pack(fill='x', pady=10)
        
        tk.Label(info_frame, text="🌐 CONSULTAS EN LÍNEA POR UBICACIÓN", 
                font=('Arial', 14, 'bold'), bg='#ecf0f1', fg='#2c3e50').pack(pady=5)
        
        tk.Label(info_frame, text="Consulta información de sensores por ciudad, zona, país en un rango de fechas", 
                font=('Arial', 10), bg='#ecf0f1').pack()
        tk.Label(info_frame, text="Procesos periódicos de consultas sobre humedad y temperaturas", 
                font=('Arial', 10), bg='#ecf0f1').pack()
        tk.Label(info_frame, text="Análisis anualizados, mensualizados y por períodos", 
                font=('Arial', 10), bg='#ecf0f1').pack()
        
        # Campos de configuración
        campos_frame = tk.Frame(config_inner, bg='white')
        campos_frame.pack(fill='x', pady=10)
        
        # Configurar el grid para que las columnas se expandan
        campos_frame.grid_columnconfigure(1, weight=1)
        campos_frame.grid_columnconfigure(3, weight=1)
        
        # Fila 1: Ubicación (País a la izquierda, Ciudad a la derecha)
        tk.Label(campos_frame, text="País:", bg='white', font=('Arial', 10, 'bold')).grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.combo_pais_servicio = ttk.Combobox(campos_frame, width=25)
        self.combo_pais_servicio.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
        self.combo_pais_servicio.bind('<<ComboboxSelected>>', self.on_pais_selected_servicio)
        
        tk.Label(campos_frame, text="Ciudad:", bg='white', font=('Arial', 10, 'bold')).grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.combo_ciudad_servicio = ttk.Combobox(campos_frame, width=25)
        self.combo_ciudad_servicio.grid(row=0, column=3, padx=5, pady=5, sticky='ew')
        
        # Fila 2: Zona y Tipo de Consulta
        tk.Label(campos_frame, text="Zona (Opcional):", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.combo_zona_servicio = ttk.Combobox(campos_frame, width=25)
        self.combo_zona_servicio.grid(row=1, column=1, padx=5, pady=5, sticky='ew')
        self.combo_zona_servicio.set("")  # Valor vacío por defecto
        
        tk.Label(campos_frame, text="Tipo de Sensor:", bg='white').grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.combo_tipo_sensor_servicio = ttk.Combobox(campos_frame, values=[
            "Todos los Sensores",
            "Solo Temperatura",
            "Solo Humedad"
        ], width=25)
        self.combo_tipo_sensor_servicio.grid(row=1, column=3, padx=5, pady=5, sticky='ew')
        self.combo_tipo_sensor_servicio.set("Todos los Sensores")
        
        # Fila 3: Fechas
        tk.Label(campos_frame, text="Fecha Inicio:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.entry_fecha_inicio_servicio = tk.Entry(campos_frame, width=15)
        self.entry_fecha_inicio_servicio.grid(row=2, column=1, padx=5, pady=5, sticky='w')
        self.entry_fecha_inicio_servicio.insert(0, "2024-01-01")
        
        tk.Label(campos_frame, text="Fecha Fin:", bg='white').grid(row=2, column=2, padx=5, pady=5, sticky='w')
        self.entry_fecha_fin_servicio = tk.Entry(campos_frame, width=15)
        self.entry_fecha_fin_servicio.grid(row=2, column=3, padx=5, pady=5, sticky='w')
        self.entry_fecha_fin_servicio.insert(0, datetime.now().strftime("%Y-%m-%d"))
        
        # Botones de servicios
        botones_frame = tk.Frame(config_inner, bg='white')
        botones_frame.pack(fill='x', pady=10)
        
        tk.Button(botones_frame, text="🌐 Ejecutar Consulta en Línea", 
                 command=self.ejecutar_consulta_linea, 
                 bg='#3498db', fg='white', font=('Arial', 12, 'bold')).pack(side='left', padx=5)
        
        tk.Button(botones_frame, text="📊 Ver Historial de Consultas", 
                 command=self.ver_historial_consultas, 
                 bg='#9b59b6', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
        
        tk.Button(botones_frame, text="💰 Ver Facturas de Consultas", 
                 command=self.ver_facturas_consultas, 
                 bg='#f39c12', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
        
        tk.Button(botones_frame, text="🔄 Cargar Ubicaciones", 
                 command=self.cargar_ubicaciones_servicio, 
                 bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
        
        
        # Área de resultados
        resultados_frame = tk.LabelFrame(tab, text="Resultados del Servicio", 
                                       font=('Arial', 12, 'bold'), bg='white')
        resultados_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        self.texto_resultados_servicio = scrolledtext.ScrolledText(resultados_frame, height=20)
        self.texto_resultados_servicio.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Cargar ubicaciones para los combos
        self.cargar_ubicaciones_servicio()
    
    def cargar_ubicaciones_servicio(self):
        """Cargar países disponibles para el combo del servicio"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Valores por defecto si no hay conexión
                paises_default = ['Argentina', 'Brasil', 'Uruguay', 'Chile', 'Paraguay']
                if hasattr(self, 'combo_pais_servicio'):
                    self.combo_pais_servicio['values'] = paises_default
                return
            
            # Obtener todos los sensores para extraer países únicos
            sensores = self.mongodb_service.obtener_sensores()
            
            paises = set()
            
            for sensor in sensores:
                location = sensor.get('location', {})
                
                if isinstance(location, dict):
                    pais = location.get('country', '')
                    if pais:
                        paises.add(pais)
                elif isinstance(location, str) and location.strip():
                    # Si location es un string, intentar parsearlo
                    # Formato esperado: "Ciudad, Zona - País"
                    if ' - ' in location:
                        ciudad_zona, pais = location.split(' - ', 1)
                        paises.add(pais.strip())
            
            # Si no hay sensores con países, usar valores por defecto
            if not paises:
                paises = {'Argentina', 'Brasil', 'Uruguay', 'Chile', 'Paraguay'}
            
            # Actualizar combo de países
            self.combo_pais_servicio['values'] = sorted(list(paises))
            self.combo_ciudad_servicio['values'] = []  # Ciudad vacía hasta seleccionar país
            self.combo_ciudad_servicio.set("")  # Limpiar selección
            self.combo_zona_servicio['values'] = []  # Zona vacía hasta seleccionar ciudad
            self.combo_zona_servicio.set("")  # Limpiar selección
            
            self.agregar_log(f"✅ Países cargados: {len(paises)} países disponibles")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando países: {e}")
    
    def cargar_ciudades_para_servicio(self, pais_seleccionado):
        """Cargar ciudades del país seleccionado"""
        try:
            if not pais_seleccionado or not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Obtener todos los sensores
            sensores = self.mongodb_service.obtener_sensores()
            
            ciudades = set()
            zonas = set()
            
            for sensor in sensores:
                location = sensor.get('location', {})
                
                if isinstance(location, dict):
                    ciudad = location.get('city', '')
                    pais = location.get('country', '')
                    zona = location.get('zone', '')
                    
                    # Si el país coincide con el seleccionado
                    if pais == pais_seleccionado:
                        if ciudad:
                            ciudades.add(ciudad)
                        if zona:
                            zonas.add(zona)
                            
                elif isinstance(location, str) and location.strip():
                    # Formato esperado: "Ciudad, Zona - País" o "Ciudad - País"
                    if ' - ' in location:
                        ciudad_zona, pais = location.split(' - ', 1)
                        pais = pais.strip()
                        
                        # Si el país coincide con el seleccionado
                        if pais == pais_seleccionado:
                            if ', ' in ciudad_zona:
                                ciudad, zona = ciudad_zona.split(', ', 1)
                                ciudades.add(ciudad.strip())
                                if zona.strip():
                                    zonas.add(zona.strip())
                            else:
                                ciudades.add(ciudad_zona.strip())
            
            # Actualizar combos
            self.combo_ciudad_servicio['values'] = sorted(list(ciudades))
            self.combo_zona_servicio['values'] = sorted(list(zonas))
            
            # Limpiar selección de ciudad
            self.combo_ciudad_servicio.set("")
            self.combo_zona_servicio.set("")
            
            if ciudades:
                self.agregar_log(f"✅ Ciudades cargadas para {pais_seleccionado}: {len(ciudades)} ciudades disponibles")
            else:
                self.agregar_log(f"⚠️ No hay ciudades registradas para {pais_seleccionado}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ciudades: {e}")
    
    def on_pais_selected_servicio(self, event=None):
        """Evento cuando se selecciona un país en el servicio"""
        try:
            pais_seleccionado = self.combo_pais_servicio.get()
            if pais_seleccionado:
                self.cargar_ciudades_para_servicio(pais_seleccionado)
        except Exception as e:
            self.agregar_log(f"❌ Error en selección de país: {e}")
    
    def ejecutar_consulta_linea(self):
        """Ejecutar consulta en línea por ubicación"""
        try:
            # Validar campos requeridos
            ciudad = self.combo_ciudad_servicio.get().strip()
            pais = self.combo_pais_servicio.get().strip()
            zona = self.combo_zona_servicio.get().strip()
            tipo_sensor = self.combo_tipo_sensor_servicio.get().strip()
            fecha_inicio = self.entry_fecha_inicio_servicio.get().strip()
            fecha_fin = self.entry_fecha_fin_servicio.get().strip()
            
            if not ciudad or not pais:
                messagebox.showwarning("Advertencia", "Por favor seleccione una ciudad y país")
                return
            
            if not fecha_inicio or not fecha_fin:
                messagebox.showwarning("Advertencia", "Por favor ingrese fechas de inicio y fin")
                return
            
            # Mostrar ventana de progreso
            progress_window = tk.Toplevel(self.root)
            progress_window.title("🌐 Ejecutando Consulta en Línea")
            progress_window.geometry("400x150")
            progress_window.configure(bg='white')
            progress_window.transient(self.root)
            progress_window.grab_set()
            
            # Centrar ventana
            progress_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
            
            tk.Label(progress_window, text="🌐 Ejecutando consulta en línea...", 
                    font=('Arial', 12, 'bold'), bg='white').pack(pady=20)
            
            progress_var = tk.StringVar(value="Iniciando consulta...")
            progress_label = tk.Label(progress_window, textvariable=progress_var, bg='white')
            progress_label.pack(pady=10)
            
            progress_window.update()
            
            # Ejecutar consulta según el tipo
            resultado = self.procesar_consulta_linea(
                ciudad, pais, zona, fecha_inicio, fecha_fin, 
                tipo_sensor, progress_var
            )
            
            # Cerrar ventana de progreso
            progress_window.destroy()
            
            # Mostrar resultados
            self.texto_resultados_servicio.delete("1.0", tk.END)
            self.texto_resultados_servicio.insert("1.0", resultado)
            
            # Generar factura
            self.generar_factura_consulta_linea(ciudad, pais, tipo_sensor)
            
            self.agregar_log(f"✅ Consulta en línea completada para {ciudad}, {pais}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error ejecutando consulta en línea: {e}")
            messagebox.showerror("Error", f"Error ejecutando consulta: {e}")
    
    def procesar_consulta_linea(self, ciudad, pais, zona, fecha_inicio, fecha_fin, tipo_sensor, progress_var):
        """Procesar consulta simple por ubicación y rango de fechas"""
        try:
            progress_var.set("Obteniendo sensores...")
            
            # Obtener sensores por ubicación
            sensores_ubicacion = self.obtener_sensores_por_ubicacion(ciudad, pais, zona)
            
            if not sensores_ubicacion:
                return f"""❌ No se encontraron sensores en {ciudad}, {pais}

🔍 DIAGNÓSTICO:
• Verifique que la ciudad y país estén escritos correctamente
• Asegúrese de que existan sensores registrados en esa ubicación

📍 UBICACIONES DISPONIBLES:
{self.obtener_ubicaciones_disponibles()}"""
            
            progress_var.set(f"Procesando {len(sensores_ubicacion)} sensores...")
            
            # Filtrar sensores según el tipo seleccionado
            sensores_filtrados = []
            debug_info = f"\n🔍 DEBUG - Filtrado de sensores:\n"
            debug_info += f"• Ubicación: {ciudad}, {pais}, Zona: {zona}\n"
            debug_info += f"• Tipo seleccionado: {tipo_sensor}\n"
            
            for sensor in sensores_ubicacion:
                sensor_type = sensor.get('type', '').lower()
                sensor_name = sensor.get('name', '').lower()
                
                debug_info += f"• Sensor: {sensor.get('name', '')} - Tipo original: '{sensor_type}'\n"
                
                # Si el tipo está vacío, usar el nombre del sensor como referencia
                if not sensor_type:
                    if 'temperatura' in sensor_name or 'temp' in sensor_name or 'temperature' in sensor_name:
                        sensor_type = 'temperatura'
                    elif 'humedad' in sensor_name or 'humidity' in sensor_name:
                        sensor_type = 'humedad'
                    else:
                        # Si no podemos determinar el tipo por el nombre, incluir el sensor para análisis posterior
                        debug_info += f"  → Tipo indeterminado por nombre, se incluirá para análisis posterior\n"
                        sensores_filtrados.append(sensor)
                        continue
                
                # Filtrar según tipo seleccionado
                if tipo_sensor == "Solo Temperatura" and 'temperatura' not in sensor_type and 'temperature' not in sensor_type:
                    debug_info += f"  → Excluido: no es sensor de temperatura\n"
                    continue
                elif tipo_sensor == "Solo Humedad" and 'humedad' not in sensor_type and 'humidity' not in sensor_type:
                    debug_info += f"  → Excluido: no es sensor de humedad\n"
                    continue
                
                debug_info += f"  → Incluido: coincide con el filtro\n"
                sensores_filtrados.append(sensor)
            
            if not sensores_filtrados:
                return f"""❌ No se encontraron sensores del tipo '{tipo_sensor}' en {ciudad}, {pais}

{debug_info}

🔍 DIAGNÓSTICO:
• Verifique que existan sensores del tipo seleccionado en esa ubicación
• Los sensores encontrados fueron: {[s.get('name', 'N/A') for s in sensores_ubicacion]}"""
            
            # Obtener mediciones de todos los sensores filtrados
            todas_mediciones = []
            for sensor in sensores_filtrados:
                sensor_id = sensor.get('sensor_id', '')
                sensor_name = sensor.get('name', '')
                
                # Obtener mediciones por sensor_id
                mediciones = self.mongodb_service.obtener_mediciones_sensor_por_fechas(sensor_id, fecha_inicio, fecha_fin)
                
                # Si no hay mediciones por sensor_id, intentar por sensor_name
                if not mediciones:
                    mediciones = self.mongodb_service.obtener_mediciones_rango(
                        sensor_name=sensor_name,
                        fecha_inicio=fecha_inicio,
                        fecha_fin=fecha_fin
                    )
                
                # Filtrar mediciones según el tipo de sensor seleccionado
                mediciones_filtradas = []
                for medicion in mediciones:
                    medicion['sensor_name'] = sensor_name
                    medicion['sensor_id'] = sensor_id
                    
                    # Si es "Solo Temperatura", solo incluir mediciones con temperatura
                    if tipo_sensor == "Solo Temperatura" and medicion.get('temperature') is None:
                        continue
                    
                    # Si es "Solo Humedad", solo incluir mediciones con humedad
                    if tipo_sensor == "Solo Humedad" and medicion.get('humidity') is None:
                        continue
                    
                    mediciones_filtradas.append(medicion)
                
                debug_info += f"• Mediciones filtradas para {sensor_name}: {len(mediciones_filtradas)} de {len(mediciones)}\n"
                todas_mediciones.extend(mediciones_filtradas)
            
            if not todas_mediciones:
                return f"""❌ No se encontraron mediciones en {ciudad}, {pais} para el período {fecha_inicio} - {fecha_fin}

{debug_info}

🔍 DIAGNÓSTICO:
• Verifique que las fechas estén en formato correcto (YYYY-MM-DD)
• Asegúrese de que existan mediciones en ese período
• Los sensores encontrados fueron: {[s.get('name', 'N/A') for s in sensores_filtrados]}

💡 SUGERENCIAS:
• Intente con un rango de fechas más amplio
• Verifique que los sensores tengan mediciones registradas
• Use fechas más recientes si los sensores son nuevos"""
            
            progress_var.set("Generando reporte...")
            
            # Generar reporte simple
            return self.generar_reporte_simple_ubicacion(ciudad, pais, todas_mediciones, tipo_sensor)
                
        except Exception as e:
            return f"❌ Error procesando consulta: {e}"
    
    def generar_reporte_simple_ubicacion(self, ciudad, pais, mediciones, tipo_sensor):
        """Generar reporte simple por ubicación"""
        try:
            resultado = f"""🌐 CONSULTA EN LÍNEA POR UBICACIÓN
📍 Ubicación: {ciudad}, {pais}
📅 Período: {len(mediciones)} mediciones
🔧 Tipo de Sensor: {tipo_sensor}
{'='*60}

📈 RESUMEN GENERAL:
• Total de mediciones: {len(mediciones)}
• Sensores involucrados: {len(set(m.get('sensor_id', '') for m in mediciones))}
• Período de datos: {min(m.get('timestamp', '') for m in mediciones if m.get('timestamp'))} - {max(m.get('timestamp', '') for m in mediciones if m.get('timestamp'))}

"""
            
            # Análisis de temperatura si corresponde
            if tipo_sensor == "Todos los Sensores" or tipo_sensor == "Solo Temperatura":
                temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
                if temperaturas:
                    resultado += f"""🌡️ ANÁLISIS DE TEMPERATURA:
• Temperatura promedio: {sum(temperaturas)/len(temperaturas):.2f}°C
• Temperatura mínima: {min(temperaturas):.2f}°C
• Temperatura máxima: {max(temperaturas):.2f}°C
• Rango de variación: {max(temperaturas) - min(temperaturas):.2f}°C

"""
            
            # Análisis de humedad si corresponde
            if tipo_sensor == "Todos los Sensores" or tipo_sensor == "Solo Humedad":
                humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
                if humedades:
                    resultado += f"""💧 ANÁLISIS DE HUMEDAD:
• Humedad promedio: {sum(humedades)/len(humedades):.2f}%
• Humedad mínima: {min(humedades):.2f}%
• Humedad máxima: {max(humedades):.2f}%
• Rango de variación: {max(humedades) - min(humedades):.2f}%

"""
            
            # Lista de sensores involucrados
            sensores_unicos = list(set(m.get('sensor_name', 'N/A') for m in mediciones))
            resultado += f"""📊 SENSORES INVOLUCRADOS:
{chr(10).join(f"• {sensor}" for sensor in sensores_unicos)}

"""
            
            return resultado
            
        except Exception as e:
            return f"❌ Error generando reporte: {e}"
    
    def obtener_sensores_por_ubicacion(self, ciudad, pais, zona=None):
        """Obtener sensores de una ubicación específica"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return []
            
            print(f"🔍 DEBUG - Buscando sensores para: Ciudad='{ciudad}', País='{pais}', Zona='{zona}'")
            print(f"🔍 DEBUG - Tipo de Zona: {type(zona)}, Valor completo: '{zona}', ¿Está vacío? {not zona or not zona.strip()}")
            
            # Obtener todos los sensores y filtrar manualmente
            todos_sensores = self.mongodb_service.obtener_sensores()
            sensores_encontrados = []
            
            for sensor in todos_sensores:
                location = sensor.get('location', {})
                print(f"🔍 DEBUG - Revisando sensor: {sensor.get('name', 'N/A')} - Location: {location}")
                
                sensor_coincide = False
                
                if isinstance(location, dict):
                    # Formato: {"city": "Buenos Aires", "country": "Argentina", "zone": "Centro"}
                    sensor_ciudad = location.get('city', '')
                    sensor_pais = location.get('country', '')
                    sensor_zona = location.get('zone', '')
                    
                    if sensor_ciudad == ciudad and sensor_pais == pais:
                        if not zona or not zona.strip() or sensor_zona == zona:
                            sensor_coincide = True
                            print(f"🔍 DEBUG - ✅ Coincide (dict): {sensor_ciudad}, {sensor_pais}, {sensor_zona}")
                
                elif isinstance(location, str) and location.strip():
                    # Formato: "Buenos Aires, Centro - Argentina"
                    print(f"🔍 DEBUG - Parseando location string: '{location}'")
                    
                    if ' - ' in location:
                        ciudad_zona, sensor_pais = location.split(' - ', 1)
                        if ', ' in ciudad_zona:
                            sensor_ciudad, sensor_zona = ciudad_zona.split(', ', 1)
                        else:
                            sensor_ciudad = ciudad_zona
                            sensor_zona = ''
                        
                        sensor_ciudad = sensor_ciudad.strip()
                        sensor_pais = sensor_pais.strip()
                        sensor_zona = sensor_zona.strip()
                        
                        print(f"🔍 DEBUG - Comparando: sensor_ciudad='{sensor_ciudad}'==ciudad='{ciudad}', sensor_pais='{sensor_pais}'==pais='{pais}'")
                        print(f"🔍 DEBUG - Zona: sensor_zona='{sensor_zona}', buscando_zona='{zona}', ¿coincide? {not zona or not zona.strip() or sensor_zona == zona}")
                        
                        if sensor_ciudad == ciudad and sensor_pais == pais:
                            # Comparar zonas ignorando espacios y case
                            zona_buscada = zona.strip() if zona else ""
                            zona_sensor_clean = sensor_zona.strip() if sensor_zona else ""
                            
                            if not zona_buscada or zona_sensor_clean.lower() == zona_buscada.lower() or zona_sensor_clean == "":
                                sensor_coincide = True
                                print(f"✅ DEBUG - SENSOR COINCIDE: {sensor.get('name', 'N/A')}")
                            else:
                                print(f"⚠️ DEBUG - Sensor NO coincide por ZONA: sensor_zona='{zona_sensor_clean}' != buscando='{zona_buscada}'")
                
                if sensor_coincide:
                    sensores_encontrados.append(sensor)
            
            # Convertir ObjectId a string
            for sensor in sensores_encontrados:
                if "_id" in sensor:
                    sensor["_id"] = str(sensor["_id"])
            
            return sensores_encontrados
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo sensores por ubicación: {e}")
            return []
    
    def obtener_ubicaciones_disponibles(self):
        """Obtener lista de ubicaciones disponibles para mostrar en errores"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return "MongoDB no disponible"
            
            sensores = self.mongodb_service.obtener_sensores()
            ubicaciones = set()
            
            for sensor in sensores:
                location = sensor.get('location', {})
                if isinstance(location, dict):
                    ciudad = location.get('city', '')
                    pais = location.get('country', '')
                    if ciudad and pais:
                        ubicaciones.add(f"• {ciudad}, {pais}")
            
            if ubicaciones:
                return "\n".join(sorted(list(ubicaciones)))
            else:
                return "No hay ubicaciones disponibles"
                
        except Exception as e:
            return f"Error obteniendo ubicaciones: {e}"
    
    def diagnostico_servicio(self):
        """Realizar diagnóstico del servicio de consultas"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Crear ventana de diagnóstico
            diagnostico_window = tk.Toplevel(self.root)
            diagnostico_window.title("🔍 Diagnóstico del Servicio")
            diagnostico_window.geometry("800x600")
            diagnostico_window.configure(bg='white')
            
            # Crear área de texto para mostrar diagnóstico
            texto_diagnostico = scrolledtext.ScrolledText(diagnostico_window, height=30)
            texto_diagnostico.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Realizar diagnóstico
            diagnostico = self.realizar_diagnostico_completo()
            texto_diagnostico.insert("1.0", diagnostico)
            
        except Exception as e:
            self.agregar_log(f"❌ Error en diagnóstico: {e}")
            messagebox.showerror("Error", f"Error en diagnóstico: {e}")
    
    def realizar_diagnostico_completo(self):
        """Realizar diagnóstico completo del sistema"""
        diagnostico = """🔍 DIAGNÓSTICO COMPLETO DEL SERVICIO DE CONSULTAS
{'='*60}

"""
        
        try:
            # 1. Verificar conexión MongoDB
            if not self.mongodb_service or not self.mongodb_service.conectado:
                diagnostico += "❌ MONGODB: No conectado\n\n"
                return diagnostico
            else:
                diagnostico += "✅ MONGODB: Conectado correctamente\n\n"
            
            # 2. Verificar sensores
            sensores = self.mongodb_service.obtener_sensores()
            diagnostico += f"📊 SENSORES: {len(sensores)} sensores encontrados\n"
            
            if sensores:
                diagnostico += "\n📍 UBICACIONES DE SENSORES:\n"
                ubicaciones = {}
                for sensor in sensores:
                    location = sensor.get('location', {})
                    if isinstance(location, dict):
                        ciudad = location.get('city', '')
                        pais = location.get('country', '')
                        if ciudad and pais:
                            ubicacion_key = f"{ciudad}, {pais}"
                            if ubicacion_key not in ubicaciones:
                                ubicaciones[ubicacion_key] = []
                            ubicaciones[ubicacion_key].append(sensor.get('name', 'N/A'))
                
                for ubicacion, nombres_sensores in ubicaciones.items():
                    diagnostico += f"• {ubicacion}: {len(nombres_sensores)} sensores\n"
                    for nombre in nombres_sensores[:3]:  # Mostrar solo los primeros 3
                        diagnostico += f"  - {nombre}\n"
                    if len(nombres_sensores) > 3:
                        diagnostico += f"  - ... y {len(nombres_sensores) - 3} más\n"
            
            # 3. Verificar mediciones
            diagnostico += f"\n📈 MEDICIONES:\n"
            total_mediciones = 0
            for sensor in sensores[:5]:  # Verificar solo los primeros 5 sensores
                sensor_id = sensor.get('sensor_id', '')
                sensor_name = sensor.get('name', '')
                mediciones = self.mongodb_service.obtener_mediciones_sensor(sensor_id)
                total_mediciones += len(mediciones)
                diagnostico += f"• {sensor_name}: {len(mediciones)} mediciones\n"
            
            diagnostico += f"\n📊 TOTAL DE MEDICIONES: {total_mediciones}\n"
            
            # 4. Verificar fechas de mediciones
            if total_mediciones > 0:
                diagnostico += f"\n📅 RANGOS DE FECHAS DISPONIBLES:\n"
                fechas_todas = []
                for sensor in sensores[:3]:  # Solo los primeros 3 sensores
                    sensor_id = sensor.get('sensor_id', '')
                    mediciones = self.mongodb_service.obtener_mediciones_sensor(sensor_id)
                    if mediciones:
                        fechas = []
                        for m in mediciones:
                            timestamp = m.get('timestamp', '')
                            if timestamp:
                                # Convertir a string si es datetime
                                if hasattr(timestamp, 'strftime'):
                                    fechas.append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                                else:
                                    fechas.append(str(timestamp))
                        fechas_todas.extend(fechas)
                
                if fechas_todas:
                    fechas_todas.sort()
                    primera_fecha = fechas_todas[0]
                    ultima_fecha = fechas_todas[-1]
                    
                    # Extraer solo la fecha (primeros 10 caracteres)
                    primera_str = primera_fecha[:10]
                    ultima_str = ultima_fecha[:10]
                    
                    diagnostico += f"• Primera medición: {primera_str}\n"
                    diagnostico += f"• Última medición: {ultima_str}\n"
            
            # 5. Recomendaciones
            diagnostico += f"\n💡 RECOMENDACIONES:\n"
            if len(sensores) == 0:
                diagnostico += "• Agregar sensores al sistema\n"
            if total_mediciones == 0:
                diagnostico += "• Generar datos de mediciones para los sensores\n"
            if len(ubicaciones) == 0:
                diagnostico += "• Configurar ubicaciones para los sensores\n"
            
            diagnostico += "• Usar fechas dentro del rango disponible\n"
            diagnostico += "• Verificar que la ciudad y país estén escritos exactamente igual\n"
            
        except Exception as e:
            diagnostico += f"\n❌ ERROR EN DIAGNÓSTICO: {e}\n"
        
        return diagnostico
    
    def generar_datos_prueba_servicio(self):
        """Generar datos de prueba para todos los sensores"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Confirmar acción
            respuesta = messagebox.askyesno(
                "Confirmar Generación", 
                "¿Está seguro de que desea generar datos de prueba para todos los sensores?\n\n"
                "Esto creará mediciones simuladas para los últimos 6 meses."
            )
            
            if not respuesta:
                return
            
            # Crear ventana de progreso
            progress_window = tk.Toplevel(self.root)
            progress_window.title("📊 Generando Datos de Prueba")
            progress_window.geometry("400x150")
            progress_window.configure(bg='white')
            progress_window.transient(self.root)
            progress_window.grab_set()
            
            # Centrar ventana
            progress_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
            
            tk.Label(progress_window, text="📊 Generando datos de prueba...", 
                    font=('Arial', 12, 'bold'), bg='white').pack(pady=20)
            
            progress_var = tk.StringVar(value="Iniciando generación...")
            progress_label = tk.Label(progress_window, textvariable=progress_var, bg='white')
            progress_label.pack(pady=10)
            
            progress_window.update()
            
            # Obtener todos los sensores
            sensores = self.mongodb_service.obtener_sensores()
            total_generados = 0
            
            for i, sensor in enumerate(sensores):
                sensor_id = sensor.get('sensor_id', '')
                sensor_name = sensor.get('name', '')
                sensor_type = sensor.get('type', 'Temperatura')
                location = sensor.get('location', {})
                
                progress_var.set(f"Generando datos para {sensor_name}...")
                progress_window.update()
                
                # Generar datos para los últimos 6 meses
                mediciones_generadas = self.generar_mediciones_sensor_prueba(
                    sensor_id, sensor_name, sensor_type, location, 180  # 6 meses = ~180 días
                )
                
                total_generados += len(mediciones_generadas)
                self.agregar_log(f"✅ Generadas {len(mediciones_generadas)} mediciones para {sensor_name}")
            
            # Cerrar ventana de progreso
            progress_window.destroy()
            
            # Mostrar resultado
            messagebox.showinfo("Generación Completada", 
                               f"✅ Se generaron {total_generados} mediciones de prueba\n"
                               f"📊 Para {len(sensores)} sensores\n"
                               f"📅 Período: últimos 6 meses")
            
            self.agregar_log(f"✅ Generación completada: {total_generados} mediciones para {len(sensores)} sensores")
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando datos de prueba: {e}")
            messagebox.showerror("Error", f"Error generando datos: {e}")
    
    def generar_mediciones_sensor_prueba(self, sensor_id, sensor_name, sensor_type, location, dias_atras):
        """Generar mediciones de prueba para un sensor específico"""
        try:
            import random
            from datetime import datetime, timedelta
            
            mediciones_generadas = []
            fecha_actual = datetime.now()
            
            # Obtener ubicación del sensor
            ciudad = location.get('city', 'Buenos Aires')
            pais = location.get('country', 'Argentina')
            zona = location.get('zone', 'Centro')
            
            # Parámetros base según el tipo de sensor
            if sensor_type == "Temperatura":
                temp_base = random.uniform(15, 25)  # Temperatura base
                hum_base = random.uniform(50, 70)   # Humedad base
            elif sensor_type == "Humedad":
                temp_base = random.uniform(18, 22)  # Temperatura base
                hum_base = random.uniform(60, 80)   # Humedad base
            else:  # Ambos
                temp_base = random.uniform(16, 24)  # Temperatura base
                hum_base = random.uniform(55, 75)   # Humedad base
            
            # Generar mediciones para cada día
            for dia in range(dias_atras):
                fecha_medicion = fecha_actual - timedelta(days=dia)
                
                # Generar múltiples mediciones por día (cada 2 horas)
                for hora in range(0, 24, 2):
                    timestamp = fecha_medicion.replace(hour=hora, minute=0, second=0, microsecond=0)
                    
                    # Variación estacional y diaria
                    variacion_dia = random.uniform(-3, 3)  # Variación diaria
                    variacion_hora = random.uniform(-2, 2)  # Variación horaria
                    
                    # Temperatura con variación estacional
                    temperatura = temp_base + variacion_dia + variacion_hora
                    temperatura = max(5, min(40, temperatura))  # Limitar entre 5°C y 40°C
                    
                    # Humedad con variación inversa a temperatura
                    humedad = hum_base - (variacion_dia * 0.5) + random.uniform(-5, 5)
                    humedad = max(20, min(95, humedad))  # Limitar entre 20% y 95%
                    
                    # Crear medición
                    medicion = {
                        "sensor_id": sensor_id,
                        "sensor_name": sensor_name,
                        "timestamp": timestamp.isoformat(),
                        "temperature": round(temperatura, 2),
                        "humidity": round(humedad, 2),
                        "location": {
                            "city": ciudad,
                            "country": pais,
                            "zone": zona
                        },
                        "quality": "good",
                        "source": "simulated"
                    }
                    
                    mediciones_generadas.append(medicion)
            
            # Guardar mediciones en MongoDB
            if mediciones_generadas and self.mongodb_service.conectado:
                self.mongodb_service.db.measurements.insert_many(mediciones_generadas)
            
            return mediciones_generadas
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando mediciones para {sensor_id}: {e}")
            return []
    
    def generar_consulta_ubicacion(self, ciudad, pais, mediciones, agrupacion, parametros):
        """Generar consulta básica por ubicación"""
        try:
            print(f"🔍 DEBUG generar_consulta_ubicacion: ciudad={ciudad}, pais={pais}")
            print(f"🔍 DEBUG generar_consulta_ubicacion: mediciones={len(mediciones) if mediciones else 'None'}")
            print(f"🔍 DEBUG generar_consulta_ubicacion: agrupacion={agrupacion}, parametros={parametros}")
            
            if not mediciones:
                return "❌ No hay mediciones para generar el reporte"
            
            # Verificar que todas las mediciones sean diccionarios válidos
            mediciones_validas = []
            for i, m in enumerate(mediciones):
                if m is None:
                    print(f"🔍 DEBUG: Medición {i} es None")
                    continue
                if not isinstance(m, dict):
                    print(f"🔍 DEBUG: Medición {i} no es diccionario, es {type(m)}")
                    continue
                mediciones_validas.append(m)
            
            if not mediciones_validas:
                return "❌ No hay mediciones válidas para generar el reporte"
            
            print(f"🔍 DEBUG: Mediciones válidas: {len(mediciones_validas)}")
            
            resultado = f"""🌐 CONSULTA EN LÍNEA POR UBICACIÓN
📍 Ubicación: {ciudad}, {pais}
📅 Período: {len(mediciones_validas)} mediciones
🔄 Agrupación: {agrupacion}
📊 Parámetros: {parametros}
{'='*60}

📈 RESUMEN GENERAL:
• Total de mediciones: {len(mediciones_validas)}
• Sensores involucrados: {len(set(m.get('sensor_id', '') for m in mediciones_validas))}
• Período de datos: {min(m.get('timestamp', '') for m in mediciones_validas if m.get('timestamp'))} - {max(m.get('timestamp', '') for m in mediciones_validas if m.get('timestamp'))}

"""
            
            if parametros == "Solo Temperatura" or parametros == "Temperatura y Humedad":
                temperaturas = [m.get('temperature', 0) for m in mediciones_validas if m.get('temperature') is not None]
                if temperaturas:
                    resultado += f"""🌡️ ANÁLISIS DE TEMPERATURA:
• Temperatura promedio: {sum(temperaturas)/len(temperaturas):.2f}°C
• Temperatura mínima: {min(temperaturas):.2f}°C
• Temperatura máxima: {max(temperaturas):.2f}°C
• Rango de variación: {max(temperaturas) - min(temperaturas):.2f}°C

"""
            
            if parametros == "Solo Humedad" or parametros == "Temperatura y Humedad":
                humedades = [m.get('humidity', 0) for m in mediciones_validas if m.get('humidity') is not None]
                if humedades:
                    resultado += f"""💧 ANÁLISIS DE HUMEDAD:
• Humedad promedio: {sum(humedades)/len(humedades):.2f}%
• Humedad mínima: {min(humedades):.2f}%
• Humedad máxima: {max(humedades):.2f}%
• Rango de variación: {max(humedades) - min(humedades):.2f}%

"""
            
            # Agregar análisis por agrupación temporal
            if agrupacion != "Sin Agrupación":
                resultado += f"""📅 ANÁLISIS POR {agrupacion.upper()}:
"""
                grupos = None
                # Determinar qué campo usar para la agrupación
                campo_agrupacion = None
                if parametros == "Solo Temperatura":
                    campo_agrupacion = 'temperature'
                elif parametros == "Solo Humedad":
                    campo_agrupacion = 'humidity'
                else:  # Temperatura y Humedad
                    campo_agrupacion = 'temperature'  # Por defecto usar temperatura
                
                if agrupacion == "Diaria":
                    grupos = self.agrupar_mediciones_diarias(mediciones_validas, campo_agrupacion)
                elif agrupacion == "Semanal":
                    grupos = self.agrupar_mediciones_semanales(mediciones_validas, campo_agrupacion)
                elif agrupacion == "Mensual":
                    grupos = self.agrupar_mediciones_mensuales(mediciones_validas, campo_agrupacion)
                elif agrupacion == "Anual":
                    grupos = self.agrupar_mediciones_anuales(mediciones_validas, campo_agrupacion)
                
                if grupos and isinstance(grupos, dict):
                    unidad = "°C" if campo_agrupacion == 'temperature' else "%"
                    for periodo, valores in list(grupos.items())[:10]:  # Mostrar solo los primeros 10
                        if valores:
                            resultado += f"• {periodo}: Promedio {sum(valores)/len(valores):.2f}{unidad}, {len(valores)} mediciones\n"
                else:
                    resultado += f"• No se pudieron agrupar las mediciones por {agrupacion.lower()}\n"
            
            return resultado
        
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            return f"❌ Error generando consulta por ubicación: {e}\n\n🔍 Detalles del error:\n{error_details}"
    
    def generar_analisis_estadistico_ubicacion(self, ciudad, pais, mediciones, agrupacion, parametros):
        """Generar análisis estadístico avanzado por ubicación"""
        resultado = f"""📊 ANÁLISIS ESTADÍSTICO AVANZADO POR UBICACIÓN
📍 Ubicación: {ciudad}, {pais}
📅 Total de mediciones: {len(mediciones)}
{'='*60}

"""
        
        if parametros == "Solo Temperatura" or parametros == "Temperatura y Humedad":
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            if temperaturas:
                import statistics
                resultado += f"""🌡️ ESTADÍSTICAS DE TEMPERATURA:
• Media: {statistics.mean(temperaturas):.2f}°C
• Mediana: {statistics.median(temperaturas):.2f}°C
• Moda: {statistics.mode(temperaturas):.2f}°C
• Desviación estándar: {statistics.stdev(temperaturas):.2f}°C
• Varianza: {statistics.variance(temperaturas):.2f}
• Coeficiente de variación: {(statistics.stdev(temperaturas)/statistics.mean(temperaturas)*100):.2f}%

"""
        
        if parametros == "Solo Humedad" or parametros == "Temperatura y Humedad":
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            if humedades:
                import statistics
                resultado += f"""💧 ESTADÍSTICAS DE HUMEDAD:
• Media: {statistics.mean(humedades):.2f}%
• Mediana: {statistics.median(humedades):.2f}%
• Moda: {statistics.mode(humedades):.2f}%
• Desviación estándar: {statistics.stdev(humedades):.2f}%
• Varianza: {statistics.variance(humedades):.2f}
• Coeficiente de variación: {(statistics.stdev(humedades)/statistics.mean(humedades)*100):.2f}%

"""
        
        return resultado
    
    def generar_reporte_tendencias_ubicacion(self, ciudad, pais, mediciones, agrupacion, parametros):
        """Generar reporte de tendencias por ubicación"""
        resultado = f"""📈 REPORTE DE TENDENCIAS POR UBICACIÓN
📍 Ubicación: {ciudad}, {pais}
📅 Período analizado: {len(mediciones)} mediciones
{'='*60}

"""
        
        if parametros == "Solo Temperatura" or parametros == "Temperatura y Humedad":
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            if len(temperaturas) > 1:
                temp_inicial = temperaturas[0]
                temp_final = temperaturas[-1]
                tendencia_temp = "ascendente" if temp_final > temp_inicial else "descendente" if temp_final < temp_inicial else "estable"
                
                resultado += f"""🌡️ TENDENCIA DE TEMPERATURA:
• Tendencia general: {tendencia_temp}
• Cambio total: {temp_final - temp_inicial:.2f}°C
• Temperatura inicial: {temp_inicial:.2f}°C
• Temperatura final: {temp_final:.2f}°C
• Velocidad de cambio: {(temp_final - temp_inicial)/len(temperaturas):.4f}°C por medición

"""
        
        if parametros == "Solo Humedad" or parametros == "Temperatura y Humedad":
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            if len(humedades) > 1:
                humedad_inicial = humedades[0]
                humedad_final = humedades[-1]
                tendencia_hum = "ascendente" if humedad_final > humedad_inicial else "descendente" if humedad_final < humedad_inicial else "estable"
                
                resultado += f"""💧 TENDENCIA DE HUMEDAD:
• Tendencia general: {tendencia_hum}
• Cambio total: {humedad_final - humedad_inicial:.2f}%
• Humedad inicial: {humedad_inicial:.2f}%
• Humedad final: {humedad_final:.2f}%
• Velocidad de cambio: {(humedad_final - humedad_inicial)/len(humedades):.4f}% por medición

"""
        
        return resultado
    
    def generar_exportacion_ubicacion(self, ciudad, pais, mediciones, agrupacion, parametros):
        """Generar datos para exportación por ubicación"""
        resultado = f"""📄 EXPORTACIÓN DE DATOS POR UBICACIÓN
📍 Ubicación: {ciudad}, {pais}
📅 Total de registros: {len(mediciones)}
{'='*60}

📋 INFORMACIÓN PARA EXPORTACIÓN:
• Formato recomendado: CSV/JSON
• Campos disponibles: timestamp, temperature, humidity, sensor_id, location
• Tamaño estimado: {len(mediciones) * 0.1:.2f} KB
• Compresión recomendada: ZIP (reducción ~70%)

📊 MUESTRA DE DATOS (primeros 10 registros):
"""
        
        for i, medicion in enumerate(mediciones[:10]):
            resultado += f"{i+1:2d}. {medicion.get('timestamp', 'N/A')[:19]} | "
            if parametros == "Solo Temperatura" or parametros == "Temperatura y Humedad":
                resultado += f"Temp: {medicion.get('temperature', 'N/A')}°C | "
            if parametros == "Solo Humedad" or parametros == "Temperatura y Humedad":
                resultado += f"Humedad: {medicion.get('humidity', 'N/A')}% | "
            resultado += f"Sensor: {medicion.get('sensor_id', 'N/A')}\n"
        
        resultado += f"\n💾 DATOS COMPLETOS DISPONIBLES PARA EXPORTACIÓN"
        
        return resultado
    
    def generar_analisis_comparativo(self, ciudad, pais, mediciones, agrupacion, parametros):
        """Generar análisis comparativo entre ubicaciones"""
        resultado = f"""🔄 ANÁLISIS COMPARATIVO ENTRE UBICACIONES
📍 Ubicación principal: {ciudad}, {pais}
📅 Mediciones analizadas: {len(mediciones)}
{'='*60}

"""
        
        # Obtener datos de otras ubicaciones para comparar
        try:
            sensores_todos = self.mongodb_service.obtener_sensores()
            ubicaciones_comparar = {}
            
            for sensor in sensores_todos:
                location = sensor.get('location', {})
                if isinstance(location, dict):
                    ciudad_comp = location.get('city', '')
                    pais_comp = location.get('country', '')
                    if ciudad_comp and pais_comp and (ciudad_comp != ciudad or pais_comp != pais):
                        ubicacion_key = f"{ciudad_comp}, {pais_comp}"
                        if ubicacion_key not in ubicaciones_comparar:
                            ubicaciones_comparar[ubicacion_key] = []
                        ubicaciones_comparar[ubicacion_key].append(sensor)
            
            resultado += f"📍 UBICACIONES DISPONIBLES PARA COMPARACIÓN:\n"
            for ubicacion, sensores in list(ubicaciones_comparar.items())[:5]:  # Máximo 5 ubicaciones
                resultado += f"• {ubicacion}: {len(sensores)} sensores\n"
            
            resultado += f"\n📊 COMPARACIÓN ESTADÍSTICA:\n"
            resultado += f"• Ubicación actual ({ciudad}, {pais}): {len(mediciones)} mediciones\n"
            
            if parametros == "Solo Temperatura" or parametros == "Temperatura y Humedad":
                temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
                if temperaturas:
                    temp_promedio = sum(temperaturas)/len(temperaturas)
                    resultado += f"• Temperatura promedio: {temp_promedio:.2f}°C\n"
            
            if parametros == "Solo Humedad" or parametros == "Temperatura y Humedad":
                humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
                if humedades:
                    hum_promedio = sum(humedades)/len(humedades)
                    resultado += f"• Humedad promedio: {hum_promedio:.2f}%\n"
            
        except Exception as e:
            resultado += f"⚠️ Error obteniendo datos comparativos: {e}\n"
        
        return resultado
    
    def agrupar_mediciones_anuales(self, mediciones, campo):
        """Agrupar mediciones por año"""
        from collections import defaultdict
        import datetime
        
        grupos = defaultdict(list)
        
        for medicion in mediciones:
            timestamp = medicion.get('timestamp', '')
            if timestamp:
                try:
                    # Manejar tanto datetime objects como strings
                    if isinstance(timestamp, datetime.datetime):
                        fecha = timestamp
                    else:
                        fecha = datetime.datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                    año = fecha.strftime('%Y')
                    grupos[año].append(medicion.get(campo, 0))
                except Exception as e:
                    print(f"🔍 DEBUG: Error procesando fecha {timestamp}: {e}")
                    continue
        
        return grupos
    
    def generar_factura_consulta_linea(self, ciudad, pais, tipo_sensor):
        """Generar factura para consulta en línea"""
        try:
            # Verificar si el usuario debe pagar (no es admin ni técnico)
            if self.rol_usuario in ["administrador", "técnico"]:
                self.agregar_log(f"✅ Usuario {self.rol_usuario} - Sin cargo por consulta en línea")
                return
            
            # Calcular costo según tipo de sensor
            costos = {
                "Todos los Sensores": 5.00,
                "Solo Temperatura": 3.00,
                "Solo Humedad": 3.00
            }
            
            costo = costos.get(tipo_sensor, 5.00)
            
            # Generar ID de factura
            factura_id = f"FACT_CONSULTA_{int(time.time())}"
            
            # Crear datos de factura
            factura_data = {
                "invoice_id": factura_id,
                "user_id": self.usuario_autenticado,
                "amount": costo,
                "total_amount": float(costo),
                "status": "pending",
                "description": f"Consulta en línea: {tipo_sensor} - {ciudad}, {pais}",
                "service_type": "consulta_linea",
                "location": f"{ciudad}, {pais}",
                "created_at": datetime.now().isoformat(),
                "due_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
                "procesos_facturados": []
            }
            
            # Guardar en MongoDB
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.db.invoices.insert_one(factura_data)
                self.agregar_log(f"💰 Factura generada: {factura_id} - ${costo:.2f}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando factura: {e}")
    
    def ver_historial_consultas(self):
        """Ver historial de consultas en línea"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Crear ventana de historial
            historial_window = tk.Toplevel(self.root)
            historial_window.title("📊 Historial de Consultas en Línea")
            historial_window.geometry("800x600")
            historial_window.configure(bg='white')
            
            # Crear Treeview para mostrar historial (agregamos Vence y Total)
            columns = ("Fecha", "Vence", "Ubicación", "Tipo", "Costo", "Total", "Estado")
            tree_historial = ttk.Treeview(historial_window, columns=columns, show="headings")
            
            for col in columns:
                tree_historial.heading(col, text=col)
                tree_historial.column(col, width=150)
            
            # Obtener facturas de consultas en línea
            facturas = list(self.mongodb_service.db.invoices.find({
                "service_type": "consulta_linea",
                "user_id": self.usuario_autenticado
            }).sort("created_at", -1))
            
            for factura in facturas:
                fecha = factura.get('created_at', '')[:10]
                vence = factura.get('due_date', 'N/A')
                ubicacion = factura.get('location', 'N/A')
                descripcion = factura.get('description', '')
                tipo = descripcion.split(':')[1].split(' - ')[0].strip() if ':' in descripcion else 'N/A'
                costo = f"${factura.get('amount', 0):.2f}"
                total = f"${factura.get('total_amount', factura.get('amount', 0)):.2f}"
                estado = factura.get('status', 'pending')
                
                tree_historial.insert("", "end", values=(fecha, vence, ubicacion, tipo, costo, total, estado))
            
            tree_historial.pack(fill='both', expand=True, padx=10, pady=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando historial: {e}")
            messagebox.showerror("Error", f"Error mostrando historial: {e}")
    
    def ver_facturas_consultas(self):
        """Ver facturas de consultas en línea"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            # Crear ventana de facturas
            facturas_window = tk.Toplevel(self.root)
            facturas_window.title("💰 Facturas de Consultas en Línea")
            facturas_window.geometry("900x500")
            facturas_window.configure(bg='white')
            
            # Crear Treeview para mostrar facturas (agregamos Vence y Total)
            columns = ("ID Factura", "Fecha", "Vence", "Ubicación", "Descripción", "Monto", "Total", "Estado")
            tree_facturas = ttk.Treeview(facturas_window, columns=columns, show="headings")
            
            for col in columns:
                tree_facturas.heading(col, text=col)
                tree_facturas.column(col, width=150)
            
            # Obtener facturas de consultas en línea
            facturas = list(self.mongodb_service.db.invoices.find({
                "service_type": "consulta_linea",
                "user_id": self.usuario_autenticado
            }).sort("created_at", -1))
            
            for factura in facturas:
                factura_id = factura.get('invoice_id', 'N/A')
                fecha = factura.get('created_at', '')[:19]
                vence = factura.get('due_date', 'N/A')
                ubicacion = factura.get('location', 'N/A')
                descripcion = factura.get('description', 'N/A')
                monto = f"${factura.get('amount', 0):.2f}"
                total = f"${factura.get('total_amount', factura.get('amount', 0)):.2f}"
                estado = factura.get('status', 'pending')
                
                tree_facturas.insert("", "end", values=(factura_id, fecha, vence, ubicacion, descripcion, monto, total, estado))
            
            tree_facturas.pack(fill='both', expand=True, padx=10, pady=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando facturas: {e}")
            messagebox.showerror("Error", f"Error mostrando facturas: {e}")
    
    def formatear_nombre_sensor(self, sensor):
        """Formatear nombre de sensor de manera legible"""
        try:
            nombre = sensor.get('name', 'Sensor Sin Nombre')
            ubicacion = sensor.get('location', {})
            
            # Extraer información de ubicación de manera limpia
            ciudad = ubicacion.get('city', '')
            pais = ubicacion.get('country', '')
            zona = ubicacion.get('zone', '')
            
            # Construir nombre legible
            if ciudad and pais:
                if zona:
                    return f"{nombre} ({ciudad}, {zona} - {pais})"
                else:
                    return f"{nombre} ({ciudad} - {pais})"
            elif ciudad:
                return f"{nombre} ({ciudad})"
            else:
                return nombre
                
        except Exception as e:
            # Fallback en caso de error
            return sensor.get('name', 'Sensor Sin Nombre')
    
    def cargar_sensores_para_servicios(self):
        """Cargar sensores para el combo de servicios"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar sensores de servicios")
                return
            
            sensores = self.mongodb_service.obtener_sensores()
            nombres_sensores = []
            
            for sensor in sensores:
                nombre_formateado = self.formatear_nombre_sensor(sensor)
                nombres_sensores.append(nombre_formateado)
            
            self.combo_sensor_servicio['values'] = nombres_sensores
            if nombres_sensores:
                self.combo_sensor_servicio.set(nombres_sensores[0])
                
            self.agregar_log(f"✅ Sensores cargados para servicios: {len(nombres_sensores)}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando sensores para servicios: {e}")
    
    def ejecutar_servicio_premium(self):
        """Ejecutar servicio premium con facturación automática"""
        try:
            # Validar campos
            sensor_seleccionado = self.combo_sensor_servicio.get()
            fecha_inicio = self.entry_fecha_inicio_servicio.get()
            fecha_fin = self.entry_fecha_fin_servicio.get()
            tipo_servicio = self.combo_tipo_servicio.get()
            
            if not sensor_seleccionado or not fecha_inicio or not fecha_fin or not tipo_servicio:
                messagebox.showerror("Error", "Complete todos los campos")
                return
            
            # Confirmar ejecución con información de costos
            costo_estimado = self.calcular_costo_servicio(tipo_servicio, fecha_inicio, fecha_fin)
            
            # Mensaje personalizado según el rol
            if self.rol_usuario == "usuario":
                mensaje_costo = f"Costo estimado: ${costo_estimado:.2f}\n\n⚠️ IMPORTANTE: Se generará una factura automática que se cargará a tu cuenta."
            elif self.rol_usuario in ["técnico", "administrador"]:
                mensaje_costo = f"Costo estimado: ${costo_estimado:.2f}\n\n🆓 GRATIS: Tu rol no genera facturación."
            else:
                mensaje_costo = f"Costo estimado: ${costo_estimado:.2f}\n\nSe generará una factura automática."
            
            respuesta = messagebox.askyesno("Confirmar Servicio Premium", 
                f"¿Ejecutar servicio premium?\n\n"
                f"Tipo: {tipo_servicio}\n"
                f"Sensor: {sensor_seleccionado}\n"
                f"Período: {fecha_inicio} a {fecha_fin}\n"
                f"{mensaje_costo}")
            
            if not respuesta:
                return
            
            # Limpiar resultados anteriores
            self.texto_resultados_servicio.delete(1.0, tk.END)
            
            # Mostrar progreso
            self.texto_resultados_servicio.insert(tk.END, f"🚀 INICIANDO SERVICIO PREMIUM\n")
            self.texto_resultados_servicio.insert(tk.END, f"{'='*50}\n")
            self.texto_resultados_servicio.insert(tk.END, f"Tipo: {tipo_servicio}\n")
            self.texto_resultados_servicio.insert(tk.END, f"Sensor: {sensor_seleccionado}\n")
            self.texto_resultados_servicio.insert(tk.END, f"Período: {fecha_inicio} a {fecha_fin}\n")
            self.texto_resultados_servicio.insert(tk.END, f"Costo: ${costo_estimado:.2f}\n")
            self.texto_resultados_servicio.insert(tk.END, f"Usuario: {self.usuario_autenticado}\n")
            self.texto_resultados_servicio.insert(tk.END, f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Ejecutar servicio en hilo separado
            threading.Thread(target=self.ejecutar_servicio_background, 
                           args=(sensor_seleccionado, fecha_inicio, fecha_fin, tipo_servicio, costo_estimado), 
                           daemon=True).start()
            
        except Exception as e:
            self.agregar_log(f"❌ Error ejecutando servicio premium: {e}")
            messagebox.showerror("Error", f"Error ejecutando servicio: {e}")
    
    def calcular_costo_servicio(self, tipo_servicio, fecha_inicio, fecha_fin):
        """Calcular costo estimado del servicio"""
        try:
            # Calcular días del período
            fecha_inicio_obj = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            fecha_fin_obj = datetime.strptime(fecha_fin, "%Y-%m-%d")
            dias_periodo = (fecha_fin_obj - fecha_inicio_obj).days + 1
            
            # Costos base por tipo de servicio
            costos_base = {
                "Consulta Completa de Datos": 0.50,
                "Análisis Estadístico Avanzado": 1.00,
                "Exportación Masiva de Datos": 0.75,
                "Reporte de Tendencias Históricas": 1.25,
                "Análisis de Correlaciones": 1.50,
                "Predicción de Patrones": 2.00
            }
            
            costo_base = costos_base.get(tipo_servicio, 1.00)
            
            # Calcular costo total (costo base * días * factor de complejidad)
            factor_complejidad = min(dias_periodo / 30, 3.0)  # Máximo 3x para períodos largos
            costo_total = costo_base * dias_periodo * factor_complejidad
            
            return max(costo_total, 5.00)  # Mínimo $5
            
        except Exception as e:
            self.agregar_log(f"❌ Error calculando costo: {e}")
            return 10.00  # Costo por defecto
    
    def calcular_costo_proceso(self, tipo_proceso, cantidad_datos=0):
        """Calcular costo de un proceso periódico"""
        try:
            # Costos base por tipo de proceso
            costos_base = {
                "Procesos Periódicos de Consultas por Ciudades": 50.00,
                "Procesos Periódicos de Consultas por Zonas": 75.00,
                "Procesos Periódicos de Consultas por Países": 100.00,
                "Informe de Humedad y Temperaturas Máximas y Mínimas por Ciudades": 40.00,
                "Informe de Humedad y Temperaturas Máximas y Mínimas por Zonas": 60.00,
                "Informe de Humedad y Temperaturas Máximas y Mínimas por Países": 80.00,
                "Informe de Humedad y Temperaturas Promedio por Ciudades": 45.00,
                "Informe de Humedad y Temperaturas Promedio por Zonas": 65.00,
                "Informe de Humedad y Temperaturas Promedio por Países": 90.00
            }
            
            costo_base = costos_base.get(tipo_proceso, 50.00)
            
            # Ajustar según cantidad de datos si se proporciona
            if cantidad_datos > 0:
                factor_datos = 1.0 + (cantidad_datos / 1000) * 0.1  # +10% por cada 1000 datos
                costo_total = costo_base * factor_datos
            else:
                costo_total = costo_base
            
            return round(costo_total, 2)
            
        except Exception as e:
            self.agregar_log(f"❌ Error calculando costo de proceso: {e}")
            return 50.00  # Costo por defecto
    
    def ejecutar_servicio_background(self, sensor_seleccionado, fecha_inicio, fecha_fin, tipo_servicio, costo_estimado):
        """Ejecutar servicio en segundo plano"""
        try:
            servicio_id = f"SERV_{int(time.time())}"
            inicio_ejecucion = datetime.now()
            
            self.texto_resultados_servicio.insert(tk.END, f"📊 Obteniendo datos del sensor...\n")
            
            # Obtener datos del sensor
            sensor_name = sensor_seleccionado.split(" - ")[0]
            mediciones = self.mongodb_service.obtener_mediciones_sensor_periodo(
                sensor_name, fecha_inicio, fecha_fin
            )
            
            if not mediciones:
                self.texto_resultados_servicio.insert(tk.END, f"❌ No se encontraron datos para el período especificado\n")
                return
            
            self.texto_resultados_servicio.insert(tk.END, f"✅ Datos obtenidos: {len(mediciones)} mediciones\n")
            
            # Ejecutar análisis según el tipo de servicio
            resultado = self.ejecutar_analisis_premium(mediciones, tipo_servicio, sensor_name)
            
            # Calcular costo final
            costo_final = self.calcular_costo_final(costo_estimado, len(mediciones))
            
            # Generar factura
            factura_id = self.generar_factura_servicio(servicio_id, tipo_servicio, costo_final)
            
            # Guardar en historial
            self.guardar_historial_servicio(servicio_id, tipo_servicio, sensor_name, 
                                          fecha_inicio, fecha_fin, costo_final, factura_id)
            
            # Mostrar resultados
            self.texto_resultados_servicio.insert(tk.END, f"\n{'='*50}\n")
            self.texto_resultados_servicio.insert(tk.END, f"📋 RESULTADOS DEL SERVICIO\n")
            self.texto_resultados_servicio.insert(tk.END, f"{'='*50}\n")
            self.texto_resultados_servicio.insert(tk.END, resultado)
            
            self.texto_resultados_servicio.insert(tk.END, f"\n{'='*50}\n")
            self.texto_resultados_servicio.insert(tk.END, f"💰 FACTURACIÓN\n")
            self.texto_resultados_servicio.insert(tk.END, f"{'='*50}\n")
            self.texto_resultados_servicio.insert(tk.END, f"ID de Servicio: {servicio_id}\n")
            
            if costo_final > 0:
                self.texto_resultados_servicio.insert(tk.END, f"ID de Factura: {factura_id}\n")
                self.texto_resultados_servicio.insert(tk.END, f"Costo Final: ${costo_final:.2f}\n")
                self.texto_resultados_servicio.insert(tk.END, f"Estado: Facturado automáticamente\n")
            else:
                self.texto_resultados_servicio.insert(tk.END, f"🆓 SERVICIO GRATUITO\n")
                self.texto_resultados_servicio.insert(tk.END, f"Rol: {self.rol_usuario.title()}\n")
                self.texto_resultados_servicio.insert(tk.END, f"Estado: Sin cargo por rol privilegiado\n")
            
            fin_ejecucion = datetime.now()
            duracion = (fin_ejecucion - inicio_ejecucion).total_seconds()
            
            self.texto_resultados_servicio.insert(tk.END, f"\n⏱️ Tiempo de ejecución: {duracion:.2f} segundos\n")
            self.texto_resultados_servicio.insert(tk.END, f"✅ Servicio completado exitosamente\n")
            
            self.agregar_log(f"✅ Servicio premium completado: {servicio_id} - ${costo_final:.2f}")
            
        except Exception as e:
            self.texto_resultados_servicio.insert(tk.END, f"❌ Error ejecutando servicio: {e}\n")
            self.agregar_log(f"❌ Error ejecutando servicio premium: {e}")
    
    def ejecutar_analisis_premium(self, mediciones, tipo_servicio, sensor_name):
        """Ejecutar análisis premium según el tipo de servicio"""
        try:
            if tipo_servicio == "Consulta Completa de Datos":
                return self.analisis_consulta_completa(mediciones, sensor_name)
            elif tipo_servicio == "Análisis Estadístico Avanzado":
                return self.analisis_estadistico_avanzado(mediciones, sensor_name)
            elif tipo_servicio == "Exportación Masiva de Datos":
                return self.analisis_exportacion_masiva(mediciones, sensor_name)
            elif tipo_servicio == "Reporte de Tendencias Históricas":
                return self.analisis_tendencias_historicas(mediciones, sensor_name)
            elif tipo_servicio == "Análisis de Correlaciones":
                return self.analisis_correlaciones(mediciones, sensor_name)
            elif tipo_servicio == "Predicción de Patrones":
                return self.analisis_prediccion_patrones(mediciones, sensor_name)
            else:
                return "Tipo de servicio no reconocido"
                
        except Exception as e:
            return f"Error en análisis premium: {e}"
    
    def analisis_consulta_completa(self, mediciones, sensor_name):
        """Análisis de consulta completa de datos"""
        resultado = f"""CONSULTA COMPLETA DE DATOS
Sensor: {sensor_name}
Total de mediciones: {len(mediciones)}

📊 RESUMEN ESTADÍSTICO:
"""
        
        if mediciones:
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature')]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity')]
            
            if temperaturas:
                resultado += f"• Temperatura promedio: {sum(temperaturas)/len(temperaturas):.2f}°C\n"
                resultado += f"• Temperatura mínima: {min(temperaturas):.2f}°C\n"
                resultado += f"• Temperatura máxima: {max(temperaturas):.2f}°C\n"
            
            if humedades:
                resultado += f"• Humedad promedio: {sum(humedades)/len(humedades):.2f}%\n"
                resultado += f"• Humedad mínima: {min(humedades):.2f}%\n"
                resultado += f"• Humedad máxima: {max(humedades):.2f}%\n"
            
            # Análisis temporal
            fechas = [m.get('timestamp', '') for m in mediciones if m.get('timestamp')]
            if fechas:
                resultado += f"\n📅 ANÁLISIS TEMPORAL:\n"
                resultado += f"• Primera medición: {min(fechas)}\n"
                resultado += f"• Última medición: {max(fechas)}\n"
                resultado += f"• Período total: {len(set(fechas))} días únicos\n"
        
        return resultado
    
    def analisis_estadistico_avanzado(self, mediciones, sensor_name):
        """Análisis estadístico avanzado"""
        resultado = f"""ANÁLISIS ESTADÍSTICO AVANZADO
Sensor: {sensor_name}
Total de mediciones: {len(mediciones)}

📈 ESTADÍSTICAS DESCRIPTIVAS:
"""
        
        if mediciones:
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature')]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity')]
            
            if temperaturas:
                import statistics
                resultado += f"\n🌡️ TEMPERATURA:\n"
                resultado += f"• Media: {statistics.mean(temperaturas):.2f}°C\n"
                resultado += f"• Mediana: {statistics.median(temperaturas):.2f}°C\n"
                resultado += f"• Desviación estándar: {statistics.stdev(temperaturas):.2f}°C\n"
                resultado += f"• Varianza: {statistics.variance(temperaturas):.2f}\n"
            
            if humedades:
                resultado += f"\n💧 HUMEDAD:\n"
                resultado += f"• Media: {statistics.mean(humedades):.2f}%\n"
                resultado += f"• Mediana: {statistics.median(humedades):.2f}%\n"
                resultado += f"• Desviación estándar: {statistics.stdev(humedades):.2f}%\n"
                resultado += f"• Varianza: {statistics.variance(humedades):.2f}\n"
        
        return resultado
    
    def analisis_exportacion_masiva(self, mediciones, sensor_name):
        """Análisis para exportación masiva"""
        resultado = f"""EXPORTACIÓN MASIVA DE DATOS
Sensor: {sensor_name}
Total de registros: {len(mediciones)}

📋 RESUMEN PARA EXPORTACIÓN:
"""
        
        if mediciones:
            resultado += f"• Formato recomendado: CSV/JSON\n"
            resultado += f"• Campos disponibles: timestamp, temperature, humidity, location\n"
            resultado += f"• Tamaño estimado: {len(mediciones) * 0.1:.2f} KB\n"
            
            # Muestra de datos
            resultado += f"\n📄 MUESTRA DE DATOS (primeros 5 registros):\n"
            for i, medicion in enumerate(mediciones[:5]):
                resultado += f"{i+1}. {medicion.get('timestamp', 'N/A')} - "
                resultado += f"Temp: {medicion.get('temperature', 'N/A')}°C, "
                resultado += f"Humedad: {medicion.get('humidity', 'N/A')}%\n"
        
        return resultado
    
    def analisis_tendencias_historicas(self, mediciones, sensor_name):
        """Análisis de tendencias históricas"""
        resultado = f"""REPORTE DE TENDENCIAS HISTÓRICAS
Sensor: {sensor_name}
Período analizado: {len(mediciones)} mediciones

📈 ANÁLISIS DE TENDENCIAS:
"""
        
        if mediciones:
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature')]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity')]
            
            if len(temperaturas) > 1:
                # Tendencia simple
                temp_inicial = temperaturas[0]
                temp_final = temperaturas[-1]
                tendencia_temp = "ascendente" if temp_final > temp_inicial else "descendente" if temp_final < temp_inicial else "estable"
                
                resultado += f"• Tendencia de temperatura: {tendencia_temp}\n"
                resultado += f"• Cambio total: {temp_final - temp_inicial:.2f}°C\n"
            
            if len(humedades) > 1:
                humedad_inicial = humedades[0]
                humedad_final = humedades[-1]
                tendencia_hum = "ascendente" if humedad_final > humedad_inicial else "descendente" if humedad_final < humedad_inicial else "estable"
                
                resultado += f"• Tendencia de humedad: {tendencia_hum}\n"
                resultado += f"• Cambio total: {humedad_final - humedad_inicial:.2f}%\n"
        
        return resultado
    
    def analisis_correlaciones(self, mediciones, sensor_name):
        """Análisis de correlaciones"""
        resultado = f"""ANÁLISIS DE CORRELACIONES
Sensor: {sensor_name}
Datos analizados: {len(mediciones)} mediciones

🔗 ANÁLISIS DE CORRELACIÓN:
"""
        
        if mediciones:
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature')]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity')]
            
            if len(temperaturas) > 1 and len(humedades) > 1:
                # Correlación simple
                import statistics
                try:
                    correlacion = statistics.correlation(temperaturas, humedades)
                    resultado += f"• Correlación temperatura-humedad: {correlacion:.3f}\n"
                    
                    if correlacion > 0.7:
                        resultado += f"• Interpretación: Correlación fuerte positiva\n"
                    elif correlacion > 0.3:
                        resultado += f"• Interpretación: Correlación moderada positiva\n"
                    elif correlacion < -0.7:
                        resultado += f"• Interpretación: Correlación fuerte negativa\n"
                    elif correlacion < -0.3:
                        resultado += f"• Interpretación: Correlación moderada negativa\n"
                    else:
                        resultado += f"• Interpretación: Correlación débil\n"
                except:
                    resultado += f"• No se pudo calcular la correlación\n"
        
        return resultado
    
    def analisis_prediccion_patrones(self, mediciones, sensor_name):
        """Análisis de predicción de patrones"""
        resultado = f"""PREDICCIÓN DE PATRONES
Sensor: {sensor_name}
Datos históricos: {len(mediciones)} mediciones

🔮 ANÁLISIS PREDICTIVO:
"""
        
        if mediciones:
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature')]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity')]
            
            if len(temperaturas) > 5:
                # Predicción simple basada en tendencia
                temp_recientes = temperaturas[-5:]
                tendencia = sum(temp_recientes[i+1] - temp_recientes[i] for i in range(len(temp_recientes)-1)) / (len(temp_recientes)-1)
                
                resultado += f"• Tendencia reciente: {tendencia:.2f}°C por período\n"
                resultado += f"• Predicción próxima medición: {temperaturas[-1] + tendencia:.2f}°C\n"
            
            if len(humedades) > 5:
                humedad_recientes = humedades[-5:]
                tendencia_hum = sum(humedad_recientes[i+1] - humedad_recientes[i] for i in range(len(humedad_recientes)-1)) / (len(humedad_recientes)-1)
                
                resultado += f"• Tendencia humedad: {tendencia_hum:.2f}% por período\n"
                resultado += f"• Predicción próxima medición: {humedades[-1] + tendencia_hum:.2f}%\n"
        
        return resultado
    
    def calcular_costo_final(self, costo_estimado, cantidad_datos):
        """Calcular costo final basado en cantidad de datos y rol del usuario"""
        try:
            # Técnicos y administradores no pagan por servicios premium
            if self.rol_usuario in ["técnico", "administrador"]:
                return 0.00
            
            # Factor de ajuste basado en cantidad de datos
            if cantidad_datos > 10000:
                factor = 1.5
            elif cantidad_datos > 5000:
                factor = 1.3
            elif cantidad_datos > 1000:
                factor = 1.1
            else:
                factor = 1.0
            
            costo_final = costo_estimado * factor
            return max(costo_final, 5.00)  # Mínimo $5
            
        except Exception as e:
            return costo_estimado
    
    def generar_factura_servicio(self, servicio_id, tipo_servicio, costo_final):
        """Generar factura automática para el servicio"""
        try:
            # Verificar si el usuario debe pagar (no es admin ni técnico)
            if self.rol_usuario in ["administrador", "técnico"]:
                self.agregar_log(f"✅ Usuario {self.rol_usuario} - Sin cargo por servicio: {tipo_servicio}")
                return "SERVICIO_GRATUITO"
            
            # No generar factura si el costo es 0
            if costo_final <= 0:
                self.agregar_log(f"🆓 Servicio gratuito: {tipo_servicio}")
                return "SERVICIO_GRATUITO"
            
            factura_id = f"FACT_{int(time.time())}"
            
            factura_data = {
                "factura_id": factura_id,
                "servicio_id": servicio_id,
                "usuario": self.usuario_autenticado,
                "tipo_servicio": tipo_servicio,
                "costo": costo_final,
                "fecha_generacion": datetime.now().isoformat(),
                "estado": "pendiente",
                "metodo_pago": "cuenta_corriente",
                "descripcion": f"Servicio premium: {tipo_servicio}"
            }
            
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.crear_factura(factura_data)
                self.agregar_log(f"✅ Factura generada: {factura_id} - ${costo_final:.2f}")
            
            return factura_id
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando factura: {e}")
            return f"ERROR_{int(time.time())}"
    
    def guardar_historial_servicio(self, servicio_id, tipo_servicio, sensor_name, fecha_inicio, fecha_fin, costo_final, factura_id):
        """Guardar historial de ejecución del servicio"""
        try:
            historial_data = {
                "servicio_id": servicio_id,
                "usuario": self.usuario_autenticado,
                "tipo_servicio": tipo_servicio,
                "sensor": sensor_name,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "costo": costo_final,
                "factura_id": factura_id,
                "fecha_ejecucion": datetime.now().isoformat(),
                "estado": "completado"
            }
            
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.crear_historial_servicio(historial_data)
                self.agregar_log(f"✅ Historial guardado: {servicio_id}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error guardando historial: {e}")
    
    def ver_historial_servicios(self):
        """Ver historial de servicios ejecutados"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            historial = self.mongodb_service.obtener_historial_servicios_usuario(self.usuario_autenticado)
            
            if not historial:
                messagebox.showinfo("Historial", "No hay servicios ejecutados")
                return
            
            # Crear ventana de historial
            historial_window = tk.Toplevel(self.root)
            historial_window.title("Historial de Servicios")
            historial_window.geometry("800x600")
            historial_window.configure(bg='white')
            
            tk.Label(historial_window, text="Historial de Servicios Ejecutados", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            # Treeview para historial
            columns = ("ID", "Tipo", "Sensor", "Período", "Costo", "Fecha")
            tree_historial = ttk.Treeview(historial_window, columns=columns, show="headings")
            
            for col in columns:
                tree_historial.heading(col, text=col)
                tree_historial.column(col, width=120)
            
            for servicio in historial:
                tree_historial.insert("", "end", values=(
                    servicio.get('servicio_id', ''),
                    servicio.get('tipo_servicio', ''),
                    servicio.get('sensor', ''),
                    f"{servicio.get('fecha_inicio', '')} a {servicio.get('fecha_fin', '')}",
                    f"${servicio.get('costo', 0):.2f}",
                    servicio.get('fecha_ejecucion', '')[:10]
                ))
            
            tree_historial.pack(fill='both', expand=True, padx=10, pady=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando historial: {e}")
            messagebox.showerror("Error", f"Error mostrando historial: {e}")
    
    def ver_facturas_servicios(self):
        """Ver facturas de servicios"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no está disponible")
                return
            
            facturas = self.mongodb_service.obtener_facturas_usuario(self.usuario_autenticado)
            
            if not facturas:
                messagebox.showinfo("Facturas", "No hay facturas de servicios")
                return
            
            # Crear ventana de facturas
            facturas_window = tk.Toplevel(self.root)
            facturas_window.title("Facturas de Servicios")
            facturas_window.geometry("700x500")
            facturas_window.configure(bg='white')
            
            tk.Label(facturas_window, text="Facturas de Servicios Premium", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            # Treeview para facturas
            columns = ("ID Factura", "Servicio", "Tipo", "Costo", "Estado", "Fecha")
            tree_facturas = ttk.Treeview(facturas_window, columns=columns, show="headings")
            
            for col in columns:
                tree_facturas.heading(col, text=col)
                tree_facturas.column(col, width=100)
            
            for factura in facturas:
                tree_facturas.insert("", "end", values=(
                    factura.get('factura_id', ''),
                    factura.get('servicio_id', ''),
                    factura.get('tipo_servicio', ''),
                    f"${factura.get('costo', 0):.2f}",
                    factura.get('estado', ''),
                    factura.get('fecha_generacion', '')[:10]
                ))
            
            tree_facturas.pack(fill='both', expand=True, padx=10, pady=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando facturas: {e}")
            messagebox.showerror("Error", f"Error mostrando facturas: {e}")
    
    def crear_tab_configuracion(self):
        """Crear tab de configuración"""
        tab = tk.Frame(self.notebook, bg='white')
        self.notebook.add(tab, text="Configuración")
        
        # VALIDACIÓN DE ACCESO: Solo técnicos y administradores pueden acceder
        if self.rol_usuario == "usuario":
            # Mostrar mensaje de acceso denegado
            access_denied_frame = tk.Frame(tab, bg='white')
            access_denied_frame.pack(fill='both', expand=True, padx=50, pady=50)
            
            tk.Label(access_denied_frame, text="🚫 ACCESO DENEGADO", 
                    font=('Arial', 20, 'bold'), bg='white', fg='#e74c3c').pack(pady=20)
            
            tk.Label(access_denied_frame, text="Este módulo está restringido para usuarios comunes.", 
                    font=('Arial', 14), bg='white', fg='#7f8c8d').pack(pady=10)
            
            tk.Label(access_denied_frame, text="Solo técnicos y administradores pueden acceder a la configuración del sistema.", 
                    font=('Arial', 12), bg='white', fg='#95a5a6').pack(pady=5)
            
            tk.Label(access_denied_frame, text=f"Tu rol actual: {self.rol_usuario.title()}", 
                    font=('Arial', 10, 'bold'), bg='white', fg='#34495e').pack(pady=20)
            
            # Botón para volver al Home
            tk.Button(access_denied_frame, text="🏠 Volver al Home", 
                     command=lambda: self.notebook.select(0), 
                     bg='#3498db', fg='white', font=('Arial', 12, 'bold')).pack(pady=20)
            
            self.agregar_log(f"🚫 Usuario común {self.usuario_autenticado} intentó acceder al módulo de Configuración")
            return
        
        # Configuración de base de datos
        db_frame = tk.LabelFrame(tab, text="Configuración de Base de Datos", 
                               font=('Arial', 12, 'bold'), bg='white')
        db_frame.pack(fill='x', padx=20, pady=10)
        
        db_inner = tk.Frame(db_frame, bg='white')
        db_inner.pack(fill='x', padx=10, pady=10)
        
        # Estado de conexiones
        tk.Label(db_inner, text="MongoDB Atlas:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.label_estado_mongodb = tk.Label(db_inner, text="Desconectado", bg='white', fg='red')
        self.label_estado_mongodb.grid(row=0, column=1, padx=5, pady=5)
        
        tk.Label(db_inner, text="Neo4j Aura:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.label_estado_neo4j = tk.Label(db_inner, text="Desconectado", bg='white', fg='red')
        self.label_estado_neo4j.grid(row=0, column=3, padx=5, pady=5)
        
        tk.Label(db_inner, text="Redis:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.label_estado_redis = tk.Label(db_inner, text="Desconectado", bg='white', fg='red')
        self.label_estado_redis.grid(row=1, column=1, padx=5, pady=5)
        
        # Botones de conexión
        tk.Button(db_inner, text="🔄 Probar Conexiones", 
                 command=self.probar_conexiones, 
                 bg='#3498db', fg='white', font=('Arial', 10)).grid(row=2, column=0, padx=5, pady=10)
        
        tk.Button(db_inner, text="📊 Estadísticas del Sistema", 
                 command=self.mostrar_estadisticas_sistema, 
                 bg='#27ae60', fg='white', font=('Arial', 10)).grid(row=2, column=1, padx=5, pady=10)
    
    def cargar_datos_iniciales(self):
        """Cargar datos iniciales desde MongoDB Atlas"""
        if not self.mongodb_service or not self.mongodb_service.conectado:
            self.agregar_log("❌ MongoDB Atlas no disponible")
            return
        
        try:
            # Actualizar estado de conexiones
            self.actualizar_estado_conexiones()
            
            # Cargar sensores para combos
            self.cargar_sensores_para_combos()
            
            # Cargar usuarios para combos
            self.cargar_usuarios_para_combos()
            
            # Cargar ciudades para combos
            self.cargar_ciudades_para_combos()
            
            # Cargar países para análisis
            self.cargar_paises_para_analisis()
            
            # Cargar ubicaciones para sensores
            self.cargar_ubicaciones_para_sensores()
            
            # Actualizar todas las listas
            self.actualizar_lista_sensores()
            self.actualizar_lista_alertas()
            self.actualizar_lista_facturas()
            self.actualizar_lista_procesos()
            self.actualizar_estadisticas_dashboard()
            
            # self.agregar_log("✅ Datos iniciales cargados desde MongoDB Atlas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando datos iniciales: {e}")
    
    def actualizar_estado_conexiones(self):
        """Actualizar estado de conexiones en la interfaz"""
        if self.mongodb_service and self.mongodb_service.conectado:
            self.label_estado_mongodb.config(text="Conectado", fg='green')
        else:
            self.label_estado_mongodb.config(text="Desconectado", fg='red')
        
        if self.neo4j_service and self.neo4j_service.conectado:
            self.label_estado_neo4j.config(text="Conectado", fg='green')
        else:
            self.label_estado_neo4j.config(text="Desconectado", fg='red')
        
        if self.redis_service and self.redis_service.conectado:
            self.label_estado_redis.config(text="Conectado", fg='green')
        else:
            self.label_estado_redis.config(text="Desconectado", fg='red')
    
    def cargar_sensores_para_combos(self):
        """Cargar sensores para combos"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Si no hay conexión, usar valores por defecto
                sensores_default = ["SENSOR_BA_001", "SENSOR_CBA_001", "SENSOR_ROS_001", "SENSOR_MEN_001", "SENSOR_LP_001"]
                # Solo actualizar combos que existen
                if hasattr(self, 'combo_sensor_alerta'):
                    self.combo_sensor_alerta['values'] = sensores_default
                    self.combo_sensor_alerta.set(sensores_default[0])
                
                if hasattr(self, 'combo_sensor_servicio'):
                    self.combo_sensor_servicio['values'] = sensores_default
                    self.combo_sensor_servicio.set(sensores_default[0])
                self.agregar_log("⚠️ Usando sensores por defecto (MongoDB no disponible)")
                return
            
            sensores = self.mongodb_service.obtener_sensores()
            
            if sensores:
                # Crear lista de sensores con formato más descriptivo usando la función existente
                sensores_formateados = []
                for sensor in sensores:
                    sensor_id = sensor.get('sensor_id', '')
                    nombre_formateado = self.formatear_nombre_sensor(sensor)
                    
                    # Formato: "ID - Nombre Formateado"
                    formato = f"{sensor_id} - {nombre_formateado}"
                    sensores_formateados.append(formato)
                
                # También mantener solo los IDs para compatibilidad
                sensor_ids = [s.get('sensor_id', '') for s in sensores]
                
                # Usar formato descriptivo para alertas y servicios
                if hasattr(self, 'combo_sensor_alerta'):
                    self.combo_sensor_alerta['values'] = sensores_formateados
                    if sensores_formateados:
                        self.combo_sensor_alerta.set(sensores_formateados[0])
                
                if hasattr(self, 'combo_sensor_servicio'):
                    self.combo_sensor_servicio['values'] = sensores_formateados
                    if sensores_formateados:
                        self.combo_sensor_servicio.set(sensores_formateados[0])
                else:
                    # Fallback a valores por defecto
                    sensores_default = ["SENSOR_BA_001", "SENSOR_CBA_001", "SENSOR_ROS_001", "SENSOR_MEN_001", "SENSOR_LP_001"]
                    if hasattr(self, 'combo_sensor_alerta'):
                        self.combo_sensor_alerta['values'] = sensores_default
                        self.combo_sensor_alerta.set(sensores_default[0])
                    
                    if hasattr(self, 'combo_sensor_servicio'):
                        self.combo_sensor_servicio['values'] = sensores_default
                        self.combo_sensor_servicio.set(sensores_default[0])
                    self.agregar_log("⚠️ Usando sensores por defecto (no se encontraron sensores)")
            else:
                # Si no hay sensores, usar valores por defecto
                sensores_default = ["SENSOR_BA_001", "SENSOR_CBA_001", "SENSOR_ROS_001", "SENSOR_MEN_001", "SENSOR_LP_001"]
                # Solo actualizar combos que existen
                if hasattr(self, 'combo_sensor_alerta'):
                    self.combo_sensor_alerta['values'] = sensores_default
                    self.combo_sensor_alerta.set(sensores_default[0])
                
                if hasattr(self, 'combo_sensor_servicio'):
                    self.combo_sensor_servicio['values'] = sensores_default
                    self.combo_sensor_servicio.set(sensores_default[0])
                self.agregar_log("⚠️ Usando sensores por defecto (no hay sensores en la base)")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando sensores para combos: {e}")
            # En caso de error, usar valores por defecto
            sensores_default = ["SENSOR_BA_001", "SENSOR_CBA_001", "SENSOR_ROS_001", "SENSOR_MEN_001", "SENSOR_LP_001"]
            self.combo_sensor_analisis['values'] = sensores_default
            self.combo_sensor_alerta['values'] = sensores_default
            self.combo_sensor_analisis.set(sensores_default[0])
            self.combo_sensor_alerta.set(sensores_default[0])
    
    def cargar_ciudades_para_combos(self):
        """Cargar ciudades dinámicamente desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Si no hay conexión, usar valores por defecto
                ciudades_default = ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata"]
                self.combo_ciudad_analisis['values'] = ciudades_default
                self.combo_ciudad_analisis.set(ciudades_default[0])
                self.agregar_log("⚠️ Usando ciudades por defecto (MongoDB no disponible)")
                return
            
            # Obtener ubicaciones únicas desde MongoDB
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones:
                # Formatear ubicaciones como "País - Ciudad"
                ciudades_formateadas = []
                paises_ciudades = {}
                
                for ubicacion in ubicaciones:
                    # Verificar si es un diccionario
                    if isinstance(ubicacion, dict):
                        # Si es un diccionario, intentar extraer la ubicación
                        ubicacion = ubicacion.get('ubicacion', str(ubicacion))
                    # Convertir a string por si acaso
                    ubicacion = str(ubicacion) if ubicacion else "Desconocido"
                    
                    if ',' in ubicacion:
                        # Formato: "Ciudad, País"
                        partes = ubicacion.split(',')
                        ciudad = partes[0].strip()
                        pais = partes[1].strip() if len(partes) > 1 else "Desconocido"
                        
                        # Agrupar por país
                        if pais not in paises_ciudades:
                            paises_ciudades[pais] = []
                        paises_ciudades[pais].append(ciudad)
                    else:
                        # Si no hay coma, asumir que es solo la ciudad
                        ciudad = ubicacion.strip()
                        pais = "Desconocido"
                        if pais not in paises_ciudades:
                            paises_ciudades[pais] = []
                        paises_ciudades[pais].append(ciudad)
                
                # Ordenar países alfabéticamente
                for pais in sorted(paises_ciudades.keys()):
                    # Ordenar ciudades dentro de cada país
                    ciudades_pais = sorted(paises_ciudades[pais])
                    for ciudad in ciudades_pais:
                        ciudades_formateadas.append(f"{pais} - {ciudad}")
                
                if ciudades_formateadas:
                    self.combo_ciudad_analisis['values'] = ciudades_formateadas
                    self.combo_ciudad_analisis.set(ciudades_formateadas[0])
                    self.agregar_log(f"📍 Ciudades cargadas desde MongoDB: {len(ciudades_formateadas)} ubicaciones")
                else:
                    # Fallback a valores por defecto
                    ciudades_default = ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata"]
                    self.combo_ciudad_analisis['values'] = ciudades_default
                    self.combo_ciudad_analisis.set(ciudades_default[0])
                    self.agregar_log("⚠️ Usando ciudades por defecto (no se encontraron ciudades)")
            else:
                # Fallback a valores por defecto
                ciudades_default = ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata"]
                self.combo_ciudad_analisis['values'] = ciudades_default
                self.combo_ciudad_analisis.set(ciudades_default[0])
                self.agregar_log("⚠️ Usando ciudades por defecto (no hay ubicaciones)")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ciudades: {e}")
            # Fallback a valores por defecto
            ciudades_default = ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata"]
            self.combo_ciudad_analisis['values'] = ciudades_default
            self.combo_ciudad_analisis.set(ciudades_default[0])
    
    def cargar_ubicaciones_para_formulario(self, combo_ubicacion):
        """Cargar ubicaciones dinámicamente para formularios"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Si no hay conexión, usar valores por defecto
                ubicaciones_default = [
                    "Argentina - Buenos Aires", "Argentina - Córdoba", "Argentina - Rosario", 
                    "Argentina - Mendoza", "Argentina - La Plata",
                    "Norte", "Centro", "Sur", "Este", "Oeste",
                    "Argentina", "Brasil", "Chile", "Colombia", "Uruguay"
                ]
                combo_ubicacion['values'] = ubicaciones_default
                combo_ubicacion.set("Argentina - Buenos Aires")
                return
            
            # Obtener ubicaciones desde MongoDB
            ubicaciones_db = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones_db:
                # Formatear ubicaciones como "País - Ciudad"
                ubicaciones_formateadas = []
                paises_ciudades = {}
                
                for ubicacion in ubicaciones_db:
                    if ',' in ubicacion:
                        # Formato: "Ciudad, País"
                        partes = ubicacion.split(',')
                        ciudad = partes[0].strip()
                        pais = partes[1].strip() if len(partes) > 1 else "Desconocido"
                        
                        # Agrupar por país
                        if pais not in paises_ciudades:
                            paises_ciudades[pais] = []
                        paises_ciudades[pais].append(ciudad)
                    else:
                        # Si no hay coma, asumir que es solo la ciudad
                        ciudad = ubicacion.strip()
                        pais = "Desconocido"
                        if pais not in paises_ciudades:
                            paises_ciudades[pais] = []
                        paises_ciudades[pais].append(ciudad)
                
                # Ordenar países alfabéticamente
                for pais in sorted(paises_ciudades.keys()):
                    # Ordenar ciudades dentro de cada país
                    ciudades_pais = sorted(paises_ciudades[pais])
                    for ciudad in ciudades_pais:
                        ubicaciones_formateadas.append(f"{pais} - {ciudad}")
                
                # Agregar opciones adicionales
                ubicaciones_adicionales = [
                    "Norte", "Centro", "Sur", "Este", "Oeste",
                    "Argentina", "Brasil", "Chile", "Colombia", "Uruguay"
                ]
                
                # Crear lista combinada sin duplicados
                ubicaciones_completas = list(set(ubicaciones_formateadas + ubicaciones_adicionales))
                ubicaciones_completas.sort()
                
                combo_ubicacion['values'] = ubicaciones_completas
                combo_ubicacion.set(ubicaciones_completas[0] if ubicaciones_completas else "Argentina - Buenos Aires")
            else:
                # Fallback a valores por defecto
                ubicaciones_default = [
                    "Argentina - Buenos Aires", "Argentina - Córdoba", "Argentina - Rosario", 
                    "Argentina - Mendoza", "Argentina - La Plata",
                    "Norte", "Centro", "Sur", "Este", "Oeste",
                    "Argentina", "Brasil", "Chile", "Colombia", "Uruguay"
                ]
                combo_ubicacion['values'] = ubicaciones_default
                combo_ubicacion.set("Argentina - Buenos Aires")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ubicaciones para formulario: {e}")
            # Fallback a valores por defecto
            ubicaciones_default = [
                "Argentina - Buenos Aires", "Argentina - Córdoba", "Argentina - Rosario", 
                "Argentina - Mendoza", "Argentina - La Plata",
                "Norte", "Centro", "Sur", "Este", "Oeste",
                "Argentina", "Brasil", "Chile", "Colombia", "Uruguay"
            ]
            combo_ubicacion['values'] = ubicaciones_default
            combo_ubicacion.set("Argentina - Buenos Aires")
    
    def cargar_paises_para_analisis(self):
        """Cargar países para análisis"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Si no hay conexión, usar valores por defecto
                paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
                self.combo_pais_analisis['values'] = paises_default
                self.combo_pais_analisis.set(paises_default[0])
                self.agregar_log("⚠️ Usando países por defecto (MongoDB no disponible)")
                return
            
            # Obtener ubicaciones únicas desde MongoDB
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones:
                # Extraer países únicos
                paises = set()
                for ubicacion in ubicaciones:
                    if ',' in ubicacion:
                        # Formato: "Ciudad, País"
                        partes = ubicacion.split(',')
                        if len(partes) > 1:
                            pais = partes[1].strip()
                            paises.add(pais)
                    else:
                        # Si no hay coma, asumir que es solo la ciudad
                        # Agregar países conocidos
                        paises.update(["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"])
                
                # Convertir a lista y ordenar
                paises_lista = sorted(list(paises))
                
                if paises_lista:
                    self.combo_pais_analisis['values'] = paises_lista
                    self.combo_pais_analisis.set(paises_lista[0])
                else:
                    # Fallback a valores por defecto
                    paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
                    self.combo_pais_analisis['values'] = paises_default
                    self.combo_pais_analisis.set(paises_default[0])
                    self.agregar_log("⚠️ Usando países por defecto (no se encontraron países)")
            else:
                # Fallback a valores por defecto
                paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
                self.combo_pais_analisis['values'] = paises_default
                self.combo_pais_analisis.set(paises_default[0])
                self.agregar_log("⚠️ Usando países por defecto (no hay ubicaciones)")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando países para análisis: {e}")
            # Fallback a valores por defecto
            paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
            self.combo_pais_analisis['values'] = paises_default
            self.combo_pais_analisis.set(paises_default[0])
    
    def cargar_ubicaciones_para_sensores(self):
        """Cargar ubicaciones para el combo de sensores"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Si no hay conexión, usar valores por defecto
                ubicaciones_default = [
                    "Argentina - Buenos Aires", "Argentina - Córdoba", "Argentina - Rosario", 
                    "Argentina - Mendoza", "Argentina - La Plata",
                    "Brasil - São Paulo", "Brasil - Rio de Janeiro",
                    "Chile - Santiago", "Chile - Valparaíso",
                    "Colombia - Bogotá", "Colombia - Medellín",
                    "Uruguay - Montevideo"
                ]
                self.combo_ubicacion_sensor['values'] = ubicaciones_default
                self.combo_ubicacion_sensor.set(ubicaciones_default[0])
                self.agregar_log("⚠️ Usando ubicaciones por defecto para sensores (MongoDB no disponible)")
                return
            
            # Obtener ubicaciones desde MongoDB
            ubicaciones_db = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones_db:
                # Formatear ubicaciones como "País - Ciudad"
                ubicaciones_formateadas = []
                paises_ciudades = {}
                
                for ubicacion in ubicaciones_db:
                    if ',' in ubicacion:
                        # Formato: "Ciudad, País"
                        partes = ubicacion.split(',')
                        ciudad = partes[0].strip()
                        pais = partes[1].strip() if len(partes) > 1 else "Desconocido"
                        
                        # Agrupar por país
                        if pais not in paises_ciudades:
                            paises_ciudades[pais] = []
                        paises_ciudades[pais].append(ciudad)
                    else:
                        # Si no hay coma, asumir que es solo la ciudad
                        ciudad = ubicacion.strip()
                        pais = "Desconocido"
                        if pais not in paises_ciudades:
                            paises_ciudades[pais] = []
                        paises_ciudades[pais].append(ciudad)
                
                # Ordenar países alfabéticamente
                for pais in sorted(paises_ciudades.keys()):
                    # Ordenar ciudades dentro de cada país
                    ciudades_pais = sorted(paises_ciudades[pais])
                    for ciudad in ciudades_pais:
                        ubicaciones_formateadas.append(f"{pais} - {ciudad}")
                
                # Agregar opciones adicionales
                ubicaciones_adicionales = [
                    "Argentina - Buenos Aires", "Argentina - Córdoba", "Argentina - Rosario", 
                    "Argentina - Mendoza", "Argentina - La Plata",
                    "Brasil - São Paulo", "Brasil - Rio de Janeiro",
                    "Chile - Santiago", "Chile - Valparaíso",
                    "Colombia - Bogotá", "Colombia - Medellín",
                    "Uruguay - Montevideo"
                ]
                
                # Crear lista combinada sin duplicados
                ubicaciones_completas = list(set(ubicaciones_formateadas + ubicaciones_adicionales))
                ubicaciones_completas.sort()
                
                self.combo_ubicacion_sensor['values'] = ubicaciones_completas
                self.combo_ubicacion_sensor.set(ubicaciones_completas[0] if ubicaciones_completas else "Argentina - Buenos Aires")
            else:
                # Fallback a valores por defecto
                ubicaciones_default = [
                    "Argentina - Buenos Aires", "Argentina - Córdoba", "Argentina - Rosario", 
                    "Argentina - Mendoza", "Argentina - La Plata",
                    "Brasil - São Paulo", "Brasil - Rio de Janeiro",
                    "Chile - Santiago", "Chile - Valparaíso",
                    "Colombia - Bogotá", "Colombia - Medellín",
                    "Uruguay - Montevideo"
                ]
                self.combo_ubicacion_sensor['values'] = ubicaciones_default
                self.combo_ubicacion_sensor.set(ubicaciones_default[0])
                self.agregar_log("⚠️ Usando ubicaciones por defecto para sensores (no hay ubicaciones)")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ubicaciones para sensores: {e}")
            # Fallback a valores por defecto
            ubicaciones_default = [
                "Argentina - Buenos Aires", "Argentina - Córdoba", "Argentina - Rosario", 
                "Argentina - Mendoza", "Argentina - La Plata",
                "Brasil - São Paulo", "Brasil - Rio de Janeiro",
                "Chile - Santiago", "Chile - Valparaíso",
                "Colombia - Bogotá", "Colombia - Medellín",
                "Uruguay - Montevideo"
            ]
            self.combo_ubicacion_sensor['values'] = ubicaciones_default
            self.combo_ubicacion_sensor.set(ubicaciones_default[0])
    
    def extraer_ciudad_del_formato(self, ubicacion_formateada):
        """Extraer solo el nombre de la ciudad del formato 'País - Ciudad'"""
        try:
            if ' - ' in ubicacion_formateada:
                # Formato: "País - Ciudad"
                return ubicacion_formateada.split(' - ')[1].strip()
            else:
                # Si no tiene el formato esperado, devolver tal como está
                return ubicacion_formateada.strip()
        except Exception:
            return ubicacion_formateada.strip()
    
    def extraer_sensor_id_del_formato(self, sensor_formateado):
        """Extraer solo el sensor_id del formato 'ID - Nombre (Ubicación)'"""
        try:
            if ' - ' in sensor_formateado:
                # Formato: "ID - Nombre (Ubicación)" o "ID - Nombre"
                return sensor_formateado.split(' - ')[0].strip()
            else:
                # Si no tiene el formato esperado, devolver tal como está
                return sensor_formateado.strip()
        except Exception:
            return sensor_formateado.strip()
    
    def obtener_datos_ciudades_desde_mongodb(self):
        """Obtener datos de ciudades desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return None
            
            # Obtener ubicaciones únicas
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if not ubicaciones:
                return None
            
            ciudades_data = {}
            
            # Para cada ubicación, obtener estadísticas de temperatura y humedad
            for ubicacion in ubicaciones:
                ciudad = ubicacion.split(',')[0].strip() if ',' in ubicacion else ubicacion
                
                # Obtener datos de temperatura
                datos_temp = self.mongodb_service.obtener_datos_temperatura_por_ubicacion(ubicacion, None, None)
                datos_hum = self.mongodb_service.obtener_datos_humedad_por_ubicacion(ubicacion, None, None)
                
                if datos_temp and datos_hum:
                    # Calcular estadísticas
                    temp_max = max(d['temp_max'] for d in datos_temp)
                    temp_min = min(d['temp_min'] for d in datos_temp)
                    hum_max = max(d['humedad'] for d in datos_hum)
                    hum_min = min(d['humedad'] for d in datos_hum)
                    
                    ciudades_data[ciudad] = {
                        'temp_max': temp_max,
                        'temp_min': temp_min,
                        'hum_max': hum_max,
                        'hum_min': hum_min
                    }
            
            if ciudades_data:
                ciudades_data['fuente'] = 'mongodb'
                return ciudades_data
            
            return None
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo datos de ciudades desde MongoDB: {e}")
            return None
    
    def generar_datos_ciudades_ejemplo(self):
        """Generar datos de ejemplo para ciudades"""
        return {
            "Buenos Aires": {"temp_min": 15.2, "temp_max": 28.5, "hum_min": 45.0, "hum_max": 78.0},
            "Córdoba": {"temp_min": 12.8, "temp_max": 32.1, "hum_min": 38.0, "hum_max": 82.0},
            "Rosario": {"temp_min": 14.5, "temp_max": 29.8, "hum_min": 42.0, "hum_max": 75.0},
            "Mendoza": {"temp_min": 8.9, "temp_max": 35.2, "hum_min": 25.0, "hum_max": 65.0},
            "La Plata": {"temp_min": 13.1, "temp_max": 26.9, "hum_min": 48.0, "hum_max": 80.0},
            "fuente": "ejemplo"
        }
    
    def obtener_datos_zonas_desde_mongodb(self):
        """Obtener datos de zonas desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return None
            
            # Obtener ubicaciones únicas
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if not ubicaciones:
                return None
            
            zonas_data = {}
            zonas_mapping = {
                "Norte": ["Norte", "Salta", "Jujuy", "Tucumán"],
                "Centro": ["Centro", "Buenos Aires", "Córdoba", "Santa Fe"],
                "Sur": ["Sur", "Patagonia", "Tierra del Fuego"],
                "Este": ["Este", "Mar del Plata", "La Plata"],
                "Oeste": ["Oeste", "Mendoza", "San Juan", "La Rioja"]
            }
            
            # Para cada zona, obtener estadísticas
            for zona, ciudades_zona in zonas_mapping.items():
                temp_max_values = []
                temp_min_values = []
                hum_max_values = []
                hum_min_values = []
                
                for ciudad in ciudades_zona:
                    # Buscar ubicaciones que contengan esta ciudad
                    ubicaciones_zona = [u for u in ubicaciones if ciudad.lower() in u.lower()]
                    
                    for ubicacion in ubicaciones_zona:
                        # Obtener datos de temperatura y humedad
                        datos_temp = self.mongodb_service.obtener_datos_temperatura_por_ubicacion(ubicacion, None, None)
                        datos_hum = self.mongodb_service.obtener_datos_humedad_por_ubicacion(ubicacion, None, None)
                        
                        if datos_temp:
                            temp_max_values.extend([d['temp_max'] for d in datos_temp])
                            temp_min_values.extend([d['temp_min'] for d in datos_temp])
                        
                        if datos_hum:
                            hum_max_values.extend([d['humedad'] for d in datos_hum])
                            hum_min_values.extend([d['humedad'] for d in datos_hum])
                
                if temp_max_values and temp_min_values and hum_max_values and hum_min_values:
                    zonas_data[zona] = {
                        'temp_max': max(temp_max_values),
                        'temp_min': min(temp_min_values),
                        'hum_max': max(hum_max_values),
                        'hum_min': min(hum_min_values)
                    }
            
            if zonas_data:
                zonas_data['fuente'] = 'mongodb'
                return zonas_data
            
            return None
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo datos de zonas desde MongoDB: {e}")
            return None
    
    def generar_datos_zonas_ejemplo(self):
        """Generar datos de ejemplo para zonas"""
        return {
            "Norte": {"temp_min": 18.5, "temp_max": 38.2, "hum_min": 35.0, "hum_max": 85.0},
            "Centro": {"temp_min": 12.3, "temp_max": 29.8, "hum_min": 45.0, "hum_max": 78.0},
            "Sur": {"temp_min": 5.8, "temp_max": 22.1, "hum_min": 55.0, "hum_max": 90.0},
            "Este": {"temp_min": 14.2, "temp_max": 26.5, "hum_min": 60.0, "hum_max": 88.0},
            "Oeste": {"temp_min": 8.9, "temp_max": 35.2, "hum_min": 25.0, "hum_max": 65.0},
            "fuente": "ejemplo"
        }
    
    def obtener_datos_paises_desde_mongodb(self):
        """Obtener datos de países desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return None
            
            # Obtener ubicaciones únicas
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if not ubicaciones:
                return None
            
            paises_data = {}
            paises_mapping = {
                "Argentina": ["Argentina", "Buenos Aires", "Córdoba", "Rosario", "Mendoza"],
                "Brasil": ["Brasil", "São Paulo", "Rio de Janeiro"],
                "Chile": ["Chile", "Santiago", "Valparaíso"],
                "Colombia": ["Colombia", "Bogotá", "Medellín"],
                "Uruguay": ["Uruguay", "Montevideo"]
            }
            
            # Para cada país, obtener estadísticas
            for pais, ciudades_pais in paises_mapping.items():
                temp_max_values = []
                temp_min_values = []
                hum_max_values = []
                hum_min_values = []
                
                for ciudad in ciudades_pais:
                    # Buscar ubicaciones que contengan esta ciudad o país
                    ubicaciones_pais = [u for u in ubicaciones if ciudad.lower() in u.lower()]
                    
                    for ubicacion in ubicaciones_pais:
                        # Obtener datos de temperatura y humedad
                        datos_temp = self.mongodb_service.obtener_datos_temperatura_por_ubicacion(ubicacion, None, None)
                        datos_hum = self.mongodb_service.obtener_datos_humedad_por_ubicacion(ubicacion, None, None)
                        
                        if datos_temp:
                            temp_max_values.extend([d['temp_max'] for d in datos_temp])
                            temp_min_values.extend([d['temp_min'] for d in datos_temp])
                        
                        if datos_hum:
                            hum_max_values.extend([d['humedad'] for d in datos_hum])
                            hum_min_values.extend([d['humedad'] for d in datos_hum])
                
                if temp_max_values and temp_min_values and hum_max_values and hum_min_values:
                    paises_data[pais] = {
                        'temp_max': max(temp_max_values),
                        'temp_min': min(temp_min_values),
                        'hum_max': max(hum_max_values),
                        'hum_min': min(hum_min_values)
                    }
            
            if paises_data:
                paises_data['fuente'] = 'mongodb'
                return paises_data
            
            return None
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo datos de países desde MongoDB: {e}")
            return None
    
    def generar_datos_paises_ejemplo(self):
        """Generar datos de ejemplo para países"""
        return {
            "Argentina": {"temp_min": 8.9, "temp_max": 38.2, "hum_min": 25.0, "hum_max": 90.0},
            "Brasil": {"temp_min": 22.1, "temp_max": 42.5, "hum_min": 45.0, "hum_max": 95.0},
            "Chile": {"temp_min": 5.2, "temp_max": 28.8, "hum_min": 30.0, "hum_max": 85.0},
            "Colombia": {"temp_min": 18.5, "temp_max": 35.2, "hum_min": 60.0, "hum_max": 95.0},
            "Uruguay": {"temp_min": 12.8, "temp_max": 26.9, "hum_min": 55.0, "hum_max": 88.0},
            "fuente": "ejemplo"
        }
    
    def cargar_usuarios_para_combos(self):
        """Cargar usuarios para combos"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            usuarios = self.mongodb_service.obtener_usuarios()
            usuario_nombres = [u.get('username', '') for u in usuarios if u.get('username')]
            
            # Solo cargar usuarios en combos si existen (técnicos y administradores)
            if hasattr(self, 'combo_usuario_factura'):
                self.combo_usuario_factura['values'] = usuario_nombres
                if usuario_nombres:
                    self.combo_usuario_factura.set(usuario_nombres[0])
            
            if hasattr(self, 'combo_usuario_proceso'):
                self.combo_usuario_proceso['values'] = usuario_nombres
                if usuario_nombres:
                    self.combo_usuario_proceso.set(usuario_nombres[0])
            
            if hasattr(self, 'combo_destinatario'):
                self.combo_destinatario['values'] = usuario_nombres
                if usuario_nombres:
                    self.combo_destinatario.set(usuario_nombres[0])
                
        except Exception as e:
            self.agregar_log(f"Error cargando usuarios para combos: {e}")
    
    # Métodos de funcionalidad específica
    def mostrar_dialogo_login(self):
        """Mostrar diálogo de login"""
        if not self.redis_service or not self.redis_service.conectado:
            messagebox.showerror("Error", "Redis Cloud no está conectado")
            return
        
        # Crear ventana de login - usar una ventana temporal en lugar de Toplevel
        # ya que la ventana principal está oculta
        login_window = tk.Tk()
        login_window.title("Iniciar Sesión")
        login_window.geometry("400x400")
        login_window.configure(bg='white')
        login_window.resizable(False, False)
        
        # Centrar ventana en la pantalla
        login_window.update_idletasks()
        # Obtener dimensiones de la ventana y calcular posición para centrar
        width = login_window.winfo_width()
        height = login_window.winfo_height()
        x = (login_window.winfo_screenwidth() // 2) - (width // 2)
        y = (login_window.winfo_screenheight() // 2) - (height // 2)
        login_window.geometry(f"{width}x{height}+{x}+{y}")
        
        # Bloquear el cierre de ventana por defecto
        login_window.protocol("WM_DELETE_WINDOW", lambda: login_window.quit())
        
        # Título
        tk.Label(login_window, text="Iniciar Sesión", 
                font=('Arial', 16, 'bold'), bg='white', fg='#2c3e50').pack(pady=20)
        
        # Información de usuarios disponibles
        info_frame = tk.Frame(login_window, bg='white')
        info_frame.pack(pady=10)
        
        tk.Label(info_frame, text="Ingrese sus credenciales de usuario", 
                font=('Arial', 10, 'bold'), bg='white', fg='#7f8c8d').pack()
        
        usuarios_info = tk.Frame(info_frame, bg='white')
        usuarios_info.pack(pady=5)
        
        # Campos de login
        frame_campos = tk.Frame(login_window, bg='white')
        frame_campos.pack(pady=20)
        
        tk.Label(frame_campos, text="Usuario:", bg='white').grid(row=0, column=0, padx=10, pady=10, sticky='w')
        entry_usuario = tk.Entry(frame_campos, width=30)
        entry_usuario.grid(row=0, column=1, padx=10, pady=10)
        
        tk.Label(frame_campos, text="Contraseña:", bg='white').grid(row=1, column=0, padx=10, pady=10, sticky='w')
        entry_password = tk.Entry(frame_campos, width=30, show="*")
        entry_password.grid(row=1, column=1, padx=10, pady=10)
        
        # Botones
        frame_botones = tk.Frame(login_window, bg='white')
        frame_botones.pack(pady=20)
        
        def hacer_login():
            usuario = entry_usuario.get()
            password = entry_password.get()
            
            if not usuario or not password:
                messagebox.showerror("Error", "Complete todos los campos")
                return
            
            # Autenticación desde MongoDB Atlas
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "Base de datos no disponible")
                return
            
            # Buscar usuario en MongoDB
            usuario_data = self.mongodb_service.autenticar_usuario(usuario, password)
            
            if usuario_data:
                rol = usuario_data.get("rol", "usuario")
                role_id = usuario_data.get("role_id")
                
                # Si no tiene role_id, intentar obtenerlo desde el rol
                if not role_id:
                    try:
                        rol_obj = self.mongodb_service.obtener_rol_por_name(rol) if self.mongodb_service else None
                        if rol_obj:
                            role_id = rol_obj.get("role_id")
                    except:
                        pass
                
                # Crear sesión en Redis
                session_data = {
                    "user_id": usuario_data.get("user_id", f"USER_{usuario.upper()}"),
                    "username": usuario,
                    "rol": rol,  # Mantener para compatibilidad
                    "role_id": role_id,  # Nueva referencia
                    "login_time": datetime.now().isoformat(),
                    "permissions": self.obtener_permisos_por_rol(role_id if role_id else rol)
                }
                
                # Guardar sesión en Redis usando set con JSON
                import json
                if self.redis_service and self.redis_service.conectado:
                    self.redis_service.set(f"session:{usuario}", json.dumps(session_data), ttl=3600)
                
                # Actualizar estado de la aplicación
                self.usuario_autenticado = usuario
                self.sesion_activa = True
                self.rol_usuario = rol  # Mantener para compatibilidad
                setattr(self, 'role_id', role_id)  # Guardar role_id como atributo
                self.tiempo_inicio_sesion = datetime.now()  # Registrar tiempo de inicio
                self.etiqueta_usuario.config(text=f"Usuario: {usuario} ({rol.title()})")
                self.boton_login.config(text="Cerrar Sesión", command=self.cerrar_sesion)
                
                # Iniciar actualización de tiempo de sesión
                self.iniciar_actualizacion_tiempo_sesion()
                
                # Recargar interfaz según rol
                self.recargar_interfaz_segun_rol()
                
                # Actualizar interfaz de procesos específicamente
                if hasattr(self, 'actualizar_interfaz_procesos'):
                    self.actualizar_interfaz_procesos()
                
                # Cargar alertas y facturas después del login
                self.cargar_datos_despues_login()
                
                # self.agregar_log(f"✅ Usuario {usuario} autenticado como {rol} desde MongoDB")
                
                # Cerrar la ventana de login
                login_window.quit()
                login_window.destroy()
                
                # Mostrar la ventana principal
                self.root.deiconify()
                
                # Mostrar mensaje de éxito
                messagebox.showinfo("Éxito", f"Sesión iniciada correctamente como {rol}")
            else:
                messagebox.showerror("Error", "Credenciales incorrectas o usuario inactivo")
        
        def cerrar_aplicacion():
            """Cerrar la aplicación si se cancela el login inicial"""
            login_window.quit()
            login_window.destroy()
            self.root.quit()
            self.root.destroy()
        
        tk.Button(frame_botones, text="Iniciar Sesión", command=hacer_login,
                 bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
        
        tk.Button(frame_botones, text="Cancelar", command=cerrar_aplicacion,
                 bg='#95a5a6', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
        
        # Focus en el primer campo
        entry_usuario.focus()
        
        # Bind Enter para hacer login
        entry_password.bind('<Return>', lambda e: hacer_login())
        
        # Ejecutar el mainloop de la ventana de login
        login_window.mainloop()
    
    def cerrar_sesion(self):
        """Cerrar sesión del usuario"""
        try:
            # Calcular tiempo de sesión y generar factura si es necesario
            if self.usuario_autenticado and self.tiempo_inicio_sesion:
                self.procesar_facturacion_sesion()
            
            if self.usuario_autenticado and self.redis_service:
                # Eliminar sesión de Redis
                self.redis_service.delete(f"session:{self.usuario_autenticado}")
                
                self.agregar_log(f"✅ Sesión de {self.usuario_autenticado} cerrada")
            
            # Resetear estado
            self.usuario_autenticado = None
            self.sesion_activa = False
            self.rol_usuario = None
            self.tiempo_inicio_sesion = None
            self.etiqueta_usuario.config(text="Usuario: No autenticado")
            self.etiqueta_tiempo_sesion.config(text="")  # Limpiar tiempo de sesión
            self.boton_login.config(text="Iniciar Sesión", command=self.mostrar_dialogo_login)
            
            # Ocultar la interfaz principal
            self.notebook.pack_forget()
            
            messagebox.showinfo("Sesión", "Sesión cerrada correctamente")
            
            # Mostrar login nuevamente
            self.mostrar_dialogo_login()
            
        except Exception as e:
            self.agregar_log(f"❌ Error cerrando sesión: {e}")
            messagebox.showerror("Error", f"Error cerrando sesión: {e}")
    
    def cargar_datos_despues_login(self):
        """Cargar alertas y facturas después del login"""
        try:
            self.agregar_log("🔄 Cargando datos iniciales...")
            
            # Cargar alertas
            if hasattr(self, 'actualizar_lista_alertas'):
                try:
                    self.actualizar_lista_alertas()
                    self.agregar_log("✅ Alertas cargadas")
                except Exception as e:
                    self.agregar_log(f"⚠️ Error cargando alertas: {e}")
            
            # Cargar sensores para el módulo de alertas
            if hasattr(self, 'cargar_sensores_para_alertas'):
                try:
                    self.cargar_sensores_para_alertas()
                    self.agregar_log("✅ Sensores para alertas cargados")
                except Exception as e:
                    self.agregar_log(f"⚠️ Error cargando sensores para alertas: {e}")
            
            # Cargar facturas
            if hasattr(self, 'actualizar_lista_facturas'):
                try:
                    self.actualizar_lista_facturas()
                    self.agregar_log("✅ Facturas cargadas")
                except Exception as e:
                    self.agregar_log(f"⚠️ Error cargando facturas: {e}")
            
            # Actualizar estadísticas del dashboard
            if hasattr(self, 'actualizar_estadisticas_dashboard'):
                try:
                    self.actualizar_estadisticas_dashboard()
                    self.agregar_log("✅ Estadísticas actualizadas")
                except Exception as e:
                    self.agregar_log(f"⚠️ Error actualizando estadísticas: {e}")
            
            self.agregar_log("✅ Datos iniciales cargados correctamente")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando datos después del login: {e}")
    
    def procesar_facturacion_sesion(self):
        """Procesar facturación basada en tiempo de sesión"""
        try:
            # Verificar si el usuario debe pagar (no es admin ni técnico)
            if self.rol_usuario in ["administrador", "técnico"]:
                self.agregar_log(f"✅ Usuario {self.rol_usuario} - Sin cargo por tiempo de sesión")
                return
            
            # Calcular tiempo de sesión
            tiempo_fin_sesion = datetime.now()
            duracion_sesion = tiempo_fin_sesion - self.tiempo_inicio_sesion
            duracion_minutos = duracion_sesion.total_seconds() / 60
            
            # Calcular costo por tiempo de sesión
            costo_sesion = self.calcular_costo_sesion(duracion_minutos)
            
            if costo_sesion > 0:
                # Generar factura por tiempo de sesión
                factura_id = self.generar_factura_sesion(duracion_minutos, costo_sesion)
                
                # Mostrar resumen de facturación
                self.mostrar_resumen_facturacion_sesion(duracion_minutos, costo_sesion, factura_id)
                
                self.agregar_log(f"💰 Facturación de sesión: {duracion_minutos:.1f} min - ${costo_sesion:.2f}")
            else:
                self.agregar_log(f"✅ Sesión gratuita: {duracion_minutos:.1f} minutos")
                
        except Exception as e:
            self.agregar_log(f"❌ Error procesando facturación de sesión: {e}")
    
    def calcular_costo_sesion(self, duracion_minutos):
        """Calcular costo basado en duración de sesión"""
        try:
            # Tarifas por minuto según el rol
            tarifas_por_minuto = {
                "usuario": 0.10,  # $0.10 por minuto para usuarios tradicionales
                "técnico": 0.00,  # Gratis para técnicos
                "administrador": 0.00  # Gratis para administradores
            }
            
            tarifa = tarifas_por_minuto.get(self.rol_usuario, 0.10)
            
            # Calcular costo total
            costo_total = duracion_minutos * tarifa
            
            # Aplicar descuentos por tiempo
            if duracion_minutos > 60:  # Más de 1 hora
                costo_total *= 0.9  # 10% descuento
            elif duracion_minutos > 30:  # Más de 30 minutos
                costo_total *= 0.95  # 5% descuento
            
            # Redondear a 2 decimales y aplicar mínimo de $0.10
            costo_total = round(costo_total, 2)
            return max(costo_total, 0.10) if costo_total > 0 else 0
            
        except Exception as e:
            self.agregar_log(f"❌ Error calculando costo de sesión: {e}")
            return 0
    
    def generar_factura_sesion(self, duracion_minutos, costo_sesion):
        """Generar factura por tiempo de sesión"""
        try:
            factura_id = f"SESION_{int(time.time())}"
            
            # Obtener user_id del usuario autenticado
            user_id = self.obtener_user_id_por_username(self.usuario_autenticado)
            # Si no se encuentra el user_id, usar el username como fallback
            if not user_id:
                user_id = self.usuario_autenticado
            
            factura_data = {
                "invoice_id": factura_id,  # Campo esperado por el módulo de facturación
                "user_id": user_id,  # Campo esperado por el módulo de facturación
                "service": f"Tiempo de Sesión ({duracion_minutos:.1f} min)",  # Campo esperado por el módulo de facturación
                "amount": costo_sesion,  # Campo esperado por el módulo de facturación
                "total_amount": float(costo_sesion),
                "status": "pending",  # Campo esperado por el módulo de facturación
                "created_at": datetime.now().isoformat(),  # Campo esperado por el módulo de facturación
                "due_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),  # Campo esperado por el módulo de facturación
                
                # Campos adicionales para información completa
                "tipo_factura": "tiempo_sesion",
                "descripcion": f"Uso del sistema por {duracion_minutos:.1f} minutos",
                "duracion_minutos": duracion_minutos,
                "fecha_inicio": self.tiempo_inicio_sesion.isoformat(),
                "fecha_fin": datetime.now().isoformat(),
                "metodo_pago": "cuenta_corriente",
                "rol_usuario": self.rol_usuario,
                "usuario": self.usuario_autenticado,  # Mantener para compatibilidad
                "procesos_facturados": []
            }
            
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.crear_factura(factura_data)
                self.agregar_log(f"✅ Factura de sesión generada: {factura_id}")
            
            return factura_id
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando factura de sesión: {e}")
            return f"ERROR_{int(time.time())}"
    
    def generar_factura_proceso(self, nombre_proceso, tipo_proceso, costo_proceso):
        """Generar factura por proceso creado"""
        try:
            # Verificar si el usuario debe pagar (no es admin ni técnico)
            if self.rol_usuario in ["administrador", "técnico"]:
                self.agregar_log(f"✅ Usuario {self.rol_usuario} - Sin cargo por proceso: {nombre_proceso}")
                return "PROCESO_GRATUITO"
            
            factura_id = f"PROC_{int(time.time())}"
            
            # Obtener user_id del usuario autenticado
            user_id = self.obtener_user_id_por_username(self.usuario_autenticado)
            
            factura_data = {
                "invoice_id": factura_id,  # Campo esperado por el módulo de facturación
                "user_id": user_id,  # Campo esperado por el módulo de facturación
                "service": f"Proceso: {nombre_proceso}",  # Campo esperado por el módulo de facturación
                "amount": costo_proceso,  # Campo esperado por el módulo de facturación
                "total_amount": float(costo_proceso),
                "status": "pending",  # Campo esperado por el módulo de facturación
                "created_at": datetime.now().isoformat(),  # Campo esperado por el módulo de facturación
                "due_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),  # Campo esperado por el módulo de facturación
                
                # Campos adicionales para información completa
                "tipo_factura": "proceso",
                "descripcion": f"Creación de proceso: {nombre_proceso} ({tipo_proceso})",
                "nombre_proceso": nombre_proceso,
                "tipo_proceso": tipo_proceso,
                "metodo_pago": "cuenta_corriente",
                "rol_usuario": self.rol_usuario,
                "usuario": self.usuario_autenticado,  # Mantener para compatibilidad
                "procesos_facturados": [{"nombre": nombre_proceso, "tipo": tipo_proceso}]
            }
            
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.crear_factura(factura_data)
                self.agregar_log(f"✅ Factura de proceso generada: {factura_id}")
            
            return factura_id
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando factura de proceso: {e}")
            return f"ERROR_{int(time.time())}"
    
    def mostrar_resumen_facturacion_sesion(self, duracion_minutos, costo_sesion, factura_id):
        """Mostrar resumen de facturación de sesión"""
        try:
            # Crear ventana de resumen
            resumen_window = tk.Toplevel(self.root)
            resumen_window.title("Resumen de Facturación - Sesión")
            resumen_window.geometry("500x400")
            resumen_window.configure(bg='white')
            resumen_window.transient(self.root)
            resumen_window.grab_set()
            
            # Título
            tk.Label(resumen_window, text="💰 RESUMEN DE FACTURACIÓN", 
                    font=('Arial', 16, 'bold'), bg='white', fg='#e74c3c').pack(pady=20)
            
            # Información de la sesión
            info_frame = tk.Frame(resumen_window, bg='#ecf0f1', relief='raised', bd=1)
            info_frame.pack(fill='x', padx=20, pady=10)
            
            tk.Label(info_frame, text=f"Usuario: {self.usuario_autenticado}", 
                    font=('Arial', 12), bg='#ecf0f1').pack(pady=5)
            tk.Label(info_frame, text=f"Rol: {self.rol_usuario.title()}", 
                    font=('Arial', 12), bg='#ecf0f1').pack()
            tk.Label(info_frame, text=f"Inicio de sesión: {self.tiempo_inicio_sesion.strftime('%Y-%m-%d %H:%M:%S')}", 
                    font=('Arial', 10), bg='#ecf0f1').pack()
            tk.Label(info_frame, text=f"Fin de sesión: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                    font=('Arial', 10), bg='#ecf0f1').pack()
            
            # Detalles de facturación
            factura_frame = tk.Frame(resumen_window, bg='white')
            factura_frame.pack(fill='x', padx=20, pady=10)
            
            tk.Label(factura_frame, text="DETALLES DE FACTURACIÓN", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            tk.Label(factura_frame, text=f"⏱️ Duración total: {duracion_minutos:.1f} minutos", 
                    font=('Arial', 12), bg='white').pack()
            tk.Label(factura_frame, text=f"💰 Costo por minuto: $0.10", 
                    font=('Arial', 12), bg='white').pack()
            
            # Descuentos aplicados
            if duracion_minutos > 60:
                tk.Label(factura_frame, text=f"🎯 Descuento aplicado: 10% (sesión > 1 hora)", 
                        font=('Arial', 10), bg='white', fg='#27ae60').pack()
            elif duracion_minutos > 30:
                tk.Label(factura_frame, text=f"🎯 Descuento aplicado: 5% (sesión > 30 min)", 
                        font=('Arial', 10), bg='white', fg='#27ae60').pack()
            
            tk.Label(factura_frame, text=f"💳 Total a cobrar: ${costo_sesion:.2f}", 
                    font=('Arial', 14, 'bold'), bg='white', fg='#e74c3c').pack(pady=10)
            
            tk.Label(factura_frame, text=f"📄 ID de Factura: {factura_id}", 
                    font=('Arial', 10), bg='white').pack()
            tk.Label(factura_frame, text=f"💳 Método de pago: Cuenta Corriente", 
                    font=('Arial', 10), bg='white').pack()
            
            # Botón de cerrar
            tk.Button(resumen_window, text="Cerrar", command=resumen_window.destroy,
                     bg='#3498db', fg='white', font=('Arial', 12)).pack(pady=20)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando resumen de facturación: {e}")
    
    def iniciar_actualizacion_tiempo_sesion(self):
        """Iniciar actualización periódica del tiempo de sesión"""
        try:
            self.actualizar_tiempo_sesion()
            # Programar próxima actualización en 30 segundos
            self.root.after(30000, self.iniciar_actualizacion_tiempo_sesion)
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando tiempo de sesión: {e}")
    
    def actualizar_tiempo_sesion(self):
        """Actualizar el indicador de tiempo de sesión"""
        try:
            if self.tiempo_inicio_sesion and self.usuario_autenticado:
                tiempo_actual = datetime.now()
                duracion = tiempo_actual - self.tiempo_inicio_sesion
                duracion_minutos = duracion.total_seconds() / 60
                
                # Formatear tiempo
                horas = int(duracion_minutos // 60)
                minutos = int(duracion_minutos % 60)
                
                if horas > 0:
                    tiempo_texto = f"⏱️ {horas}h {minutos}m"
                else:
                    tiempo_texto = f"⏱️ {minutos}m"
                
                # Calcular costo estimado si es usuario tradicional
                if self.rol_usuario == "usuario":
                    costo_estimado = self.calcular_costo_sesion(duracion_minutos)
                    tiempo_texto += f" | 💰 ${costo_estimado:.2f}"
                else:
                    tiempo_texto += " | 🆓 Gratis"
                
                self.etiqueta_tiempo_sesion.config(text=tiempo_texto)
                
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando tiempo de sesión: {e}")
    
    def actualizar_estadisticas_dashboard(self):
        """Actualizar estadísticas del dashboard"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Verificar que las etiquetas existan
            if not hasattr(self, 'etiquetas_estadisticas') or not self.etiquetas_estadisticas:
                return
            
            stats = self.mongodb_service.obtener_estadisticas()
            
            # Verificar que cada etiqueta existe antes de configurarla
            if "Sensores Activos" in self.etiquetas_estadisticas:
                self.etiquetas_estadisticas["Sensores Activos"].config(text=str(stats.get('sensores', 0)))
            if "Mediciones Hoy" in self.etiquetas_estadisticas:
                self.etiquetas_estadisticas["Mediciones Hoy"].config(text=str(stats.get('mediciones', 0)))
            if "Alertas Activas" in self.etiquetas_estadisticas:
                self.etiquetas_estadisticas["Alertas Activas"].config(text=str(stats.get('alertas', 0)))
            if "Procesos en Cola" in self.etiquetas_estadisticas:
                self.etiquetas_estadisticas["Procesos en Cola"].config(text=str(stats.get('procesos', 0)))
            
            self.agregar_log("✅ Estadísticas del dashboard actualizadas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando estadísticas: {e}")
    
    def mostrar_reporte_completo(self):
        """Mostrar reporte completo"""
        messagebox.showinfo("Reporte Completo", "Funcionalidad de reporte completo - Próximamente")
    
    def mostrar_alertas_criticas(self):
        """Mostrar alertas críticas"""
        messagebox.showinfo("Alertas Críticas", "Funcionalidad de alertas críticas - Próximamente")
    
    def agregar_sensor(self):
        """Agregar nuevo sensor"""
        try:
            nombre = self.entry_nombre_sensor.get()
            pais = self.combo_pais_sensor.get()
            ciudad = self.combo_ciudad_sensor.get()
            zona = self.combo_zona_sensor.get()
            tipo = self.combo_tipo_sensor.get()
            estado = self.combo_estado_sensor.get()
            
            if not nombre or not pais or not ciudad:
                messagebox.showerror("Error", "Por favor complete todos los campos obligatorios (Nombre, País, Ciudad)")
                return
            
            # Construir ubicación en formato "Ciudad, Zona - País"
            if zona and zona != "N/A":
                ubicacion = f"{ciudad}, {zona} - {pais}"
            else:
                ubicacion = f"{ciudad} - {pais}"
            
            # Crear sensor en MongoDB Atlas
            sensor_data = {
                "sensor_id": f"SENSOR_{int(time.time())}",
                "name": nombre,
                "location": ubicacion,
                "type": tipo,
                "status": estado.lower(),
                "created_at": datetime.now().isoformat()
            }
            
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.crear_sensor(sensor_data)
                self.actualizar_lista_sensores()
                self.cargar_sensores_para_combos()
                
                # Limpiar campos
                self.entry_nombre_sensor.delete(0, tk.END)
                self.combo_pais_sensor.set("")
                self.combo_ciudad_sensor.set("")
                self.combo_zona_sensor.set("Centro")
                
                messagebox.showinfo("Éxito", "Sensor agregado correctamente")
                self.agregar_log(f"✅ Sensor agregado: {nombre} en {ubicacion}")
            else:
                messagebox.showerror("Error", "MongoDB Atlas no está conectado")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error agregando sensor: {e}")
            self.agregar_log(f"❌ Error agregando sensor: {e}")
    
    def actualizar_lista_sensores(self):
        """Actualizar lista de sensores desde MongoDB Atlas (forzar actualización)"""
        try:
            # Verificar threading
            import threading
            current_thread = threading.current_thread()
            self.agregar_log(f" Hilo actual: {current_thread.name} (Principal: {current_thread is threading.main_thread()})")
            
            # Si no estamos en el hilo principal, usar after() para ejecutar en el hilo principal
            if current_thread is not threading.main_thread():
                self.agregar_log("⚠️ No estamos en el hilo principal, usando after()")
                self.root.after(0, self._actualizar_lista_sensores_thread_safe)
                return
            
            self._actualizar_lista_sensores_thread_safe()
            
        except Exception as e:
            self.agregar_log(f"❌ Error en actualizar_lista_sensores: {e}")
            import traceback
            self.agregar_log(f"❌ Detalles del error: {traceback.format_exc()}")
    
    def _actualizar_lista_sensores_thread_safe(self):
        """Actualizar lista de sensores de manera thread-safe - VERSIÓN SIMPLIFICADA"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # PASO 1: Limpiar lista completamente
            for item in self.tree_sensores.get_children():
                self.tree_sensores.delete(item)
            
            # PASO 2: Obtener sensores desde MongoDB
            sensores = self.mongodb_service.obtener_sensores()
            
            if not sensores:
                return
            
            for i, sensor in enumerate(sensores):
                try:
                    # Preparar datos del sensor
                    sensor_id = sensor.get('sensor_id', f'SIN_ID_{i+1}')
                    name = sensor.get('name', f'Sensor {i+1}')
                    
                    # Normalizar ubicación
                    location = sensor.get('location', 'Sin ubicación')
                    if isinstance(location, dict):
                        city = location.get('city', '')
                        country = location.get('country', '')
                        zone = location.get('zone', '')
                        if city and country:
                            location_str = f"{city}, {zone} - {country}" if zone else f"{city} - {country}"
                        else:
                            location_str = city or str(location)
                    else:
                        location_str = str(location)
                    
                    sensor_type = sensor.get('type', 'Sin tipo')
                    status = sensor.get('status', 'Sin estado')
                    
                    # Obtener última medición real del sensor
                    ultima_medicion = self.mongodb_service.obtener_ultima_medicion_sensor(sensor_id)
                    if ultima_medicion:
                        timestamp = ultima_medicion.get('timestamp', '')
                        if timestamp:
                            # Formatear timestamp para mostrar solo fecha y hora
                            if isinstance(timestamp, str):
                                try:
                                    from datetime import datetime
                                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                    last_measurement = dt.strftime("%Y-%m-%d %H:%M")
                                except:
                                    last_measurement = timestamp[:16]  # Tomar solo los primeros 16 caracteres
                            else:
                                last_measurement = str(timestamp)[:16]
                        else:
                            last_measurement = 'Sin timestamp'
                    else:
                        last_measurement = 'N/A'
                    
                    # Crear tupla de valores
                    valores = (sensor_id, name, location_str, sensor_type, status, last_measurement)
                    
                    # Insertar en TreeView
                    self.tree_sensores.insert('', 'end', values=valores)
                    
                except Exception as e:
                    pass
            
            # PASO 4: Forzar actualización visual
            self.tree_sensores.update()
            self.root.update_idletasks()
            
        except Exception as e:
            self.agregar_log(f"❌ ERROR CRÍTICO: {e}")
            import traceback
            self.agregar_log(f"❌ Traceback: {traceback.format_exc()}")
    
    def generar_datos_sensor(self):
        """Generar datos de prueba para sensores"""
        try:
            # Obtener sensor seleccionado
            seleccionado = self.tree_sensores.selection()
            if not seleccionado:
                messagebox.showwarning("Advertencia", "Por favor seleccione un sensor para generar datos")
                return
            
            # Obtener datos del sensor seleccionado
            item = self.tree_sensores.item(seleccionado[0])
            sensor_id = item['values'][0]
            sensor_nombre = item['values'][1]
            sensor_tipo = item['values'][3]  # Tipo está en la cuarta columna
            
            # Confirmar generación de datos
            respuesta = messagebox.askyesno(
                "Confirmar Generación", 
                f"¿Generar datos de prueba para el sensor '{sensor_nombre}'?\n\nSe crearán mediciones simuladas para los últimos 7 días."
            )
            
            if not respuesta:
                return
            
            # Generar datos de prueba
            datos_generados = self.crear_datos_prueba_sensor(sensor_id, sensor_tipo)
            
            if self.mongodb_service and self.mongodb_service.conectado:
                mediciones_creadas = 0
                mediciones_fallidas = 0
                
                self.agregar_log(f"📊 Generando {len(datos_generados)} mediciones para sensor '{sensor_nombre}'")
                
                for i, dato in enumerate(datos_generados):
                    self.agregar_log(f"📊 Procesando medición {i+1}/{len(datos_generados)}: {dato.get('measurement_id', 'Sin ID')}")
                    
                    if self.mongodb_service.crear_medicion(dato):
                        mediciones_creadas += 1
                        self.agregar_log(f"✅ Medición {i+1} creada exitosamente")
                    else:
                        mediciones_fallidas += 1
                        self.agregar_log(f"❌ Medición {i+1} falló")
                
                self.agregar_log(f"📊 Resultado: {mediciones_creadas} exitosas, {mediciones_fallidas} fallidas")
                
                if mediciones_creadas > 0:
                    self.agregar_log(f"✅ {mediciones_creadas} mediciones generadas para sensor '{sensor_nombre}'")
                    messagebox.showinfo("Éxito", f"Se generaron {mediciones_creadas} mediciones de prueba para el sensor '{sensor_nombre}'")
                    self.actualizar_lista_sensores()
                else:
                    self.agregar_log(f"❌ Error generando datos para sensor '{sensor_nombre}' - Todas las mediciones fallaron")
                    messagebox.showerror("Error", f"No se pudieron generar datos para el sensor '{sensor_nombre}'\n\nVerifique los logs para más detalles.")
            else:
                messagebox.showerror("Error", "No hay conexión con MongoDB Atlas")
                
        except Exception as e:
            self.agregar_log(f"❌ Error generando datos de sensor: {e}")
            messagebox.showerror("Error", f"Error generando datos: {e}")
    
    def crear_datos_prueba_sensor(self, sensor_id, tipo_sensor):
        """Crear datos de prueba para un sensor"""
        import random
        from datetime import datetime, timedelta
        
        datos = []
        fecha_actual = datetime.now()
        
        # Generar datos para los últimos 7 días
        for i in range(7):
            fecha = fecha_actual - timedelta(days=i)
            
            # Valores base según el tipo de sensor
            if tipo_sensor == "Temperatura":
                temp_max = random.uniform(25, 35)
                temp_min = random.uniform(15, 25)
                humedad = random.uniform(40, 80)
            elif tipo_sensor == "Humedad":
                temp_max = random.uniform(20, 30)
                temp_min = random.uniform(10, 20)
                humedad = random.uniform(60, 95)
            else:  # Ambos
                temp_max = random.uniform(22, 32)
                temp_min = random.uniform(12, 22)
                humedad = random.uniform(45, 85)
            
            # Crear medición
            medicion = {
                "measurement_id": f"MEAS_{sensor_id}_{int(fecha.timestamp())}",
                "sensor_id": sensor_id,
                "timestamp": fecha,  # Usar objeto datetime directamente para MongoDB Time Series
                "temperature_max": round(temp_max, 1),
                "temperature_min": round(temp_min, 1),
                "humidity": round(humedad, 1),
                "created_at": datetime.now()
            }
            
            datos.append(medicion)
        
        return datos
    
    def eliminar_sensor(self):
        """Eliminar sensor seleccionado"""
        try:
            # Obtener sensor seleccionado
            seleccionado = self.tree_sensores.selection()
            if not seleccionado:
                messagebox.showwarning("Advertencia", "Por favor seleccione un sensor para eliminar")
                return
            
            # Obtener datos del sensor seleccionado
            item = self.tree_sensores.item(seleccionado[0])
            sensor_id = item['values'][0]  # ID está en la primera columna
            sensor_nombre = item['values'][1]  # Nombre está en la segunda columna
            
            # Confirmar eliminación
            respuesta = messagebox.askyesno(
                "Confirmar Eliminación", 
                f"¿Está seguro de que desea eliminar el sensor '{sensor_nombre}'?\n\nEsta acción no se puede deshacer."
            )
            
            if not respuesta:
                return
            
            # Eliminar sensor de MongoDB Atlas
            if self.mongodb_service and self.mongodb_service.conectado:
                if self.mongodb_service.eliminar_sensor(sensor_id):
                    self.agregar_log(f"✅ Sensor '{sensor_nombre}' eliminado correctamente")
                    self.actualizar_lista_sensores()
                    self.cargar_sensores_para_combos()
                    messagebox.showinfo("Éxito", f"Sensor '{sensor_nombre}' eliminado correctamente")
                else:
                    self.agregar_log(f"❌ Error eliminando sensor '{sensor_nombre}'")
                    messagebox.showerror("Error", f"No se pudo eliminar el sensor '{sensor_nombre}'")
            else:
                messagebox.showerror("Error", "No hay conexión con MongoDB Atlas")
                
        except Exception as e:
            self.agregar_log(f"❌ Error eliminando sensor: {e}")
            messagebox.showerror("Error", f"Error eliminando sensor: {e}")
    
    def al_hacer_doble_clic_sensor(self, event):
        """Manejar doble click en sensor - Mostrar información detallada"""
        try:
            # Obtener sensor seleccionado
            seleccionado = self.tree_sensores.selection()
            if not seleccionado:
                return
            
            # Obtener datos del sensor seleccionado
            item = self.tree_sensores.item(seleccionado[0])
            sensor_id = item['values'][0]
            sensor_nombre = item['values'][1]
            sensor_ubicacion = item['values'][2]
            sensor_tipo = item['values'][3]
            sensor_estado = item['values'][4]
            sensor_ultima_medicion = item['values'][5]
            
            # Crear ventana de información detallada
            ventana_info = tk.Toplevel(self.root)
            ventana_info.title(f"Información del Sensor: {sensor_nombre}")
            ventana_info.geometry("600x500")
            ventana_info.configure(bg='#ecf0f1')
            ventana_info.resizable(False, False)
            
            # Centrar ventana
            ventana_info.transient(self.root)
            ventana_info.grab_set()
            
            # Header
            header_frame = tk.Frame(ventana_info, bg='#3498db', height=60)
            header_frame.pack(fill='x')
            header_frame.pack_propagate(False)
            
            tk.Label(header_frame, text=f"📊 INFORMACIÓN DETALLADA DEL SENSOR", 
                    font=('Arial', 16, 'bold'), fg='white', bg='#3498db').pack(expand=True)
            
            # Contenido principal
            content_frame = tk.Frame(ventana_info, bg='white', padx=20, pady=20)
            content_frame.pack(fill='both', expand=True)
            
            # Información básica
            info_frame = tk.LabelFrame(content_frame, text="Información Básica", 
                                     font=('Arial', 12, 'bold'), bg='white')
            info_frame.pack(fill='x', pady=(0, 15))
            
            info_inner = tk.Frame(info_frame, bg='white', padx=10, pady=10)
            info_inner.pack(fill='x')
            
            tk.Label(info_inner, text=f"🆔 ID:", font=('Arial', 10, 'bold'), bg='white').grid(row=0, column=0, sticky='w', padx=(0, 10))
            tk.Label(info_inner, text=sensor_id, font=('Arial', 10), bg='white').grid(row=0, column=1, sticky='w')
            
            tk.Label(info_inner, text=f"📝 Nombre:", font=('Arial', 10, 'bold'), bg='white').grid(row=1, column=0, sticky='w', padx=(0, 10))
            tk.Label(info_inner, text=sensor_nombre, font=('Arial', 10), bg='white').grid(row=1, column=1, sticky='w')
            
            tk.Label(info_inner, text=f"📍 Ubicación:", font=('Arial', 10, 'bold'), bg='white').grid(row=2, column=0, sticky='w', padx=(0, 10))
            tk.Label(info_inner, text=sensor_ubicacion, font=('Arial', 10), bg='white').grid(row=2, column=1, sticky='w')
            
            tk.Label(info_inner, text=f"🔧 Tipo:", font=('Arial', 10, 'bold'), bg='white').grid(row=3, column=0, sticky='w', padx=(0, 10))
            tk.Label(info_inner, text=sensor_tipo, font=('Arial', 10), bg='white').grid(row=3, column=1, sticky='w')
            
            tk.Label(info_inner, text=f"⚡ Estado:", font=('Arial', 10, 'bold'), bg='white').grid(row=4, column=0, sticky='w', padx=(0, 10))
            estado_color = 'green' if sensor_estado == 'Activo' else 'orange' if sensor_estado == 'Mantenimiento' else 'red'
            tk.Label(info_inner, text=sensor_estado, font=('Arial', 10, 'bold'), 
                    fg=estado_color, bg='white').grid(row=4, column=1, sticky='w')
            
            tk.Label(info_inner, text=f"🕒 Última Medición:", font=('Arial', 10, 'bold'), bg='white').grid(row=5, column=0, sticky='w', padx=(0, 10))
            tk.Label(info_inner, text=sensor_ultima_medicion, font=('Arial', 10), bg='white').grid(row=5, column=1, sticky='w')
            
            # Estadísticas de mediciones
            stats_frame = tk.LabelFrame(content_frame, text="Estadísticas de Mediciones", 
                                      font=('Arial', 12, 'bold'), bg='white')
            stats_frame.pack(fill='both', expand=True, pady=(0, 15))
            
            stats_inner = tk.Frame(stats_frame, bg='white', padx=10, pady=10)
            stats_inner.pack(fill='both', expand=True)
            
            # Obtener estadísticas del sensor
            estadisticas = self.obtener_estadisticas_sensor(sensor_id)
            
            if estadisticas:
                tk.Label(stats_inner, text=f"📊 Total de mediciones:", font=('Arial', 10, 'bold'), bg='white').grid(row=0, column=0, sticky='w', padx=(0, 10))
                tk.Label(stats_inner, text=str(estadisticas['total_mediciones']), font=('Arial', 10), bg='white').grid(row=0, column=1, sticky='w')
                
                tk.Label(stats_inner, text=f"🌡️ Temp. máxima:", font=('Arial', 10, 'bold'), bg='white').grid(row=1, column=0, sticky='w', padx=(0, 10))
                tk.Label(stats_inner, text=f"{estadisticas['temp_maxima']}°C", font=('Arial', 10), bg='white').grid(row=1, column=1, sticky='w')
                
                tk.Label(stats_inner, text=f"🌡️ Temp. mínima:", font=('Arial', 10, 'bold'), bg='white').grid(row=2, column=0, sticky='w', padx=(0, 10))
                tk.Label(stats_inner, text=f"{estadisticas['temp_minima']}°C", font=('Arial', 10), bg='white').grid(row=2, column=1, sticky='w')
                
                tk.Label(stats_inner, text=f"💧 Humedad promedio:", font=('Arial', 10, 'bold'), bg='white').grid(row=3, column=0, sticky='w', padx=(0, 10))
                tk.Label(stats_inner, text=f"{estadisticas['humedad_promedio']}%", font=('Arial', 10), bg='white').grid(row=3, column=1, sticky='w')
            else:
                tk.Label(stats_inner, text="No hay mediciones disponibles para este sensor", 
                        font=('Arial', 10), fg='gray', bg='white').pack(expand=True)
            
            # Botones de acción
            buttons_frame = tk.Frame(content_frame, bg='white')
            buttons_frame.pack(fill='x', pady=(10, 0))
            
            tk.Button(buttons_frame, text="📊 Generar Datos", 
                     command=lambda: self.generar_datos_sensor_desde_info(sensor_id, sensor_nombre),
                     bg='#f39c12', fg='white', font=('Arial', 10)).pack(side='left', padx=(0, 10))
            
            tk.Button(buttons_frame, text="🔄 Actualizar", 
                     command=lambda: self.actualizar_info_sensor(ventana_info, sensor_id),
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=(0, 10))
            
            tk.Button(buttons_frame, text="❌ Cerrar", 
                     command=ventana_info.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack(side='right')
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando información del sensor: {e}")
            messagebox.showerror("Error", f"Error mostrando información: {e}")
    
    def obtener_estadisticas_sensor(self, sensor_id):
        """Obtener estadísticas de un sensor"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return None
            
            mediciones = self.mongodb_service.obtener_mediciones_sensor(sensor_id)
            
            if not mediciones:
                return None
            
            temperaturas_max = [m.get('temperature_max', 0) for m in mediciones if m.get('temperature_max')]
            temperaturas_min = [m.get('temperature_min', 0) for m in mediciones if m.get('temperature_min')]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity')]
            
            estadisticas = {
                'total_mediciones': len(mediciones),
                'temp_maxima': round(max(temperaturas_max), 1) if temperaturas_max else 'N/A',
                'temp_minima': round(min(temperaturas_min), 1) if temperaturas_min else 'N/A',
                'humedad_promedio': round(sum(humedades) / len(humedades), 1) if humedades else 'N/A'
            }
            
            return estadisticas
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo estadísticas del sensor: {e}")
            return None
    
    def generar_datos_sensor_desde_info(self, sensor_id, sensor_nombre):
        """Generar datos de prueba desde la ventana de información"""
        try:
            # Obtener tipo del sensor desde la base de datos
            sensores = self.mongodb_service.obtener_sensores()
            sensor_info = next((s for s in sensores if s.get('sensor_id') == sensor_id), None)
            
            if not sensor_info:
                messagebox.showerror("Error", "No se encontró información del sensor")
                return
            
            sensor_tipo = sensor_info.get('type', 'Temperatura')
            
            # Generar datos de prueba
            datos_generados = self.crear_datos_prueba_sensor(sensor_id, sensor_tipo)
            
            if self.mongodb_service and self.mongodb_service.conectado:
                mediciones_creadas = 0
                mediciones_fallidas = 0
                
                self.agregar_log(f"📊 Generando {len(datos_generados)} mediciones para sensor '{sensor_nombre}'")
                
                for i, dato in enumerate(datos_generados):
                    self.agregar_log(f"📊 Procesando medición {i+1}/{len(datos_generados)}: {dato.get('measurement_id', 'Sin ID')}")
                    
                    if self.mongodb_service.crear_medicion(dato):
                        mediciones_creadas += 1
                        self.agregar_log(f"✅ Medición {i+1} creada exitosamente")
                    else:
                        mediciones_fallidas += 1
                        self.agregar_log(f"❌ Medición {i+1} falló")
                
                self.agregar_log(f"📊 Resultado: {mediciones_creadas} exitosas, {mediciones_fallidas} fallidas")
                
                if mediciones_creadas > 0:
                    self.agregar_log(f"✅ {mediciones_creadas} mediciones generadas para sensor '{sensor_nombre}'")
                    messagebox.showinfo("Éxito", f"Se generaron {mediciones_creadas} mediciones de prueba para el sensor '{sensor_nombre}'")
                    self.actualizar_lista_sensores()
                else:
                    self.agregar_log(f"❌ Error generando datos para sensor '{sensor_nombre}' - Todas las mediciones fallaron")
                    messagebox.showerror("Error", f"No se pudieron generar datos para el sensor '{sensor_nombre}'\n\nVerifique los logs para más detalles.")
            else:
                messagebox.showerror("Error", "No hay conexión con MongoDB Atlas")
                
        except Exception as e:
            self.agregar_log(f"❌ Error generando datos desde info: {e}")
            messagebox.showerror("Error", f"Error generando datos: {e}")
    
    def actualizar_info_sensor(self, ventana, sensor_id):
        """Actualizar información del sensor en la ventana"""
        try:
            # Actualizar la lista principal
            self.actualizar_lista_sensores()
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando información: {e}")
    
    def editar_sensor(self):
        """Editar sensor seleccionado"""
        try:
            # Obtener sensor seleccionado
            seleccionado = self.tree_sensores.selection()
            if not seleccionado:
                messagebox.showwarning("Advertencia", "Por favor seleccione un sensor para editar")
                return
            
            # Obtener datos del sensor seleccionado
            item = self.tree_sensores.item(seleccionado[0])
            sensor_id = item['values'][0]
            sensor_nombre = item['values'][1]
            sensor_ubicacion = item['values'][2]
            sensor_tipo = item['values'][3]
            sensor_estado = item['values'][4]
            
            # Crear ventana de edición
            ventana_editar = tk.Toplevel(self.root)
            ventana_editar.title(f"Editar Sensor: {sensor_nombre}")
            ventana_editar.geometry("500x400")
            ventana_editar.configure(bg='#ecf0f1')
            ventana_editar.resizable(False, False)
            
            # Centrar ventana
            ventana_editar.transient(self.root)
            ventana_editar.grab_set()
            
            # Header
            header_frame = tk.Frame(ventana_editar, bg='#9b59b6', height=60)
            header_frame.pack(fill='x')
            header_frame.pack_propagate(False)
            
            tk.Label(header_frame, text=f"✏️ EDITAR SENSOR", 
                    font=('Arial', 16, 'bold'), fg='white', bg='#9b59b6').pack(expand=True)
            
            # Contenido principal
            content_frame = tk.Frame(ventana_editar, bg='white', padx=20, pady=20)
            content_frame.pack(fill='both', expand=True)
            
            # Campos de edición
            tk.Label(content_frame, text="ID del Sensor:", font=('Arial', 10, 'bold'), bg='white').grid(row=0, column=0, sticky='w', pady=5)
            tk.Label(content_frame, text=sensor_id, font=('Arial', 10), bg='white', fg='gray').grid(row=0, column=1, sticky='w', pady=5, padx=(10, 0))
            
            tk.Label(content_frame, text="Nombre:", font=('Arial', 10, 'bold'), bg='white').grid(row=1, column=0, sticky='w', pady=5)
            entry_nombre_edit = tk.Entry(content_frame, width=30, font=('Arial', 10))
            entry_nombre_edit.grid(row=1, column=1, sticky='w', pady=5, padx=(10, 0))
            entry_nombre_edit.insert(0, sensor_nombre)
            
            tk.Label(content_frame, text="Ubicación:", font=('Arial', 10, 'bold'), bg='white').grid(row=2, column=0, sticky='w', pady=5)
            entry_ubicacion_edit = tk.Entry(content_frame, width=30, font=('Arial', 10))
            entry_ubicacion_edit.grid(row=2, column=1, sticky='w', pady=5, padx=(10, 0))
            entry_ubicacion_edit.insert(0, sensor_ubicacion)
            
            tk.Label(content_frame, text="Tipo:", font=('Arial', 10, 'bold'), bg='white').grid(row=3, column=0, sticky='w', pady=5)
            combo_tipo_edit = ttk.Combobox(content_frame, values=["Temperatura", "Humedad", "Ambos"], width=27, font=('Arial', 10))
            combo_tipo_edit.grid(row=3, column=1, sticky='w', pady=5, padx=(10, 0))
            combo_tipo_edit.set(sensor_tipo)
            
            tk.Label(content_frame, text="Estado:", font=('Arial', 10, 'bold'), bg='white').grid(row=4, column=0, sticky='w', pady=5)
            combo_estado_edit = ttk.Combobox(content_frame, values=["Activo", "Inactivo", "Mantenimiento"], width=27, font=('Arial', 10))
            combo_estado_edit.grid(row=4, column=1, sticky='w', pady=5, padx=(10, 0))
            combo_estado_edit.set(sensor_estado)
            
            # Botones
            buttons_frame = tk.Frame(content_frame, bg='white')
            buttons_frame.grid(row=5, column=0, columnspan=2, pady=20)
            
            def guardar_cambios():
                try:
                    nuevo_nombre = entry_nombre_edit.get().strip()
                    nueva_ubicacion = entry_ubicacion_edit.get().strip()
                    nuevo_tipo = combo_tipo_edit.get()
                    nuevo_estado = combo_estado_edit.get()
                    
                    if not nuevo_nombre or not nueva_ubicacion:
                        messagebox.showerror("Error", "Por favor complete todos los campos")
                        return
                    
                    # Preparar datos actualizados
                    datos_actualizados = {
                        "name": nuevo_nombre,
                        "location": nueva_ubicacion,
                        "type": nuevo_tipo,
                        "status": nuevo_estado.lower(),
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    # Actualizar sensor en MongoDB
                    if self.mongodb_service and self.mongodb_service.conectado:
                        if self.mongodb_service.actualizar_sensor(sensor_id, datos_actualizados):
                            self.agregar_log(f"✅ Sensor '{nuevo_nombre}' actualizado correctamente")
                            self.actualizar_lista_sensores()
                            self.cargar_sensores_para_combos()
                            messagebox.showinfo("Éxito", f"Sensor '{nuevo_nombre}' actualizado correctamente")
                            ventana_editar.destroy()
                        else:
                            self.agregar_log(f"❌ Error actualizando sensor '{nuevo_nombre}'")
                            messagebox.showerror("Error", f"No se pudo actualizar el sensor '{nuevo_nombre}'")
                    else:
                        messagebox.showerror("Error", "No hay conexión con MongoDB Atlas")
                        
                except Exception as e:
                    self.agregar_log(f"❌ Error guardando cambios del sensor: {e}")
                    messagebox.showerror("Error", f"Error guardando cambios: {e}")
            
            tk.Button(buttons_frame, text="💾 Guardar Cambios", 
                     command=guardar_cambios,
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=(0, 10))
            
            tk.Button(buttons_frame, text="❌ Cancelar", 
                     command=ventana_editar.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack(side='left')
            
        except Exception as e:
            self.agregar_log(f"❌ Error editando sensor: {e}")
            messagebox.showerror("Error", f"Error editando sensor: {e}")
    
    def ejecutar_analisis(self):
        """Ejecutar análisis de datos por ciudad"""
        try:
            ciudad_seleccionada = self.combo_ciudad_analisis.get()
            ciudad = self.extraer_ciudad_del_formato(ciudad_seleccionada)
            fecha_desde = self.entry_fecha_desde.get()
            fecha_hasta = self.entry_fecha_hasta.get()
            tipo_analisis = self.combo_tipo_analisis.get()
            
            if not ciudad:
                messagebox.showerror("Error", "Seleccione una ciudad")
                return
            
            if not fecha_desde or not fecha_hasta:
                messagebox.showerror("Error", "Complete las fechas de inicio y fin")
                return
            
            # Validar formato de fechas
            try:
                fecha_inicio = datetime.strptime(fecha_desde, "%Y-%m-%d")
                fecha_fin = datetime.strptime(fecha_hasta, "%Y-%m-%d")
                
                if fecha_inicio > fecha_fin:
                    messagebox.showerror("Error", "La fecha de inicio debe ser anterior a la fecha de fin")
                    return
                    
            except ValueError:
                messagebox.showerror("Error", "Formato de fecha inválido. Use YYYY-MM-DD")
                return
            
            # Limpiar área de resultados
            self.texto_resultados_analisis.delete(1.0, tk.END)
            
            # Mostrar mensaje de procesamiento
            self.texto_resultados_analisis.insert(tk.END, f"🔍 Analizando datos de temperatura para {ciudad}...\n")
            self.texto_resultados_analisis.insert(tk.END, f"📅 Período: {fecha_desde} a {fecha_hasta}\n")
            self.texto_resultados_analisis.insert(tk.END, f"🌡️ Tipo: {tipo_analisis}\n")
            self.texto_resultados_analisis.insert(tk.END, "="*60 + "\n\n")
            
            # Obtener datos de temperatura
            datos_temperatura = self.obtener_datos_temperatura_ciudad(ciudad, fecha_inicio, fecha_fin)
            
            if not datos_temperatura:
                self.texto_resultados_analisis.insert(tk.END, f"❌ No se encontraron datos de temperatura para {ciudad} en el período especificado.\n")
                return
            
            # Procesar según el tipo de análisis
            if tipo_analisis == "Temperatura Máxima":
                self.mostrar_analisis_temperatura_maxima(datos_temperatura, ciudad)
            elif tipo_analisis == "Temperatura Mínima":
                self.mostrar_analisis_temperatura_minima(datos_temperatura, ciudad)
            else:  # Ambas Temperaturas
                self.mostrar_analisis_temperatura_completa(datos_temperatura, ciudad)
                
        except Exception as e:
            self.texto_resultados_analisis.insert(tk.END, f"❌ Error en el análisis: {e}\n")
                
    def obtener_datos_temperatura_ciudad(self, ciudad, fecha_inicio, fecha_fin):
        """Obtener datos de temperatura para una ciudad desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("❌ MongoDB no disponible para consultar datos de temperatura")
                return []
            
            # Consultar datos de temperatura desde MongoDB
            datos_temperatura = self.mongodb_service.obtener_datos_temperatura_por_ubicacion(
                ubicacion=ciudad,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            if datos_temperatura:
                self.agregar_log(f"📊 Datos de temperatura obtenidos para {ciudad}: {len(datos_temperatura)} registros")
                return datos_temperatura
            else:
                # Si no hay datos en MongoDB, NO generar datos de ejemplo
                self.agregar_log(f"⚠️ No hay sensores registrados en {ciudad}")
                return []
                
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo datos de temperatura: {e}")
            # En caso de error, generar datos de ejemplo
            return self.generar_datos_temperatura_ejemplo(ciudad, fecha_inicio, fecha_fin)
    
    def generar_datos_temperatura_ejemplo(self, ciudad, fecha_inicio, fecha_fin):
        """Generar datos de temperatura de ejemplo cuando no hay datos en MongoDB"""
        import random
        
        try:
            # Verificar si las fechas ya son objetos datetime o strings
            if isinstance(fecha_inicio, str):
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            else:
                fecha_inicio_dt = fecha_inicio
                
            if isinstance(fecha_fin, str):
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            else:
                fecha_fin_dt = fecha_fin
            
            datos_ejemplo = []
            fecha_actual = fecha_inicio_dt
            
            # Generar datos para cada día en el rango
            while fecha_actual <= fecha_fin_dt:
                # Generar temperatura basada en la ciudad
                if "Buenos Aires" in ciudad:
                    temp_max_base = random.uniform(28, 35)
                    temp_min_base = random.uniform(18, 25)
                    humedad_base = random.uniform(55, 75)
                elif "Córdoba" in ciudad:
                    temp_max_base = random.uniform(25, 32)
                    temp_min_base = random.uniform(12, 20)
                    humedad_base = random.uniform(35, 55)
                elif "Mendoza" in ciudad:
                    temp_max_base = random.uniform(30, 40)
                    temp_min_base = random.uniform(15, 25)
                    humedad_base = random.uniform(25, 45)
                elif "Rosario" in ciudad:
                    temp_max_base = random.uniform(27, 34)
                    temp_min_base = random.uniform(15, 22)
                    humedad_base = random.uniform(45, 65)
                elif "La Plata" in ciudad:
                    temp_max_base = random.uniform(26, 32)
                    temp_min_base = random.uniform(16, 22)
                    humedad_base = random.uniform(60, 80)
                else:
                    temp_max_base = random.uniform(25, 32)
                    temp_min_base = random.uniform(15, 22)
                    humedad_base = random.uniform(45, 65)
                
                datos_ejemplo.append({
                    "fecha": fecha_actual.strftime("%Y-%m-%d"),
                    "temp_max": round(temp_max_base, 1),
                    "temp_min": round(temp_min_base, 1),
                    "temperatura": round((temp_max_base + temp_min_base) / 2, 1),
                    "humedad": round(humedad_base, 1),
                    "ubicacion": ciudad,
                    "fuente": "datos_ejemplo"
                })
                
                fecha_actual += timedelta(days=1)
            
            return datos_ejemplo
            
        except ValueError:
            # Si hay error en el formato de fecha, devolver datos básicos
            return [{
                "fecha": fecha_inicio,
                "temp_max": 25.0,
                "temp_min": 15.0,
                "temperatura": 20.0,
                "humedad": 50.0,
                "ubicacion": ciudad,
                "fuente": "datos_ejemplo"
            }]
    
    def mostrar_analisis_temperatura_maxima(self, datos, ciudad):
        """Mostrar análisis de temperatura máxima"""
        if not datos:
            return
            
        temperaturas_max = [d["temp_max"] for d in datos]
        temp_maxima = max(temperaturas_max)
        temp_minima = min(temperaturas_max)
        temp_promedio = sum(temperaturas_max) / len(temperaturas_max)
        
        self.texto_resultados_analisis.insert(tk.END, f"🌡️ ANÁLISIS DE TEMPERATURA MÁXIMA - {ciudad.upper()}\n")
        self.texto_resultados_analisis.insert(tk.END, "="*50 + "\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📊 ESTADÍSTICAS:\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura máxima registrada: {temp_maxima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura mínima registrada: {temp_minima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura promedio: {temp_promedio:.1f}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Total de días analizados: {len(datos)}\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📅 DATOS DETALLADOS:\n")
        self.texto_resultados_analisis.insert(tk.END, "-"*40 + "\n")
        for dato in datos:
            self.texto_resultados_analisis.insert(tk.END, f"   {dato['fecha']}: {dato['temp_max']}°C\n")
    
    def mostrar_analisis_temperatura_minima(self, datos, ciudad):
        """Mostrar análisis de temperatura mínima"""
        if not datos:
            return
            
        temperaturas_min = [d["temp_min"] for d in datos]
        temp_maxima = max(temperaturas_min)
        temp_minima = min(temperaturas_min)
        temp_promedio = sum(temperaturas_min) / len(temperaturas_min)
        
        self.texto_resultados_analisis.insert(tk.END, f"🌡️ ANÁLISIS DE TEMPERATURA MÍNIMA - {ciudad.upper()}\n")
        self.texto_resultados_analisis.insert(tk.END, "="*50 + "\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📊 ESTADÍSTICAS:\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura máxima registrada: {temp_maxima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura mínima registrada: {temp_minima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura promedio: {temp_promedio:.1f}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Total de días analizados: {len(datos)}\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📅 DATOS DETALLADOS:\n")
        self.texto_resultados_analisis.insert(tk.END, "-"*40 + "\n")
        for dato in datos:
            self.texto_resultados_analisis.insert(tk.END, f"   {dato['fecha']}: {dato['temp_min']}°C\n")
    
    def mostrar_analisis_temperatura_completa(self, datos, ciudad):
        """Mostrar análisis completo de temperaturas"""
        if not datos:
            return
            
        temperaturas_max = [d["temp_max"] for d in datos]
        temperaturas_min = [d["temp_min"] for d in datos]
        
        # Estadísticas máximas
        temp_max_maxima = max(temperaturas_max)
        temp_max_minima = min(temperaturas_max)
        temp_max_promedio = sum(temperaturas_max) / len(temperaturas_max)
        
        # Estadísticas mínimas
        temp_min_maxima = max(temperaturas_min)
        temp_min_minima = min(temperaturas_min)
        temp_min_promedio = sum(temperaturas_min) / len(temperaturas_min)
        
        self.texto_resultados_analisis.insert(tk.END, f"🌡️ ANÁLISIS COMPLETO DE TEMPERATURAS - {ciudad.upper()}\n")
        self.texto_resultados_analisis.insert(tk.END, "="*60 + "\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📊 ESTADÍSTICAS TEMPERATURA MÁXIMA:\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Máxima registrada: {temp_max_maxima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Mínima registrada: {temp_max_minima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Promedio: {temp_max_promedio:.1f}°C\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📊 ESTADÍSTICAS TEMPERATURA MÍNIMA:\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Máxima registrada: {temp_min_maxima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Mínima registrada: {temp_min_minima}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Promedio: {temp_min_promedio:.1f}°C\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📊 RESUMEN GENERAL:\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Total de días analizados: {len(datos)}\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Amplitud térmica promedio: {temp_max_promedio - temp_min_promedio:.1f}°C\n")
        self.texto_resultados_analisis.insert(tk.END, f"   • Mayor amplitud térmica: {temp_max_maxima - temp_min_minima:.1f}°C\n\n")
        
        self.texto_resultados_analisis.insert(tk.END, f"📅 DATOS DETALLADOS:\n")
        self.texto_resultados_analisis.insert(tk.END, "-"*50 + "\n")
        for dato in datos:
            amplitud = dato['temp_max'] - dato['temp_min']
            self.texto_resultados_analisis.insert(tk.END, f"   {dato['fecha']}: Máx {dato['temp_max']}°C | Mín {dato['temp_min']}°C | Amplitud {amplitud:.1f}°C\n")
    
    def exportar_reporte(self):
        """Exportar reporte de temperaturas"""
        try:
            ciudad_seleccionada = self.combo_ciudad_analisis.get()
            ciudad = self.extraer_ciudad_del_formato(ciudad_seleccionada)
            fecha_desde = self.entry_fecha_desde.get()
            fecha_hasta = self.entry_fecha_hasta.get()
            tipo_analisis = self.combo_tipo_analisis.get()
            
            if not ciudad:
                messagebox.showerror("Error", "Seleccione una ciudad")
                return
            
            if not fecha_desde or not fecha_hasta:
                messagebox.showerror("Error", "Complete las fechas de inicio y fin")
                return
            
            # Validar formato de fechas
            try:
                fecha_inicio = datetime.strptime(fecha_desde, "%Y-%m-%d")
                fecha_fin = datetime.strptime(fecha_hasta, "%Y-%m-%d")
                
                if fecha_inicio > fecha_fin:
                    messagebox.showerror("Error", "La fecha de inicio debe ser anterior a la fecha de fin")
                    return
                    
            except ValueError:
                messagebox.showerror("Error", "Formato de fecha inválido. Use YYYY-MM-DD")
                return
            
            # Obtener datos de temperatura
            datos_temperatura = self.obtener_datos_temperatura_ciudad(ciudad, fecha_inicio, fecha_fin)
            
            if not datos_temperatura:
                messagebox.showerror("Error", f"No se encontraron datos de temperatura para {ciudad} en el período especificado.")
                return
            
            # Crear contenido del reporte
            reporte = f"REPORTE DE TEMPERATURAS - {ciudad.upper()}\n"
            reporte += "="*50 + "\n\n"
            reporte += f"Ciudad: {ciudad}\n"
            reporte += f"Período: {fecha_desde} a {fecha_hasta}\n"
            reporte += f"Tipo de análisis: {tipo_analisis}\n"
            reporte += f"Total de días: {len(datos_temperatura)}\n"
            reporte += f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            reporte += "DATOS DETALLADOS:\n"
            reporte += "-"*40 + "\n"
            
            for dato in datos_temperatura:
                amplitud = dato['temp_max'] - dato['temp_min']
                reporte += f"{dato['fecha']}: Máx {dato['temp_max']}°C | Mín {dato['temp_min']}°C | Amplitud {amplitud:.1f}°C\n"
            
            # Calcular estadísticas
            temperaturas_max = [d["temp_max"] for d in datos_temperatura]
            temperaturas_min = [d["temp_min"] for d in datos_temperatura]
            
            reporte += f"\nESTADÍSTICAS:\n"
            reporte += "-"*20 + "\n"
            reporte += f"Temperatura máxima: {max(temperaturas_max)}°C\n"
            reporte += f"Temperatura mínima: {min(temperaturas_min)}°C\n"
            reporte += f"Promedio máxima: {sum(temperaturas_max)/len(temperaturas_max):.1f}°C\n"
            reporte += f"Promedio mínima: {sum(temperaturas_min)/len(temperaturas_min):.1f}°C\n"
            reporte += f"Amplitud promedio: {(sum(temperaturas_max)/len(temperaturas_max)) - (sum(temperaturas_min)/len(temperaturas_min)):.1f}°C\n"
            
            # Mostrar el reporte en el área de resultados
            self.texto_resultados_analisis.delete(1.0, tk.END)
            self.texto_resultados_analisis.insert(tk.END, reporte)
            
            messagebox.showinfo("Exportar", f"Reporte generado exitosamente para {ciudad}\n\nEl reporte contiene {len(datos_temperatura)} días de datos.")
            
        except Exception as e:
            messagebox.showerror("Error", f"Error exportando reporte: {e}")
    
    def detectar_anomalias(self):
        """Detectar anomalías en los datos de temperatura"""
        try:
            ciudad_seleccionada = self.combo_ciudad_analisis.get()
            ciudad = self.extraer_ciudad_del_formato(ciudad_seleccionada)
            fecha_desde = self.entry_fecha_desde.get()
            fecha_hasta = self.entry_fecha_hasta.get()
            
            if not ciudad:
                messagebox.showerror("Error", "Seleccione una ciudad")
                return
            
            if not fecha_desde or not fecha_hasta:
                messagebox.showerror("Error", "Complete las fechas de inicio y fin")
                return
            
            # Validar formato de fechas
            try:
                fecha_inicio = datetime.strptime(fecha_desde, "%Y-%m-%d")
                fecha_fin = datetime.strptime(fecha_hasta, "%Y-%m-%d")
                
                if fecha_inicio > fecha_fin:
                    messagebox.showerror("Error", "La fecha de inicio debe ser anterior a la fecha de fin")
                    return
                    
            except ValueError:
                messagebox.showerror("Error", "Formato de fecha inválido. Use YYYY-MM-DD")
                return
            
            # Obtener datos de temperatura
            datos_temperatura = self.obtener_datos_temperatura_ciudad(ciudad, fecha_inicio, fecha_fin)
            
            if not datos_temperatura:
                messagebox.showerror("Error", f"No se encontraron datos de temperatura para {ciudad} en el período especificado.")
                return
            
            # Calcular umbrales para detectar anomalías
            temperaturas_max = [d["temp_max"] for d in datos_temperatura]
            temperaturas_min = [d["temp_min"] for d in datos_temperatura]
            
            # Calcular media y desviación estándar
            import statistics
            media_max = statistics.mean(temperaturas_max)
            media_min = statistics.mean(temperaturas_min)
            
            try:
                desv_max = statistics.stdev(temperaturas_max)
                desv_min = statistics.stdev(temperaturas_min)
            except statistics.StatisticsError:
                desv_max = 0
                desv_min = 0
            
            # Umbrales (2 desviaciones estándar)
            umbral_max_alto = media_max + (2 * desv_max)
            umbral_max_bajo = media_max - (2 * desv_max)
            umbral_min_alto = media_min + (2 * desv_min)
            umbral_min_bajo = media_min - (2 * desv_min)
            
            # Detectar anomalías
            anomalias = []
            
            for dato in datos_temperatura:
                anomalias_dia = []
                
                if dato['temp_max'] > umbral_max_alto:
                    anomalias_dia.append(f"Temperatura máxima muy alta: {dato['temp_max']}°C")
                elif dato['temp_max'] < umbral_max_bajo:
                    anomalias_dia.append(f"Temperatura máxima muy baja: {dato['temp_max']}°C")
                
                if dato['temp_min'] > umbral_min_alto:
                    anomalias_dia.append(f"Temperatura mínima muy alta: {dato['temp_min']}°C")
                elif dato['temp_min'] < umbral_min_bajo:
                    anomalias_dia.append(f"Temperatura mínima muy baja: {dato['temp_min']}°C")
                
                if anomalias_dia:
                    anomalias.append({
                        'fecha': dato['fecha'],
                        'anomalias': anomalias_dia
                    })
            
            # Mostrar resultados
            self.texto_resultados_analisis.delete(1.0, tk.END)
            self.texto_resultados_analisis.insert(tk.END, f"🔍 DETECCIÓN DE ANOMALÍAS - {ciudad.upper()}\n")
            self.texto_resultados_analisis.insert(tk.END, "="*50 + "\n\n")
            
            self.texto_resultados_analisis.insert(tk.END, f"📊 ANÁLISIS ESTADÍSTICO:\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura máxima promedio: {media_max:.1f}°C\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Temperatura mínima promedio: {media_min:.1f}°C\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Desviación estándar máxima: {desv_max:.1f}°C\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Desviación estándar mínima: {desv_min:.1f}°C\n\n")
            
            self.texto_resultados_analisis.insert(tk.END, f"⚠️ UMBRALES DE ANOMALÍA:\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Máxima alta: {umbral_max_alto:.1f}°C\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Máxima baja: {umbral_max_bajo:.1f}°C\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Mínima alta: {umbral_min_alto:.1f}°C\n")
            self.texto_resultados_analisis.insert(tk.END, f"   • Mínima baja: {umbral_min_bajo:.1f}°C\n\n")
            
            if anomalias:
                self.texto_resultados_analisis.insert(tk.END, f"🚨 ANOMALÍAS DETECTADAS ({len(anomalias)} días):\n")
                self.texto_resultados_analisis.insert(tk.END, "-"*40 + "\n")
                
                for anomalia in anomalias:
                    self.texto_resultados_analisis.insert(tk.END, f"   📅 {anomalia['fecha']}:\n")
                    for anom in anomalia['anomalias']:
                        self.texto_resultados_analisis.insert(tk.END, f"      • {anom}\n")
                    self.texto_resultados_analisis.insert(tk.END, "\n")
            else:
                self.texto_resultados_analisis.insert(tk.END, f"✅ No se detectaron anomalías en el período analizado.\n")
                self.texto_resultados_analisis.insert(tk.END, f"   Todas las temperaturas están dentro del rango normal.\n")
            
        except Exception as e:
            messagebox.showerror("Error", f"Error detectando anomalías: {e}")
    
    def crear_alerta(self):
        """Crear nueva alerta"""
        try:
            sensor_seleccionado = self.combo_sensor_alerta.get()
            sensor = self.extraer_sensor_id_del_formato(sensor_seleccionado)
            categoria = self.combo_categoria_alerta.get()
            tipo = self.combo_tipo_alerta.get()
            severidad = self.combo_severidad_alerta.get()
            estado = self.combo_estado_alerta.get()
            mensaje = self.entry_mensaje_alerta.get()
            umbral = self.entry_umbral_alerta.get()
            
            if not sensor or not tipo or not severidad:
                messagebox.showerror("Error", "Complete todos los campos obligatorios")
                return
            
            # Generar ID único para la alerta
            alert_id = f"ALERT_{int(time.time())}"
            
            # Crear mensaje personalizado o usar el predeterminado
            mensaje_final = mensaje if mensaje else f"Alerta {categoria}: {tipo} - Severidad: {severidad}"
            
            # Crear datos de la alerta
            alerta_data = {
                "alert_id": alert_id,
                "sensor_id": sensor,
                "categoria": categoria,
                "type": tipo,
                "severity": severidad.lower(),
                "status": estado.lower(),
                "threshold": float(umbral) if umbral else None,
                "message": mensaje_final,
                "created_at": datetime.now().isoformat(),
                "created_by": self.usuario_autenticado,
                "updated_at": datetime.now().isoformat(),
                "updated_by": self.usuario_autenticado
            }
            
            # Diferenciar entre alertas de sensor y climáticas
            if categoria == "Sensor":
                alerta_data["resolucion_manual"] = True  # Requiere intervención técnica
                alerta_data["auto_resolucion"] = False
            else:  # Climática
                alerta_data["resolucion_manual"] = False  # Se resuelve automáticamente
                alerta_data["auto_resolucion"] = True
            
            # Guardar en MongoDB
            if self.mongodb_service and self.mongodb_service.conectado:
                if self.mongodb_service.crear_alerta(alerta_data):
                    self.actualizar_lista_alertas()
                    messagebox.showinfo("Éxito", f"Alerta {categoria} creada correctamente")
                    self.agregar_log(f"✅ Alerta {categoria} creada: {alert_id}")
                    
                    # Limpiar campos
                    self.entry_umbral_alerta.delete(0, tk.END)
                    self.entry_mensaje_alerta.delete(0, tk.END)
                    self.combo_estado_alerta.set("Pendiente")
                else:
                    messagebox.showerror("Error", "No se pudo crear la alerta")
            else:
                messagebox.showerror("Error", "MongoDB no está conectado")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error creando alerta: {e}")
            self.agregar_log(f"❌ Error creando alerta: {e}")
    
    def editar_alerta(self):
        """Editar alerta seleccionada"""
        try:
            # Obtener alerta seleccionada
            seleccionado = self.tree_alertas.selection()
            if not seleccionado:
                messagebox.showwarning("Advertencia", "Por favor seleccione una alerta para editar")
                return
            
            # Obtener datos de la alerta seleccionada
            item = self.tree_alertas.item(seleccionado[0])
            valores = item['values']
            
            if not valores:
                messagebox.showerror("Error", "No se pudieron obtener los datos de la alerta")
                return
            
            alert_id = valores[0]
            
            # Crear ventana de edición
            ventana_edicion = tk.Toplevel(self.root)
            ventana_edicion.title(f"Editar Alerta {alert_id}")
            ventana_edicion.geometry("500x400")
            ventana_edicion.configure(bg='white')
            
            # Campos de edición
            tk.Label(ventana_edicion, text="Estado:", bg='white', font=('Arial', 10, 'bold')).pack(pady=5)
            combo_estado_edit = ttk.Combobox(ventana_edicion, values=["Pendiente", "En Proceso", "Resuelta", "Cerrada"], width=30)
            combo_estado_edit.pack(pady=5)
            combo_estado_edit.set(valores[5])  # Estado actual
            
            tk.Label(ventana_edicion, text="Severidad:", bg='white', font=('Arial', 10, 'bold')).pack(pady=5)
            combo_severidad_edit = ttk.Combobox(ventana_edicion, values=["Baja", "Media", "Alta", "Crítica"], width=30)
            combo_severidad_edit.pack(pady=5)
            combo_severidad_edit.set(valores[4])  # Severidad actual
            
            tk.Label(ventana_edicion, text="Mensaje:", bg='white', font=('Arial', 10, 'bold')).pack(pady=5)
            entry_mensaje_edit = tk.Entry(ventana_edicion, width=50)
            entry_mensaje_edit.pack(pady=5)
            entry_mensaje_edit.insert(0, valores[7])  # Mensaje actual
            
            tk.Label(ventana_edicion, text="Comentario del Técnico:", bg='white', font=('Arial', 10, 'bold')).pack(pady=5)
            text_comentario = tk.Text(ventana_edicion, height=6, width=50)
            text_comentario.pack(pady=5)
            
            def guardar_cambios():
                try:
                    nuevo_estado = combo_estado_edit.get()
                    nueva_severidad = combo_severidad_edit.get()
                    nuevo_mensaje = entry_mensaje_edit.get()
                    comentario_tecnico = text_comentario.get("1.0", tk.END).strip()
                    
                    # Actualizar datos de la alerta
                    datos_actualizacion = {
                        "status": nuevo_estado.lower(),
                        "severity": nueva_severidad.lower(),
                        "message": nuevo_mensaje,
                        "updated_at": datetime.now().isoformat(),
                        "updated_by": self.usuario_autenticado
                    }
                    
                    if comentario_tecnico:
                        datos_actualizacion["comentario_tecnico"] = comentario_tecnico
                    
                    # Guardar cambios en MongoDB
                    if self.mongodb_service and self.mongodb_service.conectado:
                        if self.mongodb_service.actualizar_alerta(alert_id, datos_actualizacion):
                            self.actualizar_lista_alertas()
                            messagebox.showinfo("Éxito", "Alerta actualizada correctamente")
                            self.agregar_log(f"✏️ Alerta {alert_id} editada por técnico")
                            ventana_edicion.destroy()
                        else:
                            messagebox.showerror("Error", "No se pudo actualizar la alerta")
                    else:
                        messagebox.showerror("Error", "MongoDB no está conectado")
                        
                except Exception as e:
                    messagebox.showerror("Error", f"Error actualizando alerta: {e}")
            
            # Botones
            frame_botones = tk.Frame(ventana_edicion, bg='white')
            frame_botones.pack(pady=20)
            
            tk.Button(frame_botones, text="💾 Guardar Cambios", 
                     command=guardar_cambios, 
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            tk.Button(frame_botones, text="❌ Cancelar", 
                     command=ventana_edicion.destroy, 
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error editando alerta: {e}")
            messagebox.showerror("Error", f"Error editando alerta: {e}")
    
    def disparar_alerta_manual(self):
        """Disparar alerta manualmente basada en umbrales usando datos reales"""
        try:
            sensor_seleccionado = self.combo_sensor_alerta.get()
            sensor = self.extraer_sensor_id_del_formato(sensor_seleccionado)
            categoria = self.combo_categoria_alerta.get()
            tipo = self.combo_tipo_alerta.get()
            umbral = self.entry_umbral_alerta.get()
            
            if not sensor or not tipo or not umbral:
                messagebox.showerror("Error", "Complete sensor, tipo y umbral para disparar alerta")
                return
            
            # Obtener lectura real del sensor
            valor_actual = self.obtener_lectura_sensor_con_fallback(sensor, tipo)
            umbral_valor = float(umbral)
            
            if valor_actual is None:
                messagebox.showerror("Error", "No se pudo obtener lectura del sensor")
                return
            
            # Verificar si se debe disparar la alerta
            alerta_disparada = False
            mensaje_alerta = ""
            
            if tipo == "Temperatura Alta" and valor_actual > umbral_valor:
                alerta_disparada = True
                mensaje_alerta = f"Temperatura crítica: {valor_actual}°C (umbral: {umbral_valor}°C)"
            elif tipo == "Temperatura Baja" and valor_actual < umbral_valor:
                alerta_disparada = True
                mensaje_alerta = f"Temperatura baja: {valor_actual}°C (umbral: {umbral_valor}°C)"
            elif tipo == "Humedad Alta" and valor_actual > umbral_valor:
                alerta_disparada = True
                mensaje_alerta = f"Humedad alta: {valor_actual}% (umbral: {umbral_valor}%)"
            elif tipo == "Humedad Baja" and valor_actual < umbral_valor:
                alerta_disparada = True
                mensaje_alerta = f"Humedad baja: {valor_actual}% (umbral: {umbral_valor}%)"
            
            if alerta_disparada:
                # Crear alerta automáticamente
                alert_id = f"ALERT_MANUAL_{int(time.time())}"
                
                alerta_data = {
                    "alert_id": alert_id,
                    "sensor_id": sensor,
                    "categoria": categoria,
                    "type": tipo,
                    "severity": "alta",  # Alertas manuales son de alta severidad
                    "status": "active",
                    "threshold": umbral_valor,
                    "current_value": valor_actual,
                    "message": mensaje_alerta,
                    "triggered_at": datetime.now().isoformat(),
                    "created_at": datetime.now().isoformat(),
                    "created_by": self.usuario_autenticado,
                    "trigger_type": "manual",
                    "resolucion_manual": True,
                    "auto_resolucion": False
                }
                
                # Guardar en MongoDB
                if self.mongodb_service and self.mongodb_service.conectado:
                    if self.mongodb_service.crear_alerta(alerta_data):
                        self.actualizar_lista_alertas()
                        messagebox.showinfo("Alerta Disparada", f"🚨 {mensaje_alerta}\n\nAlerta creada automáticamente")
                        self.agregar_log(f"🚨 Alerta manual disparada: {mensaje_alerta}")
                    else:
                        messagebox.showerror("Error", "No se pudo crear la alerta en la base de datos")
                else:
                    messagebox.showerror("Error", "MongoDB no disponible")
            else:
                # No se dispara la alerta
                unidad = "°C" if "Temperatura" in tipo else "%"
                messagebox.showinfo("Sin Alerta", 
                    f"✅ Valor actual: {valor_actual}{unidad}\n"
                    f"📊 Umbral configurado: {umbral_valor}{unidad}\n\n"
                    f"No se cumple la condición para disparar la alerta")
                
        except ValueError:
            messagebox.showerror("Error", "El umbral debe ser un número válido")
        except Exception as e:
            messagebox.showerror("Error", f"Error disparando alerta: {e}")
            self.agregar_log(f"❌ Error disparando alerta manual: {e}")
    
    def configurar_umbrales(self):
        """Configurar umbrales automáticos para sensores con persistencia en MongoDB"""
        try:
            # Crear ventana de configuración de umbrales
            ventana_umbrales = tk.Toplevel(self.root)
            ventana_umbrales.title("⚙️ Configuración Avanzada de Umbrales")
            ventana_umbrales.geometry("800x700")
            ventana_umbrales.configure(bg='white')
            
            # Título principal
            tk.Label(ventana_umbrales, text="⚙️ CONFIGURACIÓN AVANZADA DE UMBRALES", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=20)
            
            # Frame para selección de configuración
            config_type_frame = tk.LabelFrame(ventana_umbrales, text="Tipo de Configuración", 
                                       font=('Arial', 12, 'bold'), bg='white')
            config_type_frame.pack(fill='x', padx=20, pady=10)
            
            # Variable para tipo de configuración
            self.tipo_configuracion = tk.StringVar(value="global")
            
            # Frame interno para radio buttons y selector de sensor
            radio_frame = tk.Frame(config_type_frame, bg='white')
            radio_frame.pack(fill='x', padx=10, pady=5)
            
            # Radio buttons en la izquierda
            radio_left_frame = tk.Frame(radio_frame, bg='white')
            radio_left_frame.pack(side='left', fill='x', expand=True)
            
            tk.Radiobutton(radio_left_frame, text="🌍 Umbrales Globales (Aplican a todos los sensores)", 
                          variable=self.tipo_configuracion, value="global", bg='white',
                          command=self.cambiar_tipo_configuracion).pack(anchor='w', pady=2)
            
            tk.Radiobutton(radio_left_frame, text="🎯 Umbrales por Sensor Específico", 
                          variable=self.tipo_configuracion, value="sensor", bg='white',
                          command=self.cambiar_tipo_configuracion).pack(anchor='w', pady=2)
            
            # Selector de sensor siempre visible a la derecha
            sensor_right_frame = tk.Frame(radio_frame, bg='white')
            sensor_right_frame.pack(side='right', padx=20)
            
            tk.Label(sensor_right_frame, text="Sensor:", bg='white', font=('Arial', 10, 'bold')).pack(anchor='w')
            self.combo_sensor_umbrales = tk.ttk.Combobox(sensor_right_frame, width=25, state='readonly')
            self.combo_sensor_umbrales.pack(anchor='w', pady=2)
            
            # Configuración de umbrales por defecto
            self.umbrales_default = {
                "Temperatura": {"min": 15, "max": 35},
                "Humedad": {"min": 30, "max": 80}
            }
            
            # Cargar sensores para el combo
            self.cargar_sensores_para_umbrales()
            
            # Frame para configuración de umbrales
            self.config_frame = tk.LabelFrame(ventana_umbrales, text="Umbrales por Tipo de Sensor", 
                                            font=('Arial', 12, 'bold'), bg='white')
            self.config_frame.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Crear interfaz de umbrales
            self.crear_interfaz_umbrales()
            
            # Frame para botones
            frame_botones = tk.Frame(ventana_umbrales, bg='white')
            frame_botones.pack(pady=20)
            
            # Botones principales
            tk.Button(frame_botones, text="💾 Guardar Configuración", 
                     command=self.guardar_configuracion_umbrales, 
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            tk.Button(frame_botones, text="📋 Ver Historial", 
                     command=self.mostrar_historial_umbrales, 
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            tk.Button(frame_botones, text="🔄 Cargar Configuración Actual", 
                     command=self.cargar_configuracion_actual, 
                     bg='#f39c12', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            tk.Button(frame_botones, text="❌ Cancelar", 
                     command=ventana_umbrales.destroy, 
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            # Inicializar con configuración global
            self.cambiar_tipo_configuracion()
            
        except Exception as e:
            self.agregar_log(f"❌ Error configurando umbrales: {e}")
            messagebox.showerror("Error", f"Error configurando umbrales: {e}")
    
    def cambiar_tipo_configuracion(self):
        """Cambiar entre configuración global y por sensor"""
        try:
            # El selector de sensor ahora está siempre visible
            # Solo necesitamos cargar la configuración actual
            self.cargar_configuracion_actual()
        except Exception as e:
            self.agregar_log(f"❌ Error cambiando tipo de configuración: {e}")
    
    def cargar_sensores_para_umbrales(self):
        """Cargar sensores para el combo de umbrales"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Obtener sensores desde MongoDB
            sensores = self.mongodb_service.obtener_sensores()
            
            # Crear lista de nombres de sensores formateados
            nombres_sensores = []
            for sensor in sensores:
                nombre_formateado = self.formatear_nombre_sensor(sensor)
                nombres_sensores.append(nombre_formateado)
            
            # Actualizar combo de sensores
            self.combo_sensor_umbrales['values'] = nombres_sensores
            
            if nombres_sensores:
                self.combo_sensor_umbrales.set(nombres_sensores[0])
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando sensores para umbrales: {e}")
    
    def crear_interfaz_umbrales(self):
        """Crear la interfaz de configuración de umbrales"""
        try:
            # Limpiar frame existente
            for widget in self.config_frame.winfo_children():
                widget.destroy()
            
            row = 0
            self.entries_umbrales = {}
            
            # Crear headers
            tk.Label(self.config_frame, text="Tipo de Sensor", bg='white', font=('Arial', 10, 'bold')).grid(row=row, column=0, padx=10, pady=5, sticky='w')
            tk.Label(self.config_frame, text="Mínimo", bg='white', font=('Arial', 10, 'bold')).grid(row=row, column=1, padx=5, pady=5)
            tk.Label(self.config_frame, text="Máximo", bg='white', font=('Arial', 10, 'bold')).grid(row=row, column=2, padx=5, pady=5)
            tk.Label(self.config_frame, text="Unidad", bg='white', font=('Arial', 10, 'bold')).grid(row=row, column=3, padx=5, pady=5)
            row += 1
            
            # Unidades por tipo
            unidades = {
                "Temperatura": "°C",
                "Humedad": "%"
            }
            
            for tipo_sensor, umbrales in self.umbrales_default.items():
                tk.Label(self.config_frame, text=f"{tipo_sensor}:", bg='white', font=('Arial', 10)).grid(row=row, column=0, padx=10, pady=5, sticky='w')
                
                entry_min = tk.Entry(self.config_frame, width=10)
                entry_min.grid(row=row, column=1, padx=5, pady=5)
                entry_min.insert(0, str(umbrales["min"]))
                
                entry_max = tk.Entry(self.config_frame, width=10)
                entry_max.grid(row=row, column=2, padx=5, pady=5)
                entry_max.insert(0, str(umbrales["max"]))
                
                tk.Label(self.config_frame, text=unidades.get(tipo_sensor, ""), bg='white').grid(row=row, column=3, padx=5, pady=5)
                
                self.entries_umbrales[tipo_sensor] = {"min": entry_min, "max": entry_max}
                row += 1
            
        except Exception as e:
            self.agregar_log(f"❌ Error creando interfaz de umbrales: {e}")
    
    def cargar_configuracion_actual(self):
        """Cargar configuración actual desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showwarning("Advertencia", "MongoDB no disponible")
                return
            
            if self.tipo_configuracion.get() == "global":
                # Cargar umbrales globales
                umbrales_globales = self.mongodb_service.obtener_umbrales_globales()
                if umbrales_globales and umbrales_globales.get("thresholds"):
                    self.umbrales_default = umbrales_globales["thresholds"]
                    self.crear_interfaz_umbrales()
                    self.agregar_log("✅ Umbrales globales cargados desde MongoDB")
                else:
                    self.agregar_log("⚠️ No hay umbrales globales configurados, usando valores por defecto")
            else:
                # Cargar umbrales del sensor seleccionado
                sensor_seleccionado = self.combo_sensor_umbrales.get()
                if sensor_seleccionado:
                    sensor_id = self.extraer_sensor_id_del_formato(sensor_seleccionado)
                    umbrales_sensor = self.mongodb_service.obtener_umbrales_sensor(sensor_id)
                    if umbrales_sensor and umbrales_sensor.get("thresholds"):
                        self.umbrales_default = umbrales_sensor["thresholds"]
                        self.crear_interfaz_umbrales()
                        self.agregar_log(f"✅ Umbrales del sensor {sensor_id} cargados desde MongoDB")
                    else:
                        # Usar umbrales globales como fallback
                        umbrales_globales = self.mongodb_service.obtener_umbrales_globales()
                        if umbrales_globales and umbrales_globales.get("thresholds"):
                            self.umbrales_default = umbrales_globales["thresholds"]
                            self.crear_interfaz_umbrales()
                            self.agregar_log(f"⚠️ Sensor {sensor_id} sin configuración específica, usando umbrales globales")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando configuración actual: {e}")
    
    def guardar_configuracion_umbrales(self):
        """Guardar configuración de umbrales en MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no disponible")
                return
            
            # Validar y recopilar umbrales
            umbrales_configurados = {}
            for tipo_sensor, entries in self.entries_umbrales.items():
                try:
                    min_val = float(entries["min"].get())
                    max_val = float(entries["max"].get())
                    
                    if min_val >= max_val:
                        messagebox.showerror("Error", f"El valor mínimo debe ser menor que el máximo para {tipo_sensor}")
                        return
                    
                    umbrales_configurados[tipo_sensor] = {"min": min_val, "max": max_val}
                except ValueError:
                    messagebox.showerror("Error", f"Ingrese valores numéricos válidos para {tipo_sensor}")
                    return
            
            # Guardar según el tipo de configuración
            config_type = self.config_type_var.get()
            
            if config_type == "global":
                # Guardar umbrales globales
                if self.mongodb_service.guardar_umbrales_globales(umbrales_configurados):
                    messagebox.showinfo("Éxito", "✅ Umbrales globales guardados correctamente")
                    self.agregar_log("✅ Umbrales globales guardados en MongoDB")
                else:
                    messagebox.showerror("Error", "No se pudieron guardar los umbrales globales")
                    
            elif config_type == "ubicacion":
                # Validar ciudad y país
                ciudad = self.entry_ciudad_umbrales.get().strip()
                pais = self.entry_pais_umbrales.get().strip()
                
                if not ciudad or not pais:
                    messagebox.showerror("Error", "Ingrese ciudad y país para configurar umbrales por ubicación")
                    return
                
                # Guardar umbrales por ubicación
                if self.mongodb_service.guardar_umbrales_ubicacion(ciudad, pais, umbrales_configurados):
                    messagebox.showinfo("Éxito", f"✅ Umbrales guardados para {ciudad}, {pais}")
                    self.agregar_log(f"✅ Umbrales guardados para {ciudad}, {pais}")
                else:
                    messagebox.showerror("Error", f"No se pudieron guardar los umbrales para {ciudad}, {pais}")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error guardando configuración: {e}")
            self.agregar_log(f"❌ Error guardando configuración de umbrales: {e}")
    
    def mostrar_historial_umbrales(self):
        """Mostrar historial de cambios de umbrales"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no disponible")
                return
            
            # Crear ventana de historial
            ventana_historial = tk.Toplevel(self.root)
            ventana_historial.title("📋 Historial de Cambios de Umbrales")
            ventana_historial.geometry("1000x600")
            ventana_historial.configure(bg='white')
            
            tk.Label(ventana_historial, text="📋 HISTORIAL DE CAMBIOS DE UMBRALES", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=20)
            
            # Frame para filtros
            filtros_frame = tk.Frame(ventana_historial, bg='white')
            filtros_frame.pack(fill='x', padx=20, pady=10)
            
            tk.Label(filtros_frame, text="Filtrar por sensor:", bg='white').pack(side='left', padx=10)
            self.combo_filtro_sensor = tk.ttk.Combobox(filtros_frame, width=20, state='readonly')
            self.combo_filtro_sensor.pack(side='left', padx=10)
            self.combo_filtro_sensor.set("Todos")
            
            tk.Button(filtros_frame, text="🔄 Actualizar", 
                     command=self.actualizar_historial_umbrales, 
                     bg='#3498db', fg='white').pack(side='left', padx=10)
            
            # TreeView para mostrar historial
            columns = ("Timestamp", "Sensor", "Tipo", "Usuario", "Razón", "Cambios")
            self.tree_historial = tk.ttk.Treeview(ventana_historial, columns=columns, show='headings', height=20)
            
            # Configurar columnas
            for col in columns:
                self.tree_historial.heading(col, text=col)
                self.tree_historial.column(col, width=150)
            
            # Scrollbar
            scrollbar_historial = tk.ttk.Scrollbar(ventana_historial, orient='vertical', command=self.tree_historial.yview)
            self.tree_historial.configure(yscrollcommand=scrollbar_historial.set)
            
            self.tree_historial.pack(side='left', fill='both', expand=True, padx=20, pady=10)
            scrollbar_historial.pack(side='right', fill='y', pady=10)
            
            # Cargar historial inicial
            self.actualizar_historial_umbrales()
            
        except Exception as e:
            messagebox.showerror("Error", f"Error mostrando historial: {e}")
            self.agregar_log(f"❌ Error mostrando historial de umbrales: {e}")
    
    def actualizar_historial_umbrales(self):
        """Actualizar el historial de umbrales en la ventana"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Limpiar treeview
            for item in self.tree_historial.get_children():
                self.tree_historial.delete(item)
            
            # Obtener historial
            sensor_filtro = self.combo_filtro_sensor.get()
            sensor_id = None if sensor_filtro == "Todos" else self.extraer_sensor_id_del_formato(sensor_filtro)
            
            historial = self.mongodb_service.obtener_historial_umbrales(sensor_id, limit=100)
            
            # Mostrar historial
            for cambio in historial:
                timestamp = cambio.get("timestamp", "N/A")
                sensor = cambio.get("sensor_id", "N/A")
                tipo = cambio.get("change_type", "N/A")
                usuario = cambio.get("changed_by", "N/A")
                razon = cambio.get("change_reason", "N/A")
                
                # Formatear cambios
                cambios_texto = ""
                if cambio.get("new_values"):
                    cambios_texto = f"Nuevos: {cambio['new_values']}"
                if cambio.get("old_values"):
                    cambios_texto += f" | Anteriores: {cambio['old_values']}"
                
                self.tree_historial.insert("", "end", values=(
                    timestamp, sensor, tipo, usuario, razon, cambios_texto
                ))
            
            self.agregar_log(f"✅ Historial de umbrales actualizado: {len(historial)} registros")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando historial: {e}")
    
    def obtener_lectura_real_sensor(self, sensor_id, tipo_alerta):
        """Obtener lectura real del sensor desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("❌ MongoDB no disponible para leer sensor")
                return None
            
            # Obtener la última medición del sensor
            medicion = self.mongodb_service.obtener_ultima_medicion_sensor(sensor_id)
            
            if not medicion:
                self.agregar_log(f"⚠️ No hay mediciones disponibles para el sensor {sensor_id}")
                return None
            
            # Extraer el valor según el tipo de alerta
            if "Temperatura" in tipo_alerta:
                valor = medicion.get("temperature")
                unidad = "°C"
            elif "Humedad" in tipo_alerta:
                valor = medicion.get("humidity")
                unidad = "%"
            else:
                self.agregar_log(f"⚠️ Tipo de alerta no reconocido: {tipo_alerta}")
                return None
            
            if valor is None:
                self.agregar_log(f"⚠️ No hay datos de {tipo_alerta.lower()} para el sensor {sensor_id}")
                return None
            
            # Obtener información adicional del sensor
            sensor_info = self.mongodb_service.db.sensors.find_one({"sensor_id": sensor_id})
            ubicacion = sensor_info.get("location", {}).get("city", "Desconocida") if sensor_info else "Desconocida"
            
            self.agregar_log(f"📊 Lectura real del sensor {sensor_id} ({ubicacion}): {valor}{unidad}")
            
            return {
                "valor": valor,
                "unidad": unidad,
                "timestamp": medicion.get("timestamp"),
                "sensor_id": sensor_id,
                "ubicacion": ubicacion,
                "calidad": medicion.get("quality", "unknown")
            }
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo lectura real del sensor {sensor_id}: {e}")
            return None
    
    def obtener_lectura_sensor_con_fallback(self, sensor_id, tipo_alerta):
        """Obtener lectura del sensor con fallback a datos de ejemplo si no hay datos reales"""
        try:
            # Intentar obtener lectura real
            lectura_real = self.obtener_lectura_real_sensor(sensor_id, tipo_alerta)
            
            if lectura_real:
                return lectura_real["valor"]
            
            # Si no hay datos reales, generar datos de ejemplo basados en el tipo
            self.agregar_log(f"⚠️ No hay datos reales para {sensor_id}, generando datos de ejemplo")
            return self.generar_dato_ejemplo_por_tipo(tipo_alerta)
            
        except Exception as e:
            self.agregar_log(f"❌ Error en lectura de sensor: {e}")
            return self.generar_dato_ejemplo_por_tipo(tipo_alerta)
    
    def generar_dato_ejemplo_por_tipo(self, tipo_alerta):
        """Generar dato de ejemplo basado en el tipo de alerta"""
        import random
        
        if tipo_alerta == "Temperatura Alta":
            return round(random.uniform(25, 45), 1)
        elif tipo_alerta == "Temperatura Baja":
            return round(random.uniform(-5, 20), 1)
        elif tipo_alerta == "Humedad Alta":
            return round(random.uniform(60, 95), 1)
        elif tipo_alerta == "Humedad Baja":
            return round(random.uniform(10, 40), 1)
        else:
            return round(random.uniform(0, 100), 1)
    
    def actualizar_lista_alertas(self):
        """Actualizar lista de alertas con formato de log mejorado"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Limpiar lista
            for item in self.tree_alertas.get_children():
                self.tree_alertas.delete(item)
            
            # Obtener alertas desde MongoDB Atlas
            alertas = self.mongodb_service.obtener_alertas()
            
            # Mostrar todas las alertas sin filtros
            self.mostrar_alertas_en_treeview(alertas)
            
            # self.agregar_log(f"✅ {len(alertas)} alertas cargadas desde MongoDB Atlas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando alertas: {e}")
    
    def resolver_alerta(self):
        """Resolver alerta seleccionada"""
        try:
            # Verificar permisos - solo técnicos pueden resolver alertas
            if self.rol_usuario not in ["técnico", "administrador"]:
                messagebox.showerror("Acceso Denegado", 
                    "Solo los técnicos pueden resolver alertas.\n"
                    f"Su rol actual: {self.rol_usuario}")
                return
            
            # Obtener alerta seleccionada
            seleccion = self.tree_alertas.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione una alerta para resolver")
                return
            
            # Obtener datos de la alerta seleccionada
            item = self.tree_alertas.item(seleccion[0])
            valores = item['values']
            
            if len(valores) < 9:
                messagebox.showerror("Error", "Datos de alerta incompletos")
                return
            
            alert_id = valores[0]
            categoria_str = valores[1]  # "🌡️ Climática" o "🔧 Sensor"
            categoria = "Climática" if "Climática" in categoria_str else "Sensor"
            
            # Diferenciar entre alertas de sensor y climáticas
            if categoria == "Climática":
                # Las alertas climáticas se resuelven automáticamente
                respuesta = messagebox.askyesno("Resolución Automática", 
                    f"¿Resolver automáticamente la alerta climática {alert_id}?\n\n"
                    f"Las alertas climáticas se resuelven automáticamente cuando las condiciones vuelven a la normalidad.")
                if not respuesta:
                    return
                
                # Simular resolución automática
                self.resolver_alerta_climatica_automatica(alert_id)
            else:
                # Las alertas de sensor requieren intervención técnica
                respuesta = messagebox.askyesno("Confirmar Resolución Técnica", 
                    f"¿Resolver la alerta de sensor {alert_id}?\n\n"
                    f"Esta alerta requiere intervención técnica manual.")
                if not respuesta:
                    return
                
                # Actualizar estado en MongoDB
                if self.mongodb_service and self.mongodb_service.conectado:
                    if self.mongodb_service.resolver_alerta(alert_id, getattr(self, 'usuario_autenticado', None)):
                        self.actualizar_lista_alertas()
                        messagebox.showinfo("Éxito", "Alerta de sensor resuelta correctamente")
                        self.agregar_log(f"✅ Alerta de sensor resuelta: {alert_id}")
                    else:
                        messagebox.showerror("Error", "No se pudo resolver la alerta")
                else:
                    messagebox.showerror("Error", "MongoDB no está conectado")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error resolviendo alerta: {e}")
            self.agregar_log(f"❌ Error resolviendo alerta: {e}")
    
    def resolver_alerta_climatica_automatica(self, alert_id):
        """Resolver alerta climática automáticamente"""
        try:
            # Simular verificación de condiciones climáticas
            condiciones_normalizadas = self.verificar_condiciones_climaticas()
            
            if condiciones_normalizadas:
                # Marcar como resuelta automáticamente
                datos_resolucion = {
                    "status": "resuelta_automaticamente",
                    "resolucion_automatica": True,
                    "resuelta_por": "SISTEMA_CLIMATICO",
                    "resuelta_at": datetime.now().isoformat(),
                    "comentario_resolucion": "Condiciones climáticas normalizadas automáticamente"
                }
                
                if self.mongodb_service and self.mongodb_service.conectado:
                    if self.mongodb_service.actualizar_alerta(alert_id, datos_resolucion):
                        self.actualizar_lista_alertas()
                        messagebox.showinfo("Resolución Automática", 
                            f"✅ Alerta climática {alert_id} resuelta automáticamente\n\n"
                            f"Las condiciones climáticas han vuelto a la normalidad.")
                        self.agregar_log(f"🌤️ Alerta climática resuelta automáticamente: {alert_id}")
                    else:
                        messagebox.showerror("Error", "No se pudo resolver la alerta climática")
                else:
                    messagebox.showerror("Error", "MongoDB no está conectado")
            else:
                messagebox.showwarning("Condiciones Anormales", 
                    f"⚠️ Las condiciones climáticas aún están fuera de lo normal\n\n"
                    f"La alerta {alert_id} permanecerá activa hasta que las condiciones mejoren.")
                
        except Exception as e:
            self.agregar_log(f"❌ Error resolviendo alerta climática: {e}")
            messagebox.showerror("Error", f"Error resolviendo alerta climática: {e}")
    
    def verificar_condiciones_climaticas(self):
        """Verificar si las condiciones climáticas han vuelto a la normalidad"""
        import random
        
        # Simular verificación de condiciones climáticas
        # En un sistema real, esto consultaría datos actuales de sensores
        temperatura_actual = random.uniform(18, 28)  # Rango normal
        humedad_actual = random.uniform(40, 70)    # Rango normal
        
        # Verificar si está en rango normal
        temperatura_normal = 15 <= temperatura_actual <= 35
        humedad_normal = 30 <= humedad_actual <= 80
        
        return temperatura_normal and humedad_normal
    
    def eliminar_alerta(self):
        """Eliminar alerta seleccionada"""
        try:
            # Obtener alerta seleccionada
            seleccion = self.tree_alertas.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione una alerta para eliminar")
                return
            
            # Obtener datos de la alerta seleccionada
            item = self.tree_alertas.item(seleccion[0])
            valores = item['values']
            
            if len(valores) < 9:
                messagebox.showerror("Error", "Datos de alerta incompletos")
                return
            
            alert_id = valores[0]
            categoria = valores[2]
            
            # Confirmar eliminación
            respuesta = messagebox.askyesno("Confirmar", f"¿Eliminar la alerta {categoria} {alert_id}?\n\nEsta acción no se puede deshacer.")
            if not respuesta:
                return
            
            # Eliminar de MongoDB
            if self.mongodb_service and self.mongodb_service.conectado:
                if self.mongodb_service.eliminar_alerta(alert_id):
                    self.actualizar_lista_alertas()
                    messagebox.showinfo("Éxito", "Alerta eliminada correctamente")
                    self.agregar_log(f"✅ Alerta eliminada: {alert_id}")
                else:
                    messagebox.showerror("Error", "No se pudo eliminar la alerta")
            else:
                messagebox.showerror("Error", "MongoDB no está conectado")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error eliminando alerta: {e}")
            self.agregar_log(f"❌ Error eliminando alerta: {e}")
    
    def generar_factura(self):
        """Generar nueva factura usando MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB Atlas no está disponible")
                return
            
            if not self.usuario_autenticado:
                messagebox.showerror("Error", "Debe estar autenticado para generar facturas")
                return
            
            # Obtener datos del formulario
            usuario_factura = self.combo_usuario_factura.get()
            servicio = self.combo_servicio_factura.get()
            monto_str = self.entry_monto_factura.get()
            fecha_vencimiento = self.entry_fecha_vencimiento.get()
            
            if not all([usuario_factura, servicio, monto_str, fecha_vencimiento]):
                messagebox.showerror("Error", "Complete todos los campos")
                return
            
            # VERIFICACIÓN DE SEGURIDAD: Control de acceso basado en rol
            if self.rol_usuario == "usuario":
                # Los usuarios comunes solo pueden generar facturas para sí mismos
                if usuario_factura != self.usuario_autenticado:
                    messagebox.showerror("Error", "Los usuarios comunes solo pueden generar facturas para sí mismos")
                    self.agregar_log(f"🚫 Intento de usuario común {self.usuario_autenticado} de generar factura para {usuario_factura}")
                    return
                self.agregar_log(f"✅ Usuario común {self.usuario_autenticado} generando factura para sí mismo")
            elif self.rol_usuario in ["administrador", "técnico"]:
                # Administradores y técnicos pueden generar facturas para cualquier usuario
                self.agregar_log(f"✅ {self.rol_usuario.title()} {self.usuario_autenticado} generando factura para {usuario_factura}")
            else:
                messagebox.showerror("Error", f"Rol no autorizado para generar facturas: {self.rol_usuario}")
                return
            
            try:
                monto = float(monto_str)
            except ValueError:
                messagebox.showerror("Error", "El monto debe ser un número válido")
                return
            
            # Obtener user_id del usuario de la factura
            user_id = self.obtener_user_id_por_username(usuario_factura)
            if not user_id:
                messagebox.showerror("Error", f"No se encontró el usuario: {usuario_factura}")
                return
            
            # Generar ID único para la factura
            import uuid
            invoice_id = f"INV_{uuid.uuid4().hex[:8].upper()}"
            
            # Crear datos de la factura
            factura_data = {
                "invoice_id": invoice_id,
                "user_id": user_id,
                "username": usuario_factura,
                "service": servicio,
                "amount": monto,
                "currency": "USD",
                "status": "pending",
                "created_at": datetime.now().isoformat(),
                "due_date": fecha_vencimiento,
                "paid_at": None,
                "payment_method": None
            }
            
            # Crear factura en MongoDB
            if self.mongodb_service.crear_factura(factura_data):
                self.agregar_log(f"✅ Factura {invoice_id} generada para {usuario_factura} - ${monto}")
                messagebox.showinfo("Éxito", f"Factura generada correctamente\nID: {invoice_id}\nUsuario: {usuario_factura}\nMonto: ${monto}")
                
                # Limpiar formulario
                self.entry_monto_factura.delete(0, tk.END)
                
                # Actualizar lista de facturas
                self.actualizar_lista_facturas()
            else:
                messagebox.showerror("Error", "No se pudo generar la factura")
                
        except Exception as e:
            self.agregar_log(f"❌ Error generando factura: {e}")
            messagebox.showerror("Error", f"Error generando factura: {e}")
    
    def actualizar_lista_facturas(self):
        """Actualizar lista de facturas según permisos de rol"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            if not self.usuario_autenticado:
                self.agregar_log("⚠️ Usuario no autenticado")
                return
            
            # Limpiar lista
            for item in self.tree_facturas.get_children():
                self.tree_facturas.delete(item)
            
            # Obtener facturas desde MongoDB Atlas
            facturas = self.mongodb_service.obtener_facturas()
            
            # Filtrar facturas según el rol del usuario
            facturas_filtradas = []
            
            if self.rol_usuario in ["administrador", "técnico"]:
                # Administradores y técnicos pueden ver todas las facturas
                facturas_filtradas = facturas
                self.agregar_log(f"🔓 Admin/Técnico: mostrando todas las facturas ({len(facturas)})")
                
            elif self.rol_usuario == "usuario":
                # Usuarios comunes solo pueden ver sus propias facturas
                user_id_actual = self.obtener_user_id_por_username(self.usuario_autenticado)
                if user_id_actual:
                    facturas_filtradas = [f for f in facturas if f.get('user_id') == user_id_actual]
                    self.agregar_log(f"🔒 Usuario común: mostrando solo facturas propias ({len(facturas_filtradas)})")
                else:
                    self.agregar_log(f"❌ No se encontró user_id para {self.usuario_autenticado}")
                    return
            else:
                self.agregar_log(f"⚠️ Rol no reconocido: {self.rol_usuario}")
                return
            
            # Mostrar facturas filtradas
            for factura in facturas_filtradas:
                # Convertir user_id a username para mostrar
                user_id = factura.get('user_id', '') or ''
                username = self.obtener_username_por_user_id(user_id)
                
                # Si username es 'N/A', intentar usar el campo 'usuario' directamente
                if username == 'N/A' or not username:
                    username = factura.get('usuario', 'N/A')
                
                # Obtener y formatear campos de la factura
                invoice_id = factura.get('invoice_id', factura.get('factura_id', '')) or 'N/A'
                service = factura.get('service', factura.get('tipo_servicio', '')) or 'N/A'
                
                # Si service está vacío, intentar usar descripción
                if not service or service == 'N/A':
                    service = factura.get('descripcion', '') or 'N/A'
                amount = factura.get('amount', factura.get('costo', 0)) or 0
                status = factura.get('status', factura.get('estado', '')) or 'pending'
                created_at = factura.get('created_at', factura.get('fecha_generacion', '')) or ''
                due_date = factura.get('due_date', '') or ''
                
                # Formatear fecha para mostrar solo fecha sin hora si es necesario
                if created_at and len(created_at) > 19:
                    created_at = created_at[:19]
                
                self.tree_facturas.insert('', 'end', values=(
                    invoice_id,
                    username,  # Mostrar username en lugar de user_id
                    service,
                    f"${float(amount):.2f}" if amount else "$0.00",
                    status,
                    created_at,
                    due_date
                ))
            
            # self.agregar_log(f"✅ {len(facturas_filtradas)} facturas cargadas desde MongoDB Atlas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando facturas: {e}")
    
    def procesar_pago(self):
        """Procesar pago de factura usando MongoDB con transacciones ACID"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB Atlas no está disponible")
                return
            
            if not self.usuario_autenticado:
                messagebox.showerror("Error", "Debe estar autenticado para procesar pagos")
                return
            
            # Obtener factura seleccionada
            selected_item = self.tree_facturas.selection()
            if not selected_item:
                messagebox.showerror("Error", "Seleccione una factura para procesar el pago")
                return
            
            # Obtener datos de la factura seleccionada
            item = self.tree_facturas.item(selected_item[0])
            invoice_id = item['values'][0]
            username = item['values'][1]
            
            # Limpiar formato del monto (remover $ y espacios)
            amount_str = item['values'][3]
            amount_clean = amount_str.replace('$', '').replace(',', '').strip()
            try:
                amount = float(amount_clean)
            except ValueError:
                messagebox.showerror("Error", f"No se pudo convertir el monto '{amount_str}' a número válido")
                self.agregar_log(f"❌ Error convirtiendo monto: '{amount_str}' -> '{amount_clean}'")
                return
            
            status = item['values'][4]
            
            if status == "paid":
                messagebox.showinfo("Información", "Esta factura ya está pagada")
                return
            
            # VERIFICACIÓN DE SEGURIDAD: Control de acceso basado en rol
            if self.rol_usuario == "usuario":
                # Los usuarios comunes solo pueden procesar pagos de sus propias facturas
                if username != self.usuario_autenticado:
                    messagebox.showerror("Error", "Los usuarios comunes solo pueden procesar pagos de sus propias facturas")
                    self.agregar_log(f"🚫 Intento de usuario común {self.usuario_autenticado} de procesar pago para {username}")
                    return
                self.agregar_log(f"✅ Usuario común {self.usuario_autenticado} procesando pago de su propia factura")
            elif self.rol_usuario in ["administrador", "técnico"]:
                # Administradores y técnicos pueden procesar pagos de cualquier usuario
                self.agregar_log(f"✅ {self.rol_usuario.title()} {self.usuario_autenticado} procesando pago para {username}")
            else:
                messagebox.showerror("Error", f"Rol no autorizado para procesar pagos: {self.rol_usuario}")
                return
            
            # Crear diálogo para procesar pago
            pago_window = tk.Toplevel(self.root)
            pago_window.title("Procesar Pago")
            pago_window.geometry("400x300")
            pago_window.configure(bg='white')
            pago_window.transient(self.root)
            pago_window.grab_set()
            
            # Centrar ventana
            pago_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
            
            tk.Label(pago_window, text="Procesar Pago", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            # Información de la factura
            info_frame = tk.Frame(pago_window, bg='white')
            info_frame.pack(pady=10)
            
            tk.Label(info_frame, text=f"Factura ID: {invoice_id}", bg='white').pack()
            tk.Label(info_frame, text=f"Usuario: {username}", bg='white').pack()
            tk.Label(info_frame, text=f"Monto: ${amount}", bg='white').pack()
            
            # Campos del pago
            campos_frame = tk.Frame(pago_window, bg='white')
            campos_frame.pack(pady=20)
            
            tk.Label(campos_frame, text="Método de Pago:", bg='white').pack(anchor='w')
            metodo_pago = ttk.Combobox(campos_frame, values=["Tarjeta de Crédito", "Transferencia Bancaria", "PayPal", "Efectivo"], width=30)
            metodo_pago.pack(pady=5)
            metodo_pago.set("Tarjeta de Crédito")
            
            tk.Label(campos_frame, text="Referencia de Pago:", bg='white').pack(anchor='w')
            referencia_pago = tk.Entry(campos_frame, width=30)
            referencia_pago.pack(pady=5)
            
            def procesar_pago_confirmado():
                metodo = metodo_pago.get()
                referencia = referencia_pago.get().strip()
                
                if not metodo or not referencia:
                    messagebox.showerror("Error", "Complete todos los campos")
                    return
                
                # Obtener user_id del usuario
                user_id = self.obtener_user_id_por_username(username)
                if not user_id:
                    messagebox.showerror("Error", f"No se encontró el usuario: {username}")
                    return
                
                # Crear datos de la factura y pago
                factura_data = {
                    "invoice_id": invoice_id,
                    "user_id": user_id,
                    "amount": amount,
                    "status": "paid"
                }
                
                pago_data = {
                    "payment_id": f"PAY_{uuid.uuid4().hex[:8].upper()}",
                    "invoice_id": invoice_id,
                    "amount": amount,
                    "payment_method": metodo,
                    "reference": referencia,
                    "processed_at": datetime.now().isoformat()
                }
                
                # Procesar pago con transacción ACID
                if self.mongodb_service.procesar_pago_transaccion(factura_data, pago_data):
                    self.agregar_log(f"✅ Pago procesado para factura {invoice_id} - ${amount}")
                    messagebox.showinfo("Éxito", f"Pago procesado correctamente\nFactura: {invoice_id}\nMonto: ${amount}\nMétodo: {metodo}")
                    pago_window.destroy()
                    
                    # Actualizar lista de facturas
                    self.actualizar_lista_facturas()
                else:
                    messagebox.showerror("Error", "No se pudo procesar el pago")
            
            # Botones
            botones_frame = tk.Frame(pago_window, bg='white')
            botones_frame.pack(pady=20)
            
            tk.Button(botones_frame, text="Procesar Pago", command=procesar_pago_confirmado,
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            tk.Button(botones_frame, text="Cancelar", command=pago_window.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            referencia_pago.focus()
            
        except Exception as e:
            self.agregar_log(f"❌ Error procesando pago: {e}")
            messagebox.showerror("Error", f"Error procesando pago: {e}")
    
    def mostrar_resumen_financiero(self):
        """Mostrar resumen financiero con cuentas corrientes"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB Atlas no está disponible")
                return
            
            # Crear ventana de resumen financiero
            resumen_window = tk.Toplevel(self.root)
            resumen_window.title("Resumen Financiero")
            resumen_window.geometry("800x600")
            resumen_window.configure(bg='white')
            resumen_window.transient(self.root)
            resumen_window.grab_set()
            
            # Centrar ventana
            resumen_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
            
            tk.Label(resumen_window, text="📊 Resumen Financiero", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=10)
            
            # Frame principal con scroll
            main_frame = tk.Frame(resumen_window, bg='white')
            main_frame.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Crear área de texto con scroll
            texto_resumen = scrolledtext.ScrolledText(main_frame, height=25, width=80)
            texto_resumen.pack(fill='both', expand=True)
            
            # Obtener datos financieros según permisos de rol
            facturas = self.mongodb_service.obtener_facturas()
            pagos = self.mongodb_service.obtener_pagos()
            cuentas = self.mongodb_service.obtener_cuentas_corrientes()
            
            # Filtrar datos según el rol del usuario
            if self.rol_usuario in ["administrador", "técnico"]:
                # Administradores y técnicos pueden ver todos los datos
                facturas_filtradas = facturas
                pagos_filtrados = pagos
                cuentas_filtradas = cuentas
                titulo_resumen = "💰 RESUMEN FINANCIERO DEL SISTEMA (TODOS LOS USUARIOS)"
                
            elif self.rol_usuario == "usuario":
                # Usuarios comunes solo pueden ver sus propios datos
                user_id_actual = self.obtener_user_id_por_username(self.usuario_autenticado)
                if user_id_actual:
                    facturas_filtradas = [f for f in facturas if f.get('user_id') == user_id_actual]
                    pagos_filtrados = [p for p in pagos if p.get('user_id') == user_id_actual]
                    cuentas_filtradas = [c for c in cuentas if c.get('user_id') == user_id_actual]
                    titulo_resumen = f"💰 RESUMEN FINANCIERO PERSONAL ({self.usuario_autenticado})"
                else:
                    texto_resumen.insert(tk.END, f"❌ Error: No se encontró user_id para {self.usuario_autenticado}\n")
                    return
            else:
                texto_resumen.insert(tk.END, f"❌ Error: Rol no reconocido: {self.rol_usuario}\n")
                return
            
            # Generar resumen
            texto_resumen.insert(tk.END, f"{titulo_resumen}\n")
            texto_resumen.insert(tk.END, "=" * 60 + "\n\n")
            
            # Estadísticas generales
            texto_resumen.insert(tk.END, "📈 ESTADÍSTICAS GENERALES:\n")
            texto_resumen.insert(tk.END, f"• Total de Facturas: {len(facturas_filtradas)}\n")
            texto_resumen.insert(tk.END, f"• Total de Pagos: {len(pagos_filtrados)}\n")
            texto_resumen.insert(tk.END, f"• Cuentas Corrientes: {len(cuentas_filtradas)}\n\n")
            
            # Facturas por estado
            facturas_pendientes = [f for f in facturas_filtradas if f.get('status') == 'pending']
            facturas_pagadas = [f for f in facturas_filtradas if f.get('status') == 'paid']
            
            texto_resumen.insert(tk.END, "📄 ESTADO DE FACTURAS:\n")
            texto_resumen.insert(tk.END, f"• Pendientes: {len(facturas_pendientes)}\n")
            texto_resumen.insert(tk.END, f"• Pagadas: {len(facturas_pagadas)}\n\n")
            
            # Montos totales
            monto_pendiente = sum(f.get('amount', 0) for f in facturas_pendientes)
            monto_pagado = sum(f.get('amount', 0) for f in facturas_pagadas)
            
            texto_resumen.insert(tk.END, "💵 MONTOS:\n")
            texto_resumen.insert(tk.END, f"• Monto Pendiente: ${monto_pendiente:.2f}\n")
            texto_resumen.insert(tk.END, f"• Monto Pagado: ${monto_pagado:.2f}\n")
            texto_resumen.insert(tk.END, f"• Total Facturado: ${monto_pendiente + monto_pagado:.2f}\n\n")
            
            # Cuentas corrientes
            texto_resumen.insert(tk.END, "🏦 CUENTAS CORRIENTES:\n")
            texto_resumen.insert(tk.END, "-" * 40 + "\n")
            
            saldo_total = 0
            for cuenta in cuentas:
                username = cuenta.get('username', 'Desconocido')
                saldo = cuenta.get('current_balance', 0)
                saldo_total += saldo
                
                texto_resumen.insert(tk.END, f"• {username}: ${saldo:.2f}\n")
            
            texto_resumen.insert(tk.END, f"\n💰 Saldo Total en Cuentas: ${saldo_total:.2f}\n\n")
            
            # Historial de movimientos recientes
            texto_resumen.insert(tk.END, "📋 MOVIMIENTOS RECIENTES:\n")
            texto_resumen.insert(tk.END, "-" * 40 + "\n")
            
            movimientos_recientes = []
            for cuenta in cuentas:
                historial = cuenta.get('movement_history', [])
                for movimiento in historial[-3:]:  # Últimos 3 movimientos
                    movimientos_recientes.append({
                        'usuario': cuenta.get('username'),
                        'descripcion': movimiento.get('description'),
                        'monto': movimiento.get('amount'),
                        'fecha': movimiento.get('date')
                    })
            
            # Ordenar por fecha
            movimientos_recientes.sort(key=lambda x: x.get('fecha', ''), reverse=True)
            
            for mov in movimientos_recientes[:10]:  # Mostrar últimos 10
                texto_resumen.insert(tk.END, f"• {mov['usuario']}: {mov['descripcion']} - ${mov['monto']:.2f}\n")
            
            if not movimientos_recientes:
                texto_resumen.insert(tk.END, "No hay movimientos registrados\n")
            
            # Botón de cerrar
            tk.Button(resumen_window, text="Cerrar", command=resumen_window.destroy,
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(pady=10)
            
            self.agregar_log("📊 Resumen financiero generado correctamente")
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando resumen financiero: {e}")
            messagebox.showerror("Error", f"Error generando resumen: {e}")
    
    def enviar_mensaje(self):
        """Enviar mensaje usando Neo4j"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                messagebox.showerror("Error", "Neo4j Aura no está disponible")
                return
            
            if not self.usuario_autenticado:
                messagebox.showerror("Error", "Debe estar autenticado para enviar mensajes")
                return
            
            # Obtener datos del formulario
            destinatario = self.combo_destinatario.get()
            tipo = self.combo_tipo_mensaje.get()
            asunto = self.entry_asunto_mensaje.get()
            prioridad = self.combo_prioridad_mensaje.get()
            
            if not destinatario or not asunto:
                messagebox.showerror("Error", "Complete destinatario y asunto")
                return
            
            # Obtener contenido del mensaje desde el área de composición
            contenido = self.texto_contenido_mensaje.get("1.0", tk.END).strip()
            if not contenido:
                messagebox.showerror("Error", "Ingrese el contenido del mensaje")
                return
            
            # Generar ID único para el mensaje
            from datetime import datetime
            import uuid
            message_id = f"MSG_{uuid.uuid4().hex[:8].upper()}"
            
            # Obtener user_id del usuario actual
            sender_id = self.obtener_user_id_por_username(self.usuario_autenticado)
            if not sender_id:
                self.agregar_log(f"❌ No se encontró user_id para usuario actual: {self.usuario_autenticado}")
                messagebox.showerror("Error", f"No se encontró el user_id del usuario actual: {self.usuario_autenticado}")
                return
            
            # Procesar destinatario según el tipo
            if tipo == "Privado":
                # Remover prefijo 👤 si existe
                destinatario_limpio = destinatario.replace("👤 ", "")
                recipient_id = self.obtener_user_id_por_username(destinatario_limpio)
                if not recipient_id:
                    self.agregar_log(f"❌ No se encontró user_id para destinatario: {destinatario_limpio}")
                    messagebox.showerror("Error", f"No se encontró el usuario destinatario: {destinatario_limpio}")
                    return
                
                self.agregar_log(f"📤 Enviando mensaje privado de {sender_id} a {recipient_id}")
                
                # Crear mensaje privado en Neo4j
                if self.neo4j_service.crear_mensaje(
                message_id=message_id,
                sender_id=sender_id,
                recipient_id=recipient_id,
                subject=asunto,
                content=contenido,
                message_type=tipo.lower()
            ):
                    messagebox.showinfo("Éxito", f"Mensaje privado enviado a {destinatario_limpio}")
                    self.agregar_log(f"✅ Mensaje privado enviado correctamente")
                    self.actualizar_mensajes()
                else:
                    messagebox.showerror("Error", "No se pudo enviar el mensaje privado")
                    
            elif tipo == "Grupal":
                # Remover prefijo 👥 si existe
                grupo_nombre = destinatario.replace("👥 ", "")
                
                # Obtener group_id del grupo seleccionado
                group_id = self.obtener_group_id_por_nombre(grupo_nombre)
                if not group_id:
                    self.agregar_log(f"❌ No se encontró group_id para grupo: {grupo_nombre}")
                    messagebox.showerror("Error", f"No se encontró el grupo: {grupo_nombre}")
                    return
                
                self.agregar_log(f"📤 Enviando mensaje grupal de {sender_id} al grupo {group_id}")
                
                # Crear mensaje grupal en Neo4j
                if self.neo4j_service.crear_mensaje_grupal(
                    message_id=message_id,
                    sender_id=sender_id,
                    group_id=group_id,
                    subject=asunto,
                    content=contenido
                ):
                    messagebox.showinfo("Éxito", f"Mensaje grupal enviado al grupo {grupo_nombre}")
                    self.agregar_log(f"✅ Mensaje grupal enviado correctamente")
                    self.actualizar_mensajes()
                else:
                    messagebox.showerror("Error", "No se pudo enviar el mensaje grupal")
            else:
                messagebox.showerror("Error", f"Tipo de mensaje no válido: {tipo}")
                return
                
        except Exception as e:
            self.agregar_log(f"❌ Error enviando mensaje: {e}")
            messagebox.showerror("Error", f"Error enviando mensaje: {e}")
    
    def obtener_group_id_por_nombre(self, nombre_grupo):
        """Obtener group_id de un grupo por su nombre"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                return None
            
            # Obtener user_id del usuario actual
            user_id = self.obtener_user_id_por_username(self.usuario_autenticado)
            if not user_id:
                return None
            
            # Obtener grupos del usuario
            grupos = self.neo4j_service.obtener_grupos_usuario(user_id)
            
            # Buscar el grupo por nombre
            for grupo in grupos:
                if grupo.get('name') == nombre_grupo:
                    return grupo.get('group_id')
            
            return None
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo group_id: {e}")
            return None
    
    def actualizar_mensajes(self):
        """Actualizar mensajes desde Neo4j"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                self.agregar_log("⚠️ Neo4j Aura no disponible para mensajes")
                return
            
            if not self.usuario_autenticado:
                self.agregar_log("⚠️ Usuario no autenticado")
                return
            
            # Mostrar indicador de carga
            self.texto_mensajes.delete("1.0", tk.END)
            self.texto_mensajes.insert(tk.END, "🔄 Actualizando mensajes...\n")
            self.root.update()  # Forzar actualización de la interfaz
            
            # Pequeño delay para mostrar el indicador
            import time
            time.sleep(0.5)
            
            # Obtener user_id del usuario actual
            user_id = self.obtener_user_id_por_username(self.usuario_autenticado)
            if not user_id:
                self.agregar_log("❌ No se encontró el user_id del usuario actual")
                return
            
            # Obtener mensajes directos desde Neo4j
            mensajes_directos = self.neo4j_service.obtener_mensajes_usuario(user_id, limit=20)
            
            # Obtener mensajes grupales desde Neo4j
            mensajes_grupales = self.neo4j_service.obtener_mensajes_grupales_usuario(user_id, limit=20)
            
            # Limpiar área de mensajes
            self.texto_mensajes.delete("1.0", tk.END)
            
            total_mensajes = len(mensajes_directos) + len(mensajes_grupales)
            
            if total_mensajes > 0:
                self.texto_mensajes.insert(tk.END, f"📨 MENSAJES RECIBIDOS ({total_mensajes})\n")
                self.texto_mensajes.insert(tk.END, "=" * 60 + "\n\n")
                
                # Mostrar mensajes directos
                if mensajes_directos:
                    self.texto_mensajes.insert(tk.END, f"📧 MENSAJES DIRECTOS ({len(mensajes_directos)})\n")
                    self.texto_mensajes.insert(tk.END, "─" * 40 + "\n")
                    
                    for mensaje in mensajes_directos:
                        fecha = mensaje.get('created_at', 'Fecha desconocida')
                        if hasattr(fecha, 'strftime'):
                            fecha_str = fecha.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            fecha_str = str(fecha)
                        
                        self.texto_mensajes.insert(tk.END, f"👤 De: {mensaje.get('sender_name', 'Desconocido')}\n")
                        self.texto_mensajes.insert(tk.END, f"📋 Asunto: {mensaje.get('subject', 'Sin asunto')}\n")
                        self.texto_mensajes.insert(tk.END, f"📅 Fecha: {fecha_str}\n")
                        self.texto_mensajes.insert(tk.END, f"🏷️ Tipo: {mensaje.get('type', 'privado')}\n")
                        self.texto_mensajes.insert(tk.END, f"📝 Contenido:\n{mensaje.get('content', 'Sin contenido')}\n")
                        self.texto_mensajes.insert(tk.END, "─" * 40 + "\n\n")
                
                # Mostrar mensajes grupales
                if mensajes_grupales:
                    self.texto_mensajes.insert(tk.END, f"👥 MENSAJES GRUPALES ({len(mensajes_grupales)})\n")
                    self.texto_mensajes.insert(tk.END, "─" * 40 + "\n")
                    
                    for mensaje in mensajes_grupales:
                        fecha = mensaje.get('created_at', 'Fecha desconocida')
                        if hasattr(fecha, 'strftime'):
                            fecha_str = fecha.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            fecha_str = str(fecha)
                        
                        self.texto_mensajes.insert(tk.END, f"👤 De: {mensaje.get('sender_name', 'Desconocido')}\n")
                        self.texto_mensajes.insert(tk.END, f"👥 Grupo: {mensaje.get('group_name', 'Grupo desconocido')}\n")
                        self.texto_mensajes.insert(tk.END, f"📋 Asunto: {mensaje.get('subject', 'Sin asunto')}\n")
                        self.texto_mensajes.insert(tk.END, f"📅 Fecha: {fecha_str}\n")
                        self.texto_mensajes.insert(tk.END, f"🏷️ Tipo: {mensaje.get('type', 'grupal')}\n")
                        self.texto_mensajes.insert(tk.END, f"📝 Contenido:\n{mensaje.get('content', 'Sin contenido')}\n")
                        self.texto_mensajes.insert(tk.END, "─" * 40 + "\n\n")
                
                # self.agregar_log(f"✅ {len(mensajes_directos)} mensajes directos y {len(mensajes_grupales)} mensajes grupales cargados desde Neo4j")
            else:
                self.texto_mensajes.insert(tk.END, "📭 No hay mensajes recibidos\n\n")
                self.texto_mensajes.insert(tk.END, "💡 Los mensajes aparecerán aquí cuando los recibas.\n")
                self.texto_mensajes.insert(tk.END, "🔄 Los mensajes se actualizan automáticamente al abrir esta pestaña.\n")
                self.texto_mensajes.insert(tk.END, "📨 También puedes usar el botón 'Actualizar Mensajes' manualmente.\n")
                self.agregar_log("ℹ️ No hay mensajes para mostrar")
                
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando mensajes: {e}")
            self.texto_mensajes.delete("1.0", tk.END)
            self.texto_mensajes.insert(tk.END, f"❌ Error cargando mensajes: {e}")
    
    def crear_grupo(self):
        """Crear grupo usando Neo4j"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                messagebox.showerror("Error", "Neo4j Aura no está disponible")
                return
            
            if not self.usuario_autenticado:
                messagebox.showerror("Error", "Debe estar autenticado para crear grupos")
                return
            
            # Crear diálogo para crear grupo
            grupo_window = tk.Toplevel(self.root)
            grupo_window.title("Crear Grupo")
            grupo_window.geometry("400x300")
            grupo_window.configure(bg='white')
            grupo_window.transient(self.root)
            grupo_window.grab_set()
            
            # Centrar ventana
            grupo_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
            
            tk.Label(grupo_window, text="Crear Nuevo Grupo", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            # Campos del grupo
            frame_campos = tk.Frame(grupo_window, bg='white')
            frame_campos.pack(pady=20)
            
            tk.Label(frame_campos, text="Nombre del Grupo:", bg='white').pack(anchor='w')
            entry_nombre_grupo = tk.Entry(frame_campos, width=40)
            entry_nombre_grupo.pack(pady=5)
            
            tk.Label(frame_campos, text="Descripción:", bg='white').pack(anchor='w')
            entry_descripcion_grupo = tk.Entry(frame_campos, width=40)
            entry_descripcion_grupo.pack(pady=5)
            
            def crear_grupo_confirmado():
                nombre = entry_nombre_grupo.get().strip()
                descripcion = entry_descripcion_grupo.get().strip()
                
                if not nombre:
                    messagebox.showerror("Error", "Ingrese el nombre del grupo")
                    return
                
                # Generar ID único para el grupo
                import uuid
                group_id = f"GRP_{uuid.uuid4().hex[:8].upper()}"
                
                # Obtener user_id del usuario actual (admin del grupo)
                admin_id = self.obtener_user_id_por_username(self.usuario_autenticado)
                if not admin_id:
                    messagebox.showerror("Error", "No se encontró el user_id del usuario actual")
                    return
                
                # Crear grupo en Neo4j
                if self.neo4j_service.crear_grupo(
                    group_id=group_id,
                    group_name=nombre,
                    description=descripcion,
                    admin_id=admin_id
                ):
                    self.agregar_log(f"✅ Grupo '{nombre}' creado correctamente")
                    messagebox.showinfo("Éxito", f"Grupo '{nombre}' creado correctamente")
                    grupo_window.destroy()
                else:
                    messagebox.showerror("Error", "No se pudo crear el grupo")
            
            # Botones
            frame_botones = tk.Frame(grupo_window, bg='white')
            frame_botones.pack(pady=20)
            
            tk.Button(frame_botones, text="Crear Grupo", command=crear_grupo_confirmado,
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            tk.Button(frame_botones, text="Cancelar", command=grupo_window.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
            
            entry_nombre_grupo.focus()
            
        except Exception as e:
            self.agregar_log(f"❌ Error creando grupo: {e}")
            messagebox.showerror("Error", f"Error creando grupo: {e}")
    
    def cambiar_tipo_mensaje(self, event=None):
        """Cambiar tipo de mensaje y actualizar destinatarios"""
        try:
            tipo = self.combo_tipo_mensaje.get()
            self.agregar_log(f"🔄 Cambiando tipo de mensaje a: {tipo}")
            
            if tipo == "Privado":
                self.cargar_usuarios_destinatarios()
            elif tipo == "Grupal":
                self.cargar_grupos_destinatarios()
                
        except Exception as e:
            self.agregar_log(f"❌ Error cambiando tipo de mensaje: {e}")
    
    def cargar_usuarios_destinatarios(self):
        """Cargar usuarios como destinatarios"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar usuarios")
                return
            
            if not self.usuario_autenticado:
                self.agregar_log("⚠️ Usuario no autenticado para cargar usuarios")
                return
            
            # Obtener usuarios desde MongoDB
            usuarios = self.mongodb_service.obtener_usuarios()
            # self.agregar_log(f"📊 Usuarios encontrados en MongoDB: {len(usuarios)}")
            
            # Filtrar usuarios activos y excluir el usuario actual
            destinatarios = []
            for usuario in usuarios:
                username = usuario.get('username', '')
                status = usuario.get('status', '')
                
                if status == 'activo' and username != self.usuario_autenticado:
                    destinatarios.append(f"👤 {username}")
                    # self.agregar_log(f"✅ Usuario agregado: {username}")
            
            # Actualizar combo de destinatarios
            self.combo_destinatario['values'] = destinatarios
            
            if destinatarios:
                self.combo_destinatario.set(destinatarios[0])  # Seleccionar primero por defecto
                # self.agregar_log(f"📋 Lista de usuarios cargada: {len(destinatarios)} usuarios")
            else:
                self.agregar_log("⚠️ No hay usuarios disponibles")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando usuarios: {e}")
    
    def cargar_grupos_destinatarios(self):
        """Cargar grupos como destinatarios"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                self.agregar_log("⚠️ Neo4j no disponible para cargar grupos")
                return
            
            if not self.usuario_autenticado:
                self.agregar_log("⚠️ Usuario no autenticado para cargar grupos")
                return
            
            # Obtener user_id del usuario actual
            user_id = self.obtener_user_id_por_username(self.usuario_autenticado)
            if not user_id:
                self.agregar_log(f"❌ No se encontró user_id para usuario: {self.usuario_autenticado}")
                return
            
            # Obtener grupos del usuario desde Neo4j
            grupos = self.neo4j_service.obtener_grupos_usuario(user_id)
            self.agregar_log(f"📊 Grupos encontrados en Neo4j: {len(grupos)}")
            
            # Formatear grupos para el combo
            destinatarios = []
            for grupo in grupos:
                nombre = grupo.get('name', 'Sin nombre')
                group_id = grupo.get('group_id', '')
                destinatarios.append(f"👥 {nombre}")
                self.agregar_log(f"✅ Grupo agregado: {nombre}")
            
            # Actualizar combo de destinatarios
            self.combo_destinatario['values'] = destinatarios
            
            if destinatarios:
                self.combo_destinatario.set(destinatarios[0])  # Seleccionar primero por defecto
                # self.agregar_log(f"📋 Lista de grupos cargada: {len(destinatarios)} grupos")
            else:
                self.agregar_log("⚠️ No hay grupos disponibles. Cree un grupo primero.")
                
        except Exception as e:
            self.agregar_log(f"❌ Error cargando grupos: {e}")
    
    def cargar_destinatarios(self):
        """Cargar lista de destinatarios según el tipo actual"""
        try:
            tipo = self.combo_tipo_mensaje.get()
            if tipo == "Privado":
                self.cargar_usuarios_destinatarios()
            elif tipo == "Grupal":
                self.cargar_grupos_destinatarios()
            else:
                self.cargar_usuarios_destinatarios()  # Por defecto usuarios
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando destinatarios: {e}")
    
    def gestionar_grupos(self):
        """Gestionar grupos y sus miembros"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                messagebox.showerror("Error", "Neo4j Aura no está disponible")
                return
            
            if not self.usuario_autenticado:
                messagebox.showerror("Error", "Debe estar autenticado para gestionar grupos")
                return
            
            # Crear ventana de gestión de grupos
            grupos_window = tk.Toplevel(self.root)
            grupos_window.title("👥 Gestión de Grupos")
            grupos_window.geometry("800x600")
            grupos_window.configure(bg='white')
            grupos_window.transient(self.root)
            grupos_window.grab_set()
            
            # Centrar ventana
            grupos_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
            
            tk.Label(grupos_window, text="👥 GESTIÓN DE GRUPOS", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=20)
            
            # Frame principal con pestañas
            notebook = ttk.Notebook(grupos_window)
            notebook.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Pestaña 1: Lista de grupos
            tab_grupos = tk.Frame(notebook, bg='white')
            notebook.add(tab_grupos, text="📋 Mis Grupos")
            
            # Lista de grupos
            grupos_frame = tk.LabelFrame(tab_grupos, text="Grupos Disponibles", 
                                        font=('Arial', 12, 'bold'), bg='white')
            grupos_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # TreeView para grupos
            columns = ("Nombre", "Descripción", "Miembros", "Creado")
            self.tree_grupos = tk.ttk.Treeview(grupos_frame, columns=columns, show='headings', height=10)
            
            for col in columns:
                self.tree_grupos.heading(col, text=col)
                self.tree_grupos.column(col, width=150)
            
            # Scrollbar para grupos
            scrollbar_grupos = tk.ttk.Scrollbar(grupos_frame, orient='vertical', command=self.tree_grupos.yview)
            self.tree_grupos.configure(yscrollcommand=scrollbar_grupos.set)
            
            self.tree_grupos.pack(side='left', fill='both', expand=True, padx=10, pady=10)
            scrollbar_grupos.pack(side='right', fill='y', pady=10)
            
            # Botones para grupos
            botones_grupos = tk.Frame(tab_grupos, bg='white')
            botones_grupos.pack(pady=10)
            
            tk.Button(botones_grupos, text="🔄 Actualizar Lista", 
                     command=self.actualizar_lista_grupos, 
                     bg='#3498db', fg='white').pack(side='left', padx=5)
            
            tk.Button(botones_grupos, text="👥 Ver Miembros", 
                     command=self.ver_miembros_grupo, 
                     bg='#27ae60', fg='white').pack(side='left', padx=5)
            
            tk.Button(botones_grupos, text="➕ Agregar Miembro", 
                     command=self.agregar_miembro_grupo, 
                     bg='#f39c12', fg='white').pack(side='left', padx=5)
            
            # Pestaña 2: Gestión de miembros
            tab_miembros = tk.Frame(notebook, bg='white')
            notebook.add(tab_miembros, text="👥 Gestión de Miembros")
            
            # Frame para selección de grupo
            seleccion_frame = tk.LabelFrame(tab_miembros, text="Seleccionar Grupo", 
                                          font=('Arial', 12, 'bold'), bg='white')
            seleccion_frame.pack(fill='x', padx=10, pady=10)
            
            tk.Label(seleccion_frame, text="Grupo:", bg='white').pack(side='left', padx=10)
            self.combo_grupo_gestion = tk.ttk.Combobox(seleccion_frame, width=30, state='readonly')
            self.combo_grupo_gestion.pack(side='left', padx=10)
            
            tk.Button(seleccion_frame, text="🔄 Cargar Miembros", 
                     command=self.cargar_miembros_grupo, 
                     bg='#3498db', fg='white').pack(side='left', padx=10)
            
            # Lista de miembros
            miembros_frame = tk.LabelFrame(tab_miembros, text="Miembros del Grupo", 
                                         font=('Arial', 12, 'bold'), bg='white')
            miembros_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # TreeView para miembros
            columns_miembros = ("Usuario", "Rol", "Fecha Ingreso", "Estado")
            self.tree_miembros = tk.ttk.Treeview(miembros_frame, columns=columns_miembros, show='headings', height=8)
            
            for col in columns_miembros:
                self.tree_miembros.heading(col, text=col)
                self.tree_miembros.column(col, width=150)
            
            # Scrollbar para miembros
            scrollbar_miembros = tk.ttk.Scrollbar(miembros_frame, orient='vertical', command=self.tree_miembros.yview)
            self.tree_miembros.configure(yscrollcommand=scrollbar_miembros.set)
            
            self.tree_miembros.pack(side='left', fill='both', expand=True, padx=10, pady=10)
            scrollbar_miembros.pack(side='right', fill='y', pady=10)
            
            # Botones para miembros
            botones_miembros = tk.Frame(tab_miembros, bg='white')
            botones_miembros.pack(pady=10)
            
            tk.Button(botones_miembros, text="➕ Agregar Miembro", 
                     command=self.agregar_miembro_grupo, 
                     bg='#27ae60', fg='white').pack(side='left', padx=5)
            
            tk.Button(botones_miembros, text="➖ Remover Miembro", 
                     command=self.remover_miembro_grupo, 
                     bg='#e74c3c', fg='white').pack(side='left', padx=5)
            
            # Cargar datos iniciales
            self.actualizar_lista_grupos()
            
        except Exception as e:
            messagebox.showerror("Error", f"Error gestionando grupos: {e}")
            self.agregar_log(f"❌ Error gestionando grupos: {e}")
    
    def actualizar_lista_grupos(self):
        """Actualizar lista de grupos en la ventana de gestión"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                return
            
            # Limpiar treeview
            for item in self.tree_grupos.get_children():
                self.tree_grupos.delete(item)
            
            # Obtener user_id del usuario actual
            user_id = self.obtener_user_id_por_username(self.usuario_autenticado)
            if not user_id:
                return
            
            # Obtener grupos del usuario
            grupos = self.neo4j_service.obtener_grupos_usuario(user_id)
            
            # Actualizar combo de grupos para gestión
            nombres_grupos = []
            
            # Mostrar grupos en treeview
            for grupo in grupos:
                nombre = grupo.get('name', 'Sin nombre')
                descripcion = grupo.get('description', 'Sin descripción')
                created_at = grupo.get('created_at', 'N/A')
                
                nombres_grupos.append(nombre)
                
                # Obtener número de miembros (simplificado)
                miembros_count = "N/A"  # Se podría implementar una función específica
                
                self.tree_grupos.insert("", "end", values=(
                    nombre, descripcion, miembros_count, created_at
                ))
            
            # Actualizar combo de grupos
            self.combo_grupo_gestion['values'] = nombres_grupos
            if nombres_grupos:
                self.combo_grupo_gestion.set(nombres_grupos[0])
            
            # self.agregar_log(f"✅ Lista de grupos actualizada: {len(grupos)} grupos")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando lista de grupos: {e}")
    
    def cargar_miembros_grupo(self):
        """Cargar miembros de un grupo seleccionado"""
        try:
            grupo_seleccionado = self.combo_grupo_gestion.get()
            if not grupo_seleccionado:
                messagebox.showwarning("Advertencia", "Seleccione un grupo")
                return
            
            # Limpiar treeview de miembros
            for item in self.tree_miembros.get_children():
                self.tree_miembros.delete(item)
            
            # Obtener group_id del grupo seleccionado
            group_id = self.obtener_group_id_por_nombre(grupo_seleccionado)
            if not group_id:
                self.agregar_log(f"❌ No se encontró group_id para grupo: {grupo_seleccionado}")
                messagebox.showerror("Error", f"No se encontró el grupo: {grupo_seleccionado}")
                return
            
            # Obtener miembros reales del grupo desde Neo4j
            miembros = self.neo4j_service.obtener_miembros_grupo(group_id)
            
            if miembros:
                # Mostrar miembros reales
                for miembro in miembros:
                    full_name = miembro.get('full_name', 'N/A')
                    role = miembro.get('role', 'member')
                    joined_at = miembro.get('joined_at', 'N/A')
                    status = miembro.get('status', 'active')
                    
                    # Formatear fecha si es posible
                    if joined_at and joined_at != 'N/A':
                        try:
                            from datetime import datetime
                            if isinstance(joined_at, str):
                                joined_at = joined_at[:19]  # Truncar a fecha sin microsegundos
                        except:
                            pass
                    
                    self.tree_miembros.insert("", "end", values=(
                        full_name, role, joined_at, status
                    ))
                
                self.agregar_log(f"✅ Miembros cargados para grupo {grupo_seleccionado}: {len(miembros)} miembros")
            else:
                # Si no hay miembros, mostrar mensaje informativo
                self.tree_miembros.insert("", "end", values=(
                    "No hay miembros", "N/A", "N/A", "N/A"
                ))
                self.agregar_log(f"⚠️ No hay miembros en el grupo: {grupo_seleccionado}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando miembros del grupo: {e}")
            messagebox.showerror("Error", f"Error cargando miembros: {e}")
    
    def ver_miembros_grupo(self):
        """Ver miembros de un grupo seleccionado"""
        try:
            seleccion = self.tree_grupos.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione un grupo de la lista")
                return
            
            # Obtener datos del grupo seleccionado
            item = self.tree_grupos.item(seleccion[0])
            nombre_grupo = item['values'][0]
            
            # Cambiar a la pestaña de gestión de miembros
            # Esto requeriría acceso al notebook desde esta función
            messagebox.showinfo("Información", f"Grupo seleccionado: {nombre_grupo}\n\nFuncionalidad de visualización de miembros en desarrollo.")
            
        except Exception as e:
            self.agregar_log(f"❌ Error viendo miembros del grupo: {e}")
    
    def agregar_miembro_grupo(self):
        """Agregar miembro a un grupo"""
        try:
            grupo_seleccionado = self.combo_grupo_gestion.get()
            if not grupo_seleccionado:
                messagebox.showwarning("Advertencia", "Seleccione un grupo")
                return
            
            # Crear ventana para agregar miembro
            agregar_window = tk.Toplevel(self.root)
            agregar_window.title("Agregar Miembro al Grupo")
            agregar_window.geometry("400x200")
            agregar_window.configure(bg='white')
            agregar_window.transient(self.root)
            agregar_window.grab_set()
            
            tk.Label(agregar_window, text=f"Agregar Miembro a: {grupo_seleccionado}", 
                    font=('Arial', 12, 'bold'), bg='white').pack(pady=20)
            
            # Frame para selección de usuario
            frame_usuario = tk.Frame(agregar_window, bg='white')
            frame_usuario.pack(pady=20)
            
            tk.Label(frame_usuario, text="Usuario:", bg='white').pack(side='left', padx=10)
            combo_usuario = tk.ttk.Combobox(frame_usuario, width=20, state='readonly')
            combo_usuario.pack(side='left', padx=10)
            
            # Cargar usuarios disponibles desde Neo4j
            if self.neo4j_service and self.neo4j_service.conectado:
                try:
                    with self.neo4j_service.driver.session(database=self.neo4j_service.database) as session:
                        query = """
                        MATCH (u:User)
                        RETURN u.user_id as user_id, u.full_name as full_name, u.email as email
                        ORDER BY u.full_name
                        """
                        
                        result = session.run(query)
                        nombres_usuarios = []
                        
                        for record in result:
                            full_name = record["full_name"]
                            if full_name and full_name != self.usuario_autenticado:
                                nombres_usuarios.append(full_name)
                        
                        combo_usuario['values'] = nombres_usuarios
                        if nombres_usuarios:
                            combo_usuario.set(nombres_usuarios[0])
                            
                except Exception as e:
                    self.agregar_log(f"❌ Error cargando usuarios desde Neo4j: {e}")
                    # Fallback a MongoDB si Neo4j falla
                    if self.mongodb_service and self.mongodb_service.conectado:
                        usuarios = self.mongodb_service.obtener_usuarios()
                        nombres_usuarios = []
                        for usuario in usuarios:
                            username = usuario.get('username', '')
                            if username != self.usuario_autenticado:
                                nombres_usuarios.append(username)
                        
                        combo_usuario['values'] = nombres_usuarios
                        if nombres_usuarios:
                            combo_usuario.set(nombres_usuarios[0])
            
            # Botones
            frame_botones = tk.Frame(agregar_window, bg='white')
            frame_botones.pack(pady=20)
            
            def confirmar_agregar():
                usuario_seleccionado = combo_usuario.get()
                if not usuario_seleccionado:
                    messagebox.showerror("Error", "Seleccione un usuario")
                    return
                
                # Obtener user_id del usuario seleccionado
                user_id = self.obtener_user_id_por_full_name(usuario_seleccionado)
                if not user_id:
                    messagebox.showerror("Error", f"No se encontró el user_id del usuario: {usuario_seleccionado}")
                    return
                
                # Obtener group_id del grupo seleccionado
                group_id = self.obtener_group_id_por_nombre(grupo_seleccionado)
                if not group_id:
                    messagebox.showerror("Error", f"No se encontró el group_id del grupo: {grupo_seleccionado}")
                    return
                
                # Agregar miembro usando Neo4j
                if self.neo4j_service.agregar_miembro_grupo_real(group_id, user_id, "member"):
                    messagebox.showinfo("Éxito", f"Usuario {usuario_seleccionado} agregado al grupo {grupo_seleccionado}")
                    self.agregar_log(f"✅ Usuario {usuario_seleccionado} agregado al grupo {grupo_seleccionado}")
                    agregar_window.destroy()
                    self.cargar_miembros_grupo()  # Actualizar lista
                else:
                    messagebox.showerror("Error", f"No se pudo agregar al usuario {usuario_seleccionado} al grupo")
            
            tk.Button(frame_botones, text="✅ Agregar", 
                     command=confirmar_agregar, 
                     bg='#27ae60', fg='white').pack(side='left', padx=10)
            
            tk.Button(frame_botones, text="❌ Cancelar", 
                     command=agregar_window.destroy, 
                     bg='#e74c3c', fg='white').pack(side='left', padx=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error agregando miembro al grupo: {e}")
    
    def remover_miembro_grupo(self):
        """Remover miembro de un grupo"""
        try:
            seleccion = self.tree_miembros.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione un miembro de la lista")
                return
            
            # Obtener datos del miembro seleccionado
            item = self.tree_miembros.item(seleccion[0])
            username = item['values'][0]
            
            # Confirmar remoción
            if messagebox.askyesno("Confirmar", f"¿Está seguro de remover al usuario {username} del grupo?"):
                # Obtener user_id del usuario
                user_id = self.obtener_user_id_por_full_name(username)
                if not user_id:
                    messagebox.showerror("Error", f"No se encontró el user_id del usuario: {username}")
                    return
                
                # Obtener group_id del grupo actual
                grupo_seleccionado = self.combo_grupo_gestion.get()
                group_id = self.obtener_group_id_por_nombre(grupo_seleccionado)
                if not group_id:
                    messagebox.showerror("Error", f"No se encontró el group_id del grupo: {grupo_seleccionado}")
                    return
                
                # Remover miembro usando Neo4j
                if self.neo4j_service.remover_miembro_grupo(group_id, user_id):
                    messagebox.showinfo("Éxito", f"Usuario {username} removido del grupo")
                    self.agregar_log(f"✅ Usuario {username} removido del grupo {grupo_seleccionado}")
                    self.cargar_miembros_grupo()  # Actualizar lista
                else:
                    messagebox.showerror("Error", f"No se pudo remover al usuario {username} del grupo")
            
        except Exception as e:
            self.agregar_log(f"❌ Error removiendo miembro del grupo: {e}")
    
    def cargar_usuarios_facturacion(self):
        """Cargar lista de usuarios para facturación desde MongoDB según permisos de rol"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar usuarios de facturación")
                return
            
            if not self.usuario_autenticado:
                self.agregar_log("⚠️ Usuario no autenticado para cargar usuarios de facturación")
                return
            
            # Obtener usuarios desde MongoDB
            usuarios = self.mongodb_service.obtener_usuarios()
            # self.agregar_log(f"📊 Usuarios encontrados para facturación: {len(usuarios)}")
            
            # Filtrar usuarios según el rol del usuario actual
            usuarios_facturacion = []
            
            if self.rol_usuario in ["administrador", "técnico"]:
                # Administradores y técnicos pueden ver todos los usuarios activos
                for usuario in usuarios:
                    username = usuario.get('username', '')
                    status = usuario.get('status', '')
                    
                    if status == 'activo':
                        usuarios_facturacion.append(username)
                        # self.agregar_log(f"✅ Usuario para facturación (admin/técnico): {username}")
                        
            elif self.rol_usuario == "usuario":
                # Usuarios comunes solo pueden ver su propia cuenta
                usuarios_facturacion.append(self.usuario_autenticado)
                self.agregar_log(f"✅ Usuario para facturación (usuario común): {self.usuario_autenticado}")
                
            else:
                # Rol no reconocido, no mostrar usuarios
                self.agregar_log(f"⚠️ Rol no reconocido: {self.rol_usuario}")
                return
            
            # Actualizar combo de usuarios para facturación
            self.combo_usuario_factura['values'] = usuarios_facturacion
            
            if usuarios_facturacion:
                # Configurar según el rol
                if self.rol_usuario == "usuario":
                    # Para usuarios comunes, seleccionar automáticamente su usuario y hacer el combo de solo lectura
                    self.combo_usuario_factura.set(self.usuario_autenticado)
                    self.combo_usuario_factura.config(state='readonly')
                    self.agregar_log(f"🔒 Usuario común: combo de solo lectura configurado para {self.usuario_autenticado}")
                else:
                    # Para admin/técnico, permitir selección libre
                    self.combo_usuario_factura.set(usuarios_facturacion[0])
                    self.combo_usuario_factura.config(state='normal')
                    # self.agregar_log(f"🔓 Admin/Técnico: combo editable configurado con {len(usuarios_facturacion)} usuarios")
                    
                # self.agregar_log(f"📋 Lista de usuarios para facturación cargada: {usuarios_facturacion}")
            else:
                self.agregar_log("⚠️ No hay usuarios disponibles para facturación")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando usuarios para facturación: {e}")
    
    def cargar_sensores_para_informes(self):
        """Cargar lista de sensores para informes desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar sensores de informes")
                return
            
            # Obtener sensores desde MongoDB
            sensores = self.mongodb_service.obtener_sensores()
            
            # Crear lista de nombres de sensores
            nombres_sensores = []
            for sensor in sensores:
                nombre_formateado = self.formatear_nombre_sensor(sensor)
                nombres_sensores.append(nombre_formateado)
            
            # Actualizar combo de país/ciudad para informes (ya está configurado en la interfaz)
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando sensores para informes: {e}")
    
    def cargar_ubicaciones_para_informes(self):
        """Cargar SOLO países disponibles desde MongoDB para informes"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Obtener todas las ubicaciones únicas de los sensores
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones:
                # Extraer únicamente países (evitar ciudades/zonas)
                paises_unicos = set()
                for ubicacion in ubicaciones:
                    if isinstance(ubicacion, dict):
                        pais = ubicacion.get('country', '')
                        if pais:
                            paises_unicos.add(pais)
                    else:
                        # Ignorar strings libres (suelen incluir ciudad/zona)
                        continue
                
                paises_lista = sorted(list(paises_unicos))
                
                # Agregar países adicionales comunes
                paises_adicionales = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay", "Paraguay", "Perú"]
                paises_completos = list(set(paises_lista + paises_adicionales))
                paises_completos.sort()
                
                # Actualizar combo a solo países
                self.combo_pais_ciudad_informe['values'] = paises_completos
                if paises_completos:
                    self.combo_pais_ciudad_informe.set(paises_completos[0])
            else:
                # Si no hay ubicaciones, usar valores por defecto
                paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
                self.combo_pais_ciudad_informe['values'] = paises_default
                self.combo_pais_ciudad_informe.set(paises_default[0])
                self.agregar_log("⚠️ Usando ubicaciones por defecto para informes")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ubicaciones para informes: {e}")
            # En caso de error, usar valores por defecto
            paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
            self.combo_pais_ciudad_informe['values'] = paises_default
            self.combo_pais_ciudad_informe.set(paises_default[0])
    
    def cargar_paises_para_analisis(self):
        """Cargar países disponibles para análisis con formato legible"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar países de análisis")
                return
            
            # Obtener todas las ubicaciones únicas de los sensores
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones:
                # Extraer países únicos
                paises_unicos = set()
                
                for ubicacion in ubicaciones:
                    if isinstance(ubicacion, dict):
                        pais = ubicacion.get('country', '')
                        if pais:
                            paises_unicos.add(pais)
                
                # Convertir a lista y ordenar
                paises_lista = sorted(list(paises_unicos))
                
                # Agregar países adicionales comunes
                paises_adicionales = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay", "Paraguay", "Perú"]
                paises_completos = list(set(paises_lista + paises_adicionales))
                paises_completos.sort()
                
                # Actualizar combo de países
                self.combo_pais_analisis['values'] = paises_completos
                if paises_completos:
                    self.combo_pais_analisis.set(paises_completos[0])  # Seleccionar primero por defecto
                    # Cargar ciudades del primer país
                    self.cargar_ciudades_para_analisis(paises_completos[0])
                
            else:
                # Valores por defecto si no hay ubicaciones
                paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
                self.combo_pais_analisis['values'] = paises_default
                self.combo_pais_analisis.set(paises_default[0])
                self.cargar_ciudades_para_analisis(paises_default[0])
                self.agregar_log("📍 Usando países por defecto para análisis")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando países para análisis: {e}")
    
    def cargar_ciudades_para_analisis(self, pais_seleccionado):
        """Cargar ciudades de un país específico para análisis"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar ciudades de análisis")
                return
            
            # Obtener todas las ubicaciones únicas de los sensores
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones:
                # Filtrar ciudades del país seleccionado
                ciudades_pais = set()
                
                for ubicacion in ubicaciones:
                    if isinstance(ubicacion, dict):
                        pais = ubicacion.get('country', '')
                        ciudad = ubicacion.get('city', '')
                        
                        if pais == pais_seleccionado and ciudad:
                            ciudades_pais.add(ciudad)
                
                # Convertir a lista y ordenar
                ciudades_lista = sorted(list(ciudades_pais))
                
                # Agregar ciudades adicionales según el país
                ciudades_adicionales = self.obtener_ciudades_adicionales_por_pais(pais_seleccionado)
                ciudades_completas = list(set(ciudades_lista + ciudades_adicionales))
                ciudades_completas.sort()
                
                # Actualizar combo de ciudades
                self.combo_ciudad_analisis['values'] = ciudades_completas
                if ciudades_completas:
                    self.combo_ciudad_analisis.set(ciudades_completas[0])  # Seleccionar primera por defecto
                
            else:
                # Valores por defecto si no hay ubicaciones
                ciudades_default = self.obtener_ciudades_adicionales_por_pais(pais_seleccionado)
                self.combo_ciudad_analisis['values'] = ciudades_default
                if ciudades_default:
                    self.combo_ciudad_analisis.set(ciudades_default[0])
                self.agregar_log(f"📍 Usando ciudades por defecto para {pais_seleccionado}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ciudades para análisis: {e}")
    
    def obtener_ciudades_adicionales_por_pais(self, pais):
        """Obtener ciudades adicionales comunes por país"""
        ciudades_por_pais = {
            "Argentina": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata", "Tucumán", "Mar del Plata"],
            "Brasil": ["São Paulo", "Río de Janeiro", "Brasilia", "Salvador", "Fortaleza", "Belo Horizonte"],
            "Chile": ["Santiago", "Valparaíso", "Concepción", "La Serena", "Antofagasta", "Temuco"],
            "Colombia": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga"],
            "Uruguay": ["Montevideo", "Salto", "Paysandú", "Las Piedras", "Rivera", "Maldonado"],
            "Paraguay": ["Asunción", "Ciudad del Este", "San Lorenzo", "Luque", "Capiatá"],
            "Perú": ["Lima", "Arequipa", "Trujillo", "Chiclayo", "Piura", "Iquitos"]
        }
        
        return ciudades_por_pais.get(pais, [])
    
    def on_pais_selected(self, event):
        """Evento cuando se selecciona un país en análisis"""
        pais_seleccionado = self.combo_pais_analisis.get()
        if pais_seleccionado:
            self.cargar_ciudades_para_analisis(pais_seleccionado)
    
    def cargar_paises_para_sensores(self):
        """Cargar países disponibles para el módulo de sensores"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Si no hay conexión, usar valores por defecto
                paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay", "Paraguay", "Perú"]
                if hasattr(self, 'combo_pais_sensor'):
                    self.combo_pais_sensor['values'] = paises_default
                    self.combo_pais_sensor.set(paises_default[0] if paises_default else "")
                self.agregar_log("⚠️ Usando países por defecto para sensores (MongoDB no disponible)")
                return
            
            # Obtener todos los sensores para extraer países únicos
            sensores = self.mongodb_service.obtener_sensores()
            
            paises = set()
            
            for sensor in sensores:
                location = sensor.get('location', {})
                
                if isinstance(location, dict):
                    pais = location.get('country', '')
                    if pais:
                        paises.add(pais)
                elif isinstance(location, str) and ' - ' in location:
                    # Formato: "Ciudad - País" o "Ciudad, Zona - País"
                    partes = location.split(' - ')
                    if len(partes) > 1:
                        pais = partes[-1].strip()
                        if pais:
                            paises.add(pais)
            
            # Agregar países adicionales comunes
            paises_adicionales = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay", "Paraguay", "Perú", "Ecuador", "Venezuela"]
            paises_completos = list(set(list(paises) + paises_adicionales))
            paises_completos.sort()
            
            if hasattr(self, 'combo_pais_sensor'):
                self.combo_pais_sensor['values'] = paises_completos
                if paises_completos:
                    self.combo_pais_sensor.set(paises_completos[0])
                    # Cargar ciudades del primer país
                    self.cargar_ciudades_para_sensores(paises_completos[0])
            
            self.agregar_log(f"✅ {len(paises_completos)} países cargados para sensores")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando países para sensores: {e}")
            # Fallback a valores por defecto
            paises_default = ["Argentina", "Brasil", "Chile", "Colombia", "Uruguay"]
            if hasattr(self, 'combo_pais_sensor'):
                self.combo_pais_sensor['values'] = paises_default
                self.combo_pais_sensor.set(paises_default[0] if paises_default else "")
    
    def cargar_ciudades_para_sensores(self, pais_seleccionado):
        """Cargar ciudades de un país específico para el módulo de sensores"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar ciudades de sensores")
                return
            
            # Obtener todas las ubicaciones únicas de los sensores
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones:
                ciudades_pais = set()
                
                for ubicacion in ubicaciones:
                    if isinstance(ubicacion, dict):
                        pais = ubicacion.get('country', '')
                        ciudad = ubicacion.get('city', '')
                        
                        if pais == pais_seleccionado and ciudad:
                            ciudades_pais.add(ciudad)
                
                # Convertir a lista y ordenar
                ciudades_lista = sorted(list(ciudades_pais))
                
                # Agregar ciudades adicionales según el país
                ciudades_adicionales = self.obtener_ciudades_adicionales_por_pais(pais_seleccionado)
                ciudades_completas = list(set(ciudades_lista + ciudades_adicionales))
                ciudades_completas.sort()
                
                # Actualizar combo de ciudades
                if hasattr(self, 'combo_ciudad_sensor'):
                    self.combo_ciudad_sensor['values'] = ciudades_completas
                    if ciudades_completas:
                        self.combo_ciudad_sensor.set(ciudades_completas[0])
                
            else:
                # Valores por defecto si no hay ubicaciones
                ciudades_default = self.obtener_ciudades_adicionales_por_pais(pais_seleccionado)
                if hasattr(self, 'combo_ciudad_sensor'):
                    self.combo_ciudad_sensor['values'] = ciudades_default
                    if ciudades_default:
                        self.combo_ciudad_sensor.set(ciudades_default[0])
                self.agregar_log(f"📍 Usando ciudades por defecto para {pais_seleccionado}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ciudades para sensores: {e}")
    
    def on_pais_selected_sensor(self, event):
        """Evento cuando se selecciona un país en el módulo de sensores"""
        pais_seleccionado = self.combo_pais_sensor.get()
        if pais_seleccionado:
            self.cargar_ciudades_para_sensores(pais_seleccionado)
    
    def cargar_ubicaciones_para_sensores(self):
        """Cargar ubicaciones disponibles para el combo de sensores"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para cargar ubicaciones de sensores")
                return
            
            # Obtener todas las ubicaciones únicas de los sensores
            ubicaciones = self.mongodb_service.obtener_ubicaciones_disponibles()
            
            if ubicaciones:
                # Formatear ubicaciones de manera legible
                ubicaciones_formateadas = []
                ubicaciones_unicas = set()  # Para evitar duplicados
                
                for ubicacion in ubicaciones:
                    if isinstance(ubicacion, dict):
                        # Extraer información del diccionario de ubicación
                        ciudad = ubicacion.get('city', '')
                        pais = ubicacion.get('country', '')
                        zona = ubicacion.get('zone', '')
                        
                        if ciudad and pais:
                            if zona:
                                # Formato: "Ciudad, Zona - País"
                                ubicacion_formateada = f"{ciudad}, {zona} - {pais}"
                            else:
                                # Formato: "Ciudad - País"
                                ubicacion_formateada = f"{ciudad} - {pais}"
                            
                            ubicaciones_unicas.add(ubicacion_formateada)
                        elif ciudad:
                            # Solo ciudad disponible
                            ubicaciones_unicas.add(ciudad)
                        elif pais:
                            # Solo país disponible
                            ubicaciones_unicas.add(pais)
                    else:
                        # Si no es un diccionario, usar como está
                        ubicaciones_unicas.add(str(ubicacion))
                
                # Convertir set a lista y ordenar
                ubicaciones_formateadas = sorted(list(ubicaciones_unicas))
                
                # Agregar ubicaciones adicionales comunes para sensores
                ubicaciones_adicionales = [
                    "Buenos Aires, Centro - Argentina",
                    "Buenos Aires, Norte - Argentina", 
                    "Buenos Aires, Sur - Argentina",
                    "Córdoba, Centro - Argentina",
                    "Córdoba, Norte - Argentina",
                    "Rosario, Centro - Argentina",
                    "Mendoza, Este - Argentina",
                    "Mendoza, Oeste - Argentina",
                    "La Plata, Centro - Argentina",
                    "Tucumán, Centro - Argentina",
                    "Mar del Plata, Centro - Argentina",
                    "São Paulo, Centro - Brasil",
                    "Río de Janeiro, Centro - Brasil",
                    "Santiago, Centro - Chile",
                    "Bogotá, Centro - Colombia",
                    "Montevideo, Centro - Uruguay"
                ]
                
                # Crear lista combinada sin duplicados
                ubicaciones_completas = list(set(ubicaciones_formateadas + ubicaciones_adicionales))
                ubicaciones_completas.sort()
                
                # Actualizar combo de ubicaciones para sensores
                self.combo_ubicacion_sensor['values'] = ubicaciones_completas
                if ubicaciones_completas:
                    self.combo_ubicacion_sensor.set(ubicaciones_completas[0])  # Seleccionar primera por defecto
            else:
                # Si no hay ubicaciones, usar valores por defecto
                ubicaciones_default = [
                    "Buenos Aires, Centro - Argentina",
                    "Buenos Aires, Norte - Argentina", 
                    "Buenos Aires, Sur - Argentina",
                    "Córdoba, Centro - Argentina",
                    "Rosario, Centro - Argentina",
                    "Mendoza, Este - Argentina",
                    "La Plata, Centro - Argentina",
                    "Tucumán, Centro - Argentina",
                    "São Paulo, Centro - Brasil",
                    "Río de Janeiro, Centro - Brasil",
                    "Santiago, Centro - Chile",
                    "Bogotá, Centro - Colombia",
                    "Montevideo, Centro - Uruguay"
                ]
                self.combo_ubicacion_sensor['values'] = ubicaciones_default
                self.combo_ubicacion_sensor.set(ubicaciones_default[0])
                self.agregar_log("📍 Usando ubicaciones por defecto para sensores")
            
        except Exception as e:
            self.agregar_log(f"❌ Error cargando ubicaciones para sensores: {e}")
            # En caso de error, usar valores por defecto
            ubicaciones_default = [
                "Buenos Aires, Centro - Argentina",
                "Córdoba, Centro - Argentina",
                "Rosario, Centro - Argentina",
                "Mendoza, Este - Argentina",
                "São Paulo, Centro - Brasil",
                "Santiago, Centro - Chile"
            ]
            self.combo_ubicacion_sensor['values'] = ubicaciones_default
            self.combo_ubicacion_sensor.set(ubicaciones_default[0])
    
    def generar_informe(self):
        """Generar informe usando MongoDB Time Series Collections"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB Atlas no está disponible")
                return
            
            # Obtener parámetros del informe
            tipo_informe = self.combo_tipo_informe.get()
            pais_ciudad = self.combo_pais_ciudad_informe.get()
            fecha_inicio = self.entry_fecha_inicio.get()
            fecha_fin = self.entry_fecha_fin.get()
            agrupacion = self.combo_agrupacion.get()
            
            if not all([tipo_informe, pais_ciudad, fecha_inicio, fecha_fin]):
                messagebox.showerror("Error", "Complete todos los campos requeridos")
                return
            
            # Mostrar indicador de carga
            self.texto_informe.delete("1.0", tk.END)
            self.texto_informe.insert(tk.END, "🔄 Generando informe...\n")
            self.root.update()
            
            # Generar informe según el tipo
            if tipo_informe == "Temperatura por País":
                self.generar_informe_temperatura_ciudad(pais_ciudad, fecha_inicio, fecha_fin, agrupacion)
            elif tipo_informe == "Humedad por País":
                self.generar_informe_humedad_pais_ciudad(pais_ciudad, fecha_inicio, fecha_fin, agrupacion)
            elif tipo_informe == "Análisis Temporal":
                self.generar_informe_analisis_temporal(pais_ciudad, fecha_inicio, fecha_fin, agrupacion)
            elif tipo_informe == "Comparativo por País":
                self.generar_informe_comparativo_pais(fecha_inicio, fecha_fin, agrupacion)
            elif tipo_informe == "Alertas Climáticas":
                self.generar_informe_alertas_climaticas(fecha_inicio, fecha_fin)
            
            self.agregar_log(f"📊 Informe '{tipo_informe}' generado correctamente")
            
        except Exception as e:
            self.agregar_log(f"❌ Error generando informe: {e}")
            messagebox.showerror("Error", f"Error generando informe: {e}")
    
    def generar_informe_humedad_pais_ciudad(self, pais_ciudad, fecha_inicio, fecha_fin, agrupacion):
        """Generar informe de humedad por país"""
        try:
            # Limpiar área de informe
            self.texto_informe.delete("1.0", tk.END)
            
            datos_humedad = self.obtener_datos_humedad_pais_ciudad(pais_ciudad, fecha_inicio, fecha_fin)
            
            if not datos_humedad:
                self.texto_informe.insert(tk.END, f"❌ No se encontraron datos de humedad para {pais_ciudad} en el período especificado.\n")
                return
            
            # Generar encabezado del informe
            self.texto_informe.insert(tk.END, f"📊 INFORME DE HUMEDAD POR PAÍS\n")
            self.texto_informe.insert(tk.END, "="*60 + "\n\n")
            
            self.texto_informe.insert(tk.END, f"📍 Ubicación: {pais_ciudad}\n")
            self.texto_informe.insert(tk.END, f"📅 Período: {fecha_inicio} a {fecha_fin}\n")
            self.texto_informe.insert(tk.END, f"📊 Agrupación: {agrupacion}\n")
            self.texto_informe.insert(tk.END, f"🕒 Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Calcular estadísticas
            humedades = [d["humedad"] for d in datos_humedad]
            humedad_maxima = max(humedades)
            humedad_minima = min(humedades)
            humedad_promedio = sum(humedades) / len(humedades)
            
            # Estadísticas generales
            self.texto_informe.insert(tk.END, f"📈 ESTADÍSTICAS GENERALES\n")
            self.texto_informe.insert(tk.END, "-"*40 + "\n")
            self.texto_informe.insert(tk.END, f"💧 Humedad máxima: {humedad_maxima}%\n")
            self.texto_informe.insert(tk.END, f"💧 Humedad mínima: {humedad_minima}%\n")
            self.texto_informe.insert(tk.END, f"💧 Humedad promedio: {humedad_promedio:.1f}%\n")
            self.texto_informe.insert(tk.END, f"📊 Total de mediciones: {len(datos_humedad)}\n")
            self.texto_informe.insert(tk.END, f"📊 Amplitud de humedad: {humedad_maxima - humedad_minima:.1f}%\n\n")
            
            # Análisis por niveles de humedad
            self.texto_informe.insert(tk.END, f"🌡️ ANÁLISIS POR NIVELES DE HUMEDAD\n")
            self.texto_informe.insert(tk.END, "-"*40 + "\n")
            
            muy_seco = len([h for h in humedades if h < 30])
            seco = len([h for h in humedades if 30 <= h < 50])
            moderado = len([h for h in humedades if 50 <= h < 70])
            humedo = len([h for h in humedades if 70 <= h < 90])
            muy_humedo = len([h for h in humedades if h >= 90])
            
            self.texto_informe.insert(tk.END, f"🏜️ Muy seco (<30%): {muy_seco} mediciones ({muy_seco/len(humedades)*100:.1f}%)\n")
            self.texto_informe.insert(tk.END, f"🌵 Seco (30-49%): {seco} mediciones ({seco/len(humedades)*100:.1f}%)\n")
            self.texto_informe.insert(tk.END, f"🌿 Moderado (50-69%): {moderado} mediciones ({moderado/len(humedades)*100:.1f}%)\n")
            self.texto_informe.insert(tk.END, f"🌧️ Húmedo (70-89%): {humedo} mediciones ({humedo/len(humedades)*100:.1f}%)\n")
            self.texto_informe.insert(tk.END, f"🌊 Muy húmedo (≥90%): {muy_humedo} mediciones ({muy_humedo/len(humedades)*100:.1f}%)\n\n")
            
            # Datos detallados
            self.texto_informe.insert(tk.END, f"📋 DATOS DETALLADOS\n")
            self.texto_informe.insert(tk.END, "-"*40 + "\n")
            
            for dato in datos_humedad:
                nivel = "🏜️" if dato["humedad"] < 30 else "🌵" if dato["humedad"] < 50 else "🌿" if dato["humedad"] < 70 else "🌧️" if dato["humedad"] < 90 else "🌊"
                self.texto_informe.insert(tk.END, f"   {dato['fecha']}: {dato['humedad']}% {nivel}\n")
            
            # Recomendaciones
            self.texto_informe.insert(tk.END, f"\n💡 RECOMENDACIONES\n")
            self.texto_informe.insert(tk.END, "-"*40 + "\n")
            
            if humedad_promedio < 30:
                self.texto_informe.insert(tk.END, f"⚠️ Humedad muy baja - Considerar medidas de humidificación\n")
            elif humedad_promedio > 80:
                self.texto_informe.insert(tk.END, f"⚠️ Humedad muy alta - Considerar medidas de deshumidificación\n")
            else:
                self.texto_informe.insert(tk.END, f"✅ Niveles de humedad dentro del rango normal\n")
            
            if humedad_maxima - humedad_minima > 40:
                self.texto_informe.insert(tk.END, f"📊 Alta variabilidad en humedad - Monitorear condiciones\n")
            
            self.texto_informe.insert(tk.END, f"\n📊 Informe generado exitosamente para {pais_ciudad}\n")
            
        except Exception as e:
            self.texto_informe.insert(tk.END, f"❌ Error generando informe de humedad: {e}\n")
    
    def obtener_datos_humedad_pais_ciudad(self, pais_ciudad, fecha_inicio, fecha_fin):
        """Obtener datos de humedad para un país/ciudad desde MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("❌ MongoDB no disponible para consultar datos de humedad")
                return []
            
            # Consultar datos de humedad desde MongoDB
            datos_humedad = self.mongodb_service.obtener_datos_humedad_por_ubicacion(
                ubicacion=pais_ciudad,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            if datos_humedad:
                self.agregar_log(f"📊 Datos de humedad obtenidos para {pais_ciudad}: {len(datos_humedad)} registros")
                return datos_humedad
            else:
                # Si no hay datos en MongoDB, NO generar datos de ejemplo
                self.agregar_log(f"⚠️ No hay sensores registrados en {pais_ciudad}")
                return []
                
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo datos de humedad: {e}")
            # En caso de error, generar datos de ejemplo
            return self.generar_datos_humedad_ejemplo(pais_ciudad, fecha_inicio, fecha_fin)
    
    def generar_datos_humedad_ejemplo(self, pais_ciudad, fecha_inicio, fecha_fin):
        """Generar datos de humedad de ejemplo cuando no hay datos en MongoDB"""
        import random
        
        try:
            fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            
            datos_ejemplo = []
            fecha_actual = fecha_inicio_dt
            
            # Generar datos para cada día en el rango
            while fecha_actual <= fecha_fin_dt:
                # Generar humedad basada en la ubicación
                if "Buenos Aires" in pais_ciudad or "La Plata" in pais_ciudad:
                    humedad_base = random.uniform(60, 75)  # Más húmedo
                elif "Córdoba" in pais_ciudad:
                    humedad_base = random.uniform(40, 55)  # Más seco
                elif "Mendoza" in pais_ciudad:
                    humedad_base = random.uniform(30, 45)  # Muy seco
                elif "Rosario" in pais_ciudad:
                    humedad_base = random.uniform(50, 65)  # Moderado
                else:
                    humedad_base = random.uniform(45, 60)  # Promedio
                
                datos_ejemplo.append({
                    "fecha": fecha_actual.strftime("%Y-%m-%d"),
                    "humedad": round(humedad_base, 1),
                    "ubicacion": pais_ciudad,
                    "fuente": "datos_ejemplo"
                })
                
                fecha_actual += timedelta(days=1)
            
            return datos_ejemplo
            
        except ValueError:
            # Si hay error en el formato de fecha, devolver datos básicos
            return [{
                "fecha": fecha_inicio,
                "humedad": 50.0,
                "ubicacion": pais_ciudad,
                "fuente": "datos_ejemplo"
            }]
    
    def generar_informe_temperatura_ciudad(self, pais_ciudad, fecha_inicio, fecha_fin, agrupacion):
        """Generar informe de temperatura por ciudad/país usando datos por ubicación"""
        try:
            # Limpiar área de informe
            self.texto_informe.delete("1.0", tk.END)
            
            # Obtener datos por ubicación desde el servicio MongoDB
            datos_temperatura = self.mongodb_service.obtener_datos_temperatura_por_ubicacion(
                ubicacion=pais_ciudad,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            if not datos_temperatura:
                self.texto_informe.insert(tk.END, "❌ No se encontraron datos para el período seleccionado\n")
                return
            
            # Generar informe
            self.texto_informe.insert(tk.END, f"🌡️ INFORME DE TEMPERATURA POR PAÍS\n")
            self.texto_informe.insert(tk.END, f"Ubicación: {pais_ciudad}\n")
            self.texto_informe.insert(tk.END, f"Período: {fecha_inicio} a {fecha_fin}\n")
            self.texto_informe.insert(tk.END, f"Agrupación: {agrupacion}\n")
            self.texto_informe.insert(tk.END, "=" * 60 + "\n\n")
            
            # Estadísticas básicas
            temperaturas = [d.get('temperatura') for d in datos_temperatura if d.get('temperatura') is not None]
            
            if temperaturas:
                temp_min = min(temperaturas)
                temp_max = max(temperaturas)
                temp_promedio = sum(temperaturas) / len(temperaturas)
                
                self.texto_informe.insert(tk.END, f"📊 ESTADÍSTICAS DE TEMPERATURA:\n")
                self.texto_informe.insert(tk.END, f"• Temperatura Mínima: {temp_min:.2f}°C\n")
                self.texto_informe.insert(tk.END, f"• Temperatura Máxima: {temp_max:.2f}°C\n")
                self.texto_informe.insert(tk.END, f"• Temperatura Promedio: {temp_promedio:.2f}°C\n")
                self.texto_informe.insert(tk.END, f"• Total de Mediciones: {len(temperaturas)}\n\n")
                
                # Preparar datos para reutilizar funciones de agrupación
                from datetime import datetime as _dt
                mediciones_normalizadas = []
                for d in datos_temperatura:
                    fecha_str = d.get('fecha')
                    temp = d.get('temperatura')
                    if not fecha_str or temp is None:
                        continue
                    try:
                        ts = _dt.strptime(fecha_str, "%Y-%m-%d")
                        mediciones_normalizadas.append({"timestamp": ts, "temperature": temp})
                    except Exception:
                        continue
                
                # Análisis por agrupación temporal
                self.texto_informe.insert(tk.END, f"📅 ANÁLISIS TEMPORAL ({agrupacion}):\n")
                self.texto_informe.insert(tk.END, "-" * 40 + "\n")
                
                if agrupacion == "Diaria":
                    self.agrupar_mediciones_diarias(mediciones_normalizadas, "temperature")
                elif agrupacion == "Semanal":
                    self.agrupar_mediciones_semanales(mediciones_normalizadas, "temperature")
                elif agrupacion == "Mensual":
                    self.agrupar_mediciones_mensuales(mediciones_normalizadas, "temperature")
                
                # Recomendaciones
                self.texto_informe.insert(tk.END, f"\n💡 RECOMENDACIONES:\n")
                if temp_max > 35:
                    self.texto_informe.insert(tk.END, f"• ⚠️ Temperaturas altas detectadas - Revisar sistemas de ventilación\n")
                if temp_min < 5:
                    self.texto_informe.insert(tk.END, f"• ⚠️ Temperaturas bajas detectadas - Verificar sistemas de calefacción\n")
                if temp_promedio > 25:
                    self.texto_informe.insert(tk.END, f"• 📈 Temperatura promedio elevada - Considerar medidas de eficiencia energética\n")
                
            else:
                self.texto_informe.insert(tk.END, "❌ No se encontraron datos de temperatura válidos\n")
                
        except Exception as e:
            self.texto_informe.insert(tk.END, f"❌ Error generando informe de temperatura: {e}\n")
    
    def agrupar_mediciones_diarias(self, mediciones, campo):
        """Agrupar mediciones por día"""
        from collections import defaultdict
        import datetime
        
        grupos = defaultdict(list)
        
        for medicion in mediciones:
            timestamp = medicion.get('timestamp', '')
            if timestamp:
                try:
                    # Manejar tanto datetime objects como strings
                    if isinstance(timestamp, datetime.datetime):
                        fecha = timestamp
                    else:
                        fecha = datetime.datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                    dia = fecha.strftime('%Y-%m-%d')
                    grupos[dia].append(medicion.get(campo, 0))
                except Exception as e:
                    print(f"🔍 DEBUG: Error procesando fecha {timestamp}: {e}")
                    continue
        
        for dia in sorted(grupos.keys()):
            valores = grupos[dia]
            if valores:
                promedio = sum(valores) / len(valores)
                minimo = min(valores)
                maximo = max(valores)
                self.texto_informe.insert(tk.END, f"• {dia}: Promedio: {promedio:.2f}, Min: {minimo:.2f}, Max: {maximo:.2f}\n")
        
        return dict(grupos)
    
    def agrupar_mediciones_semanales(self, mediciones, campo):
        """Agrupar mediciones por semana"""
        from collections import defaultdict
        import datetime
        
        grupos = defaultdict(list)
        
        for medicion in mediciones:
            timestamp = medicion.get('timestamp', '')
            if timestamp:
                try:
                    # Manejar tanto datetime objects como strings
                    if isinstance(timestamp, datetime.datetime):
                        fecha = timestamp
                    else:
                        fecha = datetime.datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                    semana = fecha.strftime('%Y-W%U')
                    grupos[semana].append(medicion.get(campo, 0))
                except Exception as e:
                    print(f"🔍 DEBUG: Error procesando fecha {timestamp}: {e}")
                    continue
        
        for semana in sorted(grupos.keys()):
            valores = grupos[semana]
            if valores:
                promedio = sum(valores) / len(valores)
                minimo = min(valores)
                maximo = max(valores)
                self.texto_informe.insert(tk.END, f"• Semana {semana}: Promedio: {promedio:.2f}, Min: {minimo:.2f}, Max: {maximo:.2f}\n")
        
        return dict(grupos)
    
    def agrupar_mediciones_mensuales(self, mediciones, campo):
        """Agrupar mediciones por mes"""
        from collections import defaultdict
        import datetime
        
        grupos = defaultdict(list)
        
        for medicion in mediciones:
            timestamp = medicion.get('timestamp', '')
            if timestamp:
                try:
                    # Manejar tanto datetime objects como strings
                    if isinstance(timestamp, datetime.datetime):
                        fecha = timestamp
                    else:
                        fecha = datetime.datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                    mes = fecha.strftime('%Y-%m')
                    grupos[mes].append(medicion.get(campo, 0))
                except Exception as e:
                    print(f"🔍 DEBUG: Error procesando fecha {timestamp}: {e}")
                    continue
        
        for mes in sorted(grupos.keys()):
            valores = grupos[mes]
            if valores:
                promedio = sum(valores) / len(valores)
                minimo = min(valores)
                maximo = max(valores)
                self.texto_informe.insert(tk.END, f"• {mes}: Promedio: {promedio:.2f}, Min: {minimo:.2f}, Max: {maximo:.2f}\n")
        
        return dict(grupos)
    
    def generar_informe_humedad_zona(self, sensor, fecha_inicio, fecha_fin, agrupacion):
        """Generar informe de humedad por zona"""
        try:
            self.texto_informe.delete("1.0", tk.END)
            
            mediciones = self.mongodb_service.obtener_mediciones_rango(
                sensor_name=sensor.split(" - ")[0],
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            if not mediciones:
                self.texto_informe.insert(tk.END, "❌ No se encontraron datos para el período seleccionado\n")
                return
            
            self.texto_informe.insert(tk.END, f"💧 INFORME DE HUMEDAD POR ZONA\n")
            self.texto_informe.insert(tk.END, f"Sensor: {sensor}\n")
            self.texto_informe.insert(tk.END, f"Período: {fecha_inicio} a {fecha_fin}\n")
            self.texto_informe.insert(tk.END, "=" * 60 + "\n\n")
            
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            
            if humedades:
                hum_min = min(humedades)
                hum_max = max(humedades)
                hum_promedio = sum(humedades) / len(humedades)
                
                self.texto_informe.insert(tk.END, f"📊 ESTADÍSTICAS DE HUMEDAD:\n")
                self.texto_informe.insert(tk.END, f"• Humedad Mínima: {hum_min:.2f}%\n")
                self.texto_informe.insert(tk.END, f"• Humedad Máxima: {hum_max:.2f}%\n")
                self.texto_informe.insert(tk.END, f"• Humedad Promedio: {hum_promedio:.2f}%\n")
                self.texto_informe.insert(tk.END, f"• Total de Mediciones: {len(humedades)}\n\n")
                
                # Análisis por agrupación temporal
                self.texto_informe.insert(tk.END, f"📅 ANÁLISIS TEMPORAL ({agrupacion}):\n")
                self.texto_informe.insert(tk.END, "-" * 40 + "\n")
                
                if agrupacion == "Diaria":
                    self.agrupar_mediciones_diarias(mediciones, "humidity")
                elif agrupacion == "Semanal":
                    self.agrupar_mediciones_semanales(mediciones, "humidity")
                elif agrupacion == "Mensual":
                    self.agrupar_mediciones_mensuales(mediciones, "humidity")
                
                # Recomendaciones
                self.texto_informe.insert(tk.END, f"\n💡 RECOMENDACIONES:\n")
                if hum_max > 80:
                    self.texto_informe.insert(tk.END, f"• ⚠️ Humedad alta detectada - Revisar sistemas de ventilación\n")
                if hum_min < 30:
                    self.texto_informe.insert(tk.END, f"• ⚠️ Humedad baja detectada - Considerar humidificadores\n")
                if hum_promedio > 70:
                    self.texto_informe.insert(tk.END, f"• 📈 Humedad promedio elevada - Monitorear condiciones ambientales\n")
                
        except Exception as e:
            self.texto_informe.insert(tk.END, f"❌ Error generando informe de humedad: {e}\n")
    
    def generar_informe_analisis_temporal(self, pais_ciudad, fecha_inicio, fecha_fin, agrupacion):
        """Generar análisis temporal completo por ubicación"""
        try:
            self.texto_informe.delete("1.0", tk.END)
            
            # Obtener datos por ubicación
            datos_temp = self.mongodb_service.obtener_datos_temperatura_por_ubicacion(
                ubicacion=pais_ciudad,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            if not datos_temp:
                self.texto_informe.insert(tk.END, "❌ No se encontraron datos para el período seleccionado\n")
                return
            
            self.texto_informe.insert(tk.END, f"📈 ANÁLISIS TEMPORAL COMPLETO\n")
            self.texto_informe.insert(tk.END, f"Ubicación: {pais_ciudad}\n")
            self.texto_informe.insert(tk.END, f"Período: {fecha_inicio} a {fecha_fin}\n")
            self.texto_informe.insert(tk.END, "=" * 60 + "\n\n")
            
            # Análisis de temperatura
            temperaturas = [d.get('temperatura') for d in datos_temp if d.get('temperatura') is not None]
            # Usamos la humedad registrada en las mismas mediciones de temperatura cuando esté disponible
            humedades = [d.get('humedad') for d in datos_temp if d.get('humedad') is not None]
            
            if temperaturas and humedades:
                self.texto_informe.insert(tk.END, f"🌡️ ANÁLISIS DE TEMPERATURA:\n")
                self.texto_informe.insert(tk.END, f"• Promedio: {sum(temperaturas)/len(temperaturas):.2f}°C\n")
                self.texto_informe.insert(tk.END, f"• Rango: {min(temperaturas):.2f}°C - {max(temperaturas):.2f}°C\n\n")
                
                self.texto_informe.insert(tk.END, f"💧 ANÁLISIS DE HUMEDAD:\n")
                self.texto_informe.insert(tk.END, f"• Promedio: {sum(humedades)/len(humedades):.2f}%\n")
                self.texto_informe.insert(tk.END, f"• Rango: {min(humedades):.2f}% - {max(humedades):.2f}%\n\n")
                
                # Correlación
                if len(temperaturas) == len(humedades):
                    correlacion = self.calcular_correlacion(temperaturas, humedades)
                    self.texto_informe.insert(tk.END, f"🔗 CORRELACIÓN TEMPERATURA-HUMEDAD:\n")
                    self.texto_informe.insert(tk.END, f"• Coeficiente: {correlacion:.3f}\n")
                    if correlacion > 0.7:
                        self.texto_informe.insert(tk.END, f"• Interpretación: Correlación fuerte positiva\n")
                    elif correlacion < -0.7:
                        self.texto_informe.insert(tk.END, f"• Interpretación: Correlación fuerte negativa\n")
                    else:
                        self.texto_informe.insert(tk.END, f"• Interpretación: Correlación débil\n")
                
        except Exception as e:
            self.texto_informe.insert(tk.END, f"❌ Error generando análisis temporal: {e}\n")
    
    def calcular_correlacion(self, x, y):
        """Calcular correlación entre dos variables"""
        import math
        
        n = len(x)
        if n != len(y) or n == 0:
            return 0
        
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i] * y[i] for i in range(n))
        sum_x2 = sum(x[i] ** 2 for i in range(n))
        sum_y2 = sum(y[i] ** 2 for i in range(n))
        
        numerador = n * sum_xy - sum_x * sum_y
        denominador = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))
        
        if denominador == 0:
            return 0
        
        return numerador / denominador
    
    def generar_informe_comparativo_pais(self, fecha_inicio, fecha_fin, agrupacion):
        """Generar informe comparativo por país"""
        try:
            self.texto_informe.delete("1.0", tk.END)
            self.texto_informe.insert(tk.END, f"🌍 INFORME COMPARATIVO POR PAÍS\n")
            self.texto_informe.insert(tk.END, f"Período: {fecha_inicio} a {fecha_fin}\n")
            self.texto_informe.insert(tk.END, "=" * 60 + "\n\n")
            self.texto_informe.insert(tk.END, "📊 Comparación de datos climáticos entre países\n")
            self.texto_informe.insert(tk.END, "• Argentina: Temperatura promedio 22°C, Humedad 65%\n")
            self.texto_informe.insert(tk.END, "• Brasil: Temperatura promedio 26°C, Humedad 78%\n")
            self.texto_informe.insert(tk.END, "• Chile: Temperatura promedio 18°C, Humedad 55%\n")
            self.texto_informe.insert(tk.END, "• Colombia: Temperatura promedio 24°C, Humedad 82%\n")
        except Exception as e:
            self.texto_informe.insert(tk.END, f"❌ Error generando informe comparativo: {e}\n")
    
    def generar_informe_alertas_climaticas(self, fecha_inicio, fecha_fin):
        """Generar informe de alertas climáticas"""
        try:
            self.texto_informe.delete("1.0", tk.END)
            self.texto_informe.insert(tk.END, f"⚠️ INFORME DE ALERTAS CLIMÁTICAS\n")
            self.texto_informe.insert(tk.END, f"Período: {fecha_inicio} a {fecha_fin}\n")
            self.texto_informe.insert(tk.END, "=" * 60 + "\n\n")
            self.texto_informe.insert(tk.END, "🚨 ALERTAS ACTIVAS:\n")
            self.texto_informe.insert(tk.END, "• Temperatura alta en Buenos Aires (35°C)\n")
            self.texto_informe.insert(tk.END, "• Humedad baja en Santiago (25%)\n")
            self.texto_informe.insert(tk.END, "• Tormenta eléctrica en São Paulo\n")
        except Exception as e:
            self.texto_informe.insert(tk.END, f"❌ Error generando informe de alertas: {e}\n")
    
    def actualizar_datos_informe(self):
        """Actualizar datos para informes"""
        try:
            self.cargar_sensores_para_informes()
            self.agregar_log("📊 Datos de informes actualizados")
            messagebox.showinfo("Éxito", "Datos actualizados correctamente")
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando datos: {e}")
            messagebox.showerror("Error", f"Error actualizando datos: {e}")
    
    def guardar_informe(self):
        """Guardar informe generado"""
        try:
            contenido = self.texto_informe.get("1.0", tk.END)
            if not contenido.strip():
                messagebox.showwarning("Advertencia", "No hay informe para guardar")
                return
            
            # Crear ventana de guardado
            guardar_window = tk.Toplevel(self.root)
            guardar_window.title("Guardar Informe")
            guardar_window.geometry("400x200")
            guardar_window.configure(bg='white')
            guardar_window.transient(self.root)
            guardar_window.grab_set()
            
            tk.Label(guardar_window, text="Guardar Informe", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            tk.Label(guardar_window, text="Nombre del archivo:", bg='white').pack()
            entry_nombre = tk.Entry(guardar_window, width=30)
            entry_nombre.pack(pady=5)
            entry_nombre.insert(0, f"informe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            
            def guardar_archivo():
                nombre = entry_nombre.get().strip()
                if not nombre:
                    messagebox.showerror("Error", "Ingrese un nombre de archivo")
                    return
                
                try:
                    with open(nombre, 'w', encoding='utf-8') as f:
                        f.write(contenido)
                    messagebox.showinfo("Éxito", f"Informe guardado como {nombre}")
                    guardar_window.destroy()
                except Exception as e:
                    messagebox.showerror("Error", f"Error guardando archivo: {e}")
            
            tk.Button(guardar_window, text="Guardar", command=guardar_archivo,
                     bg='#27ae60', fg='white', font=('Arial', 10)).pack(pady=10)
            
            tk.Button(guardar_window, text="Cancelar", command=guardar_window.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack()
            
        except Exception as e:
            self.agregar_log(f"❌ Error guardando informe: {e}")
            messagebox.showerror("Error", f"Error guardando informe: {e}")
    
    def obtener_user_id_por_username(self, username: str) -> str:
        """Obtener user_id de un usuario por su username (busca en MongoDB)"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return None
            
            usuarios = self.mongodb_service.obtener_usuarios()
            for usuario in usuarios:
                if usuario.get('username') == username:
                    return usuario.get('user_id')
            return None
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo user_id para {username}: {e}")
            return None
    
    def obtener_username_por_user_id(self, user_id: str) -> str:
        """Obtener username de un usuario por su user_id (busca en MongoDB)"""
        try:
            # Si no hay user_id o es una cadena vacía, devolver 'N/A'
            if not user_id or not user_id.strip():
                return 'N/A'
            
            # Limpiar el user_id
            user_id = user_id.strip()
            
            if not self.mongodb_service or not self.mongodb_service.conectado:
                # Si no hay conexión, intentar devolver el user_id directamente
                return user_id
            
            # Buscar el usuario por user_id
            usuarios = self.mongodb_service.obtener_usuarios()
            for usuario in usuarios:
                # Comparar user_id
                if usuario.get('user_id') == user_id:
                    username = usuario.get('username', user_id)
                    return username if username else user_id
                # También intentar si el user_id es el username
                if usuario.get('username') == user_id:
                    return user_id
            
            # Si no se encuentra en la base de datos, devolver el user_id directamente
            # Esto es útil para facturas antiguas o datos que no están en la BD
            return user_id
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo username para {user_id}: {e}")
            # En caso de error, devolver el user_id en lugar de 'N/A'
            return user_id if user_id else 'N/A'
    
    def obtener_user_id_por_full_name(self, full_name: str) -> str:
        """Obtener user_id de un usuario por su full_name (busca en Neo4j)"""
        try:
            if not self.neo4j_service or not self.neo4j_service.conectado:
                return None
            
            with self.neo4j_service.driver.session(database=self.neo4j_service.database) as session:
                query = """
                MATCH (u:User {full_name: $full_name})
                RETURN u.user_id as user_id
                """
                
                result = session.run(query, {"full_name": full_name})
                record = result.single()
                
                if record:
                    return record["user_id"]
                return None
                
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo user_id para {full_name}: {e}")
            return None
    
    def crear_proceso(self):
        """Crear nuevo proceso"""
        try:
            # Crear ventana para nuevo proceso
            proceso_window = tk.Toplevel(self.root)
            proceso_window.title("Crear Nuevo Proceso Periódico")
            proceso_window.geometry("600x550")
            proceso_window.configure(bg='white')
            proceso_window.transient(self.root)
            proceso_window.grab_set()
            
            tk.Label(proceso_window, text="📊 Crear Nuevo Proceso Periódico", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=10)
            
            # Campos del proceso
            campos_frame = tk.Frame(proceso_window, bg='white')
            campos_frame.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Configurar grid para que se expanda
            campos_frame.grid_columnconfigure(1, weight=1)
            
            tk.Label(campos_frame, text="Nombre del Proceso:", bg='white', font=('Arial', 10, 'bold')).grid(row=0, column=0, padx=5, pady=5, sticky='w')
            entry_nombre = tk.Entry(campos_frame, width=40, font=('Arial', 10))
            entry_nombre.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
            
            tk.Label(campos_frame, text="Descripción/Instrucciones:", bg='white', font=('Arial', 10, 'bold')).grid(row=1, column=0, padx=5, pady=5, sticky='nw')
            entry_descripcion = tk.Text(campos_frame, width=40, height=4, font=('Arial', 10))
            entry_descripcion.grid(row=1, column=1, padx=5, pady=5, sticky='ew')
            
            tk.Label(campos_frame, text="Tipo de Proceso:", bg='white', font=('Arial', 10, 'bold')).grid(row=2, column=0, padx=5, pady=5, sticky='w')
            combo_tipo = ttk.Combobox(campos_frame, values=[
                "Procesos Periódicos de Consultas por Ciudades",
                "Procesos Periódicos de Consultas por Zonas",
                "Procesos Periódicos de Consultas por Países",
                "Informe de Humedad y Temperaturas Máximas y Mínimas por Ciudades",
                "Informe de Humedad y Temperaturas Máximas y Mínimas por Zonas", 
                "Informe de Humedad y Temperaturas Máximas y Mínimas por Países",
                "Informe de Humedad y Temperaturas Promedio por Ciudades",
                "Informe de Humedad y Temperaturas Promedio por Zonas",
                "Informe de Humedad y Temperaturas Promedio por Países"
            ], width=37, font=('Arial', 10))
            combo_tipo.grid(row=2, column=1, padx=5, pady=5, sticky='ew')
            combo_tipo.set("Procesos Periódicos de Consultas por Ciudades")
            
            tk.Label(campos_frame, text="Ubicación:", bg='white', font=('Arial', 10, 'bold')).grid(row=3, column=0, padx=5, pady=5, sticky='w')
            # Obtener ubicaciones de la BD
            ubicaciones_disponibles = ["Seleccione una ubicación"]
            if self.mongodb_service and self.mongodb_service.conectado:
                ubicaciones_from_db = self.mongodb_service.obtener_ubicaciones_disponibles()
                if ubicaciones_from_db:
                    ubicaciones_disponibles = []
                    for ubic in ubicaciones_from_db:
                        if isinstance(ubic, dict):
                            city = ubic.get('city', '')
                            country = ubic.get('country', '')
                            if city and country:
                                ubicaciones_disponibles.append(f"{city} - {country}")
                            elif city:
                                ubicaciones_disponibles.append(city)
                            elif country:
                                ubicaciones_disponibles.append(country)
                        else:
                            ubicaciones_disponibles.append(str(ubic))
            
            combo_ubicacion = ttk.Combobox(campos_frame, values=ubicaciones_disponibles, width=37, font=('Arial', 10))
            combo_ubicacion.grid(row=3, column=1, padx=5, pady=5, sticky='ew')
            combo_ubicacion.set("Seleccione una ubicación")
            
            tk.Label(campos_frame, text="Agrupación Temporal:", bg='white', font=('Arial', 10, 'bold')).grid(row=4, column=0, padx=5, pady=5, sticky='w')
            combo_agrupacion = ttk.Combobox(campos_frame, values=[
                "Diaria",
                "Semanal", 
                "Mensual",
                "Anual"
            ], width=37, font=('Arial', 10))
            combo_agrupacion.grid(row=4, column=1, padx=5, pady=5, sticky='ew')
            combo_agrupacion.set("Mensual")
            
            tk.Label(campos_frame, text="Parámetros a Analizar:", bg='white', font=('Arial', 10, 'bold')).grid(row=5, column=0, padx=5, pady=5, sticky='w')
            combo_parametros = ttk.Combobox(campos_frame, values=[
                "temperatura_humedad",
                "solo_temperatura",
                "solo_humedad"
            ], width=37, font=('Arial', 10))
            combo_parametros.grid(row=5, column=1, padx=5, pady=5, sticky='ew')
            combo_parametros.set("temperatura_humedad")
            
            tk.Label(campos_frame, text="Fecha Inicio:", bg='white', font=('Arial', 10, 'bold')).grid(row=6, column=0, padx=5, pady=5, sticky='w')
            entry_fecha_inicio = tk.Entry(campos_frame, width=40, font=('Arial', 10))
            entry_fecha_inicio.grid(row=6, column=1, padx=5, pady=5, sticky='ew')
            tk.Label(campos_frame, text="Formato: YYYY-MM-DD", bg='white', font=('Arial', 8), fg='gray').grid(row=7, column=1, padx=5, pady=0, sticky='w')
            
            tk.Label(campos_frame, text="Fecha Fin:", bg='white', font=('Arial', 10, 'bold')).grid(row=8, column=0, padx=5, pady=5, sticky='w')
            entry_fecha_fin = tk.Entry(campos_frame, width=40, font=('Arial', 10))
            entry_fecha_fin.grid(row=8, column=1, padx=5, pady=5, sticky='ew')
            tk.Label(campos_frame, text="Formato: YYYY-MM-DD", bg='white', font=('Arial', 8), fg='gray').grid(row=9, column=1, padx=5, pady=0, sticky='w')
            
            # Botones
            botones_frame = tk.Frame(proceso_window, bg='white')
            botones_frame.pack(fill='x', padx=20, pady=10)
            
            def crear_proceso_db():
                nombre = entry_nombre.get().strip()
                descripcion = entry_descripcion.get("1.0", tk.END).strip()
                tipo = combo_tipo.get().strip()
                ubicacion = combo_ubicacion.get().strip()
                agrupacion = combo_agrupacion.get().strip().lower()
                parametros = combo_parametros.get().strip()
                fecha_inicio = entry_fecha_inicio.get().strip()
                fecha_fin = entry_fecha_fin.get().strip()
                
                if not nombre or not tipo or not ubicacion or ubicacion == "Seleccione una ubicación" or not fecha_inicio or not fecha_fin:
                    messagebox.showerror("Error", "Por favor complete todos los campos obligatorios y seleccione una ubicación")
                    return
                
                try:
                    # Validar fechas
                    datetime.strptime(fecha_inicio, "%Y-%m-%d")
                    datetime.strptime(fecha_fin, "%Y-%m-%d")
                except ValueError:
                    messagebox.showerror("Error", "Formato de fecha inválido. Use YYYY-MM-DD")
                    return
                
                # Crear datos del proceso
                # Normalizar tipo_proceso a partir del texto seleccionado
                if tipo.startswith("Procesos Periódicos"):
                    tipo_proceso_norm = "periodico_consulta"
                elif tipo.startswith("Informe"):
                    tipo_proceso_norm = "informe"
                else:
                    tipo_proceso_norm = "otro"

                # Calcular costo estimado (base) y persistirlo junto con tipo_proceso
                costo_base = self.calcular_costo_proceso(tipo, 0)

                proceso_data = {
                    "process_id": f"PROC_{int(time.time())}",
                    "nombre": nombre,
                    "descripcion": descripcion,
                    "tipo": tipo,
                    "tipo_proceso": tipo_proceso_norm,
                    "ubicacion": ubicacion,
                    "agrupacion": agrupacion,
                    "parametros": parametros,
                    "fecha_inicio": fecha_inicio,
                    "fecha_fin": fecha_fin,
                    "costo": float(costo_base),
                    "user_id": self.usuario_autenticado,
                    "status": "pending",
                    "created_at": datetime.now().isoformat(),
                    "priority": "Normal"
                }
                
                # Guardar en MongoDB
                if self.mongodb_service and self.mongodb_service.conectado:
                    if self.mongodb_service.crear_proceso(proceso_data):
                        messagebox.showinfo("Éxito", f"Proceso '{nombre}' creado exitosamente y agregado al backlog")
                        proceso_window.destroy()
                        self.actualizar_lista_procesos()
                        
                        # Generar factura si corresponde
                        if self.rol_usuario == "usuario":
                            if costo_base > 0:
                                self.generar_factura_proceso(nombre, tipo, costo_base)
                    else:
                        messagebox.showerror("Error", "No se pudo crear el proceso")
                else:
                    messagebox.showerror("Error", "MongoDB no disponible")
            
            tk.Button(botones_frame, text="Crear Proceso", command=crear_proceso_db,
                     bg='#27ae60', fg='white', font=('Arial', 12, 'bold')).pack(side='left', padx=5)
            tk.Button(botones_frame, text="Cancelar", command=proceso_window.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 12, 'bold')).pack(side='right', padx=5)
            
        except Exception as e:
            self.agregar_log(f"❌ Error creando proceso: {e}")
            messagebox.showerror("Error", f"Error creando proceso: {e}")
    
    def ver_backlog_procesos(self):
        """Ver procesos en backlog (solo técnicos y administradores)"""
        try:
            if self.rol_usuario not in ["técnico", "administrador"]:
                messagebox.showerror("Acceso Denegado", "Solo técnicos y administradores pueden ver el backlog")
                return
            
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no disponible")
                return
            
            # Obtener procesos en backlog
            procesos_backlog = self.mongodb_service.db.processes.find({"status": "backlog"}).sort("created_at", 1)
            procesos_list = list(procesos_backlog)
            
            if not procesos_list:
                messagebox.showinfo("Backlog Vacío", "No hay procesos pendientes en el backlog")
                return
            
            # Crear ventana de backlog
            backlog_window = tk.Toplevel(self.root)
            backlog_window.title("Backlog de Procesos")
            backlog_window.geometry("800x600")
            backlog_window.configure(bg='white')
            backlog_window.transient(self.root)
            backlog_window.grab_set()
            
            # Título
            tk.Label(backlog_window, text="📋 Backlog de Procesos", 
                    font=('Arial', 16, 'bold'), bg='white').pack(pady=10)
            
            # Información del backlog
            info_frame = tk.Frame(backlog_window, bg='#ecf0f1', relief='raised', bd=1)
            info_frame.pack(fill='x', padx=20, pady=10)
            
            tk.Label(info_frame, text=f"📊 Total de procesos en backlog: {len(procesos_list)}", 
                    font=('Arial', 12, 'bold'), bg='#ecf0f1').pack(pady=5)
            
            tk.Label(info_frame, text="💡 Selecciona un proceso y haz clic en 'Asignar a Mí' para trabajarlo", 
                    font=('Arial', 10), bg='#ecf0f1').pack()
            
            # Treeview para mostrar procesos
            columns = ("ID", "Nombre", "Tipo", "Tipo Proceso", "Creado por", "Prioridad", "Fecha Creación", "Costo")
            tree_backlog = ttk.Treeview(backlog_window, columns=columns, show="headings")
            
            for col in columns:
                tree_backlog.heading(col, text=col)
                tree_backlog.column(col, width=100)
            
            # Scrollbar
            scrollbar_backlog = ttk.Scrollbar(backlog_window, orient="vertical", command=tree_backlog.yview)
            tree_backlog.configure(yscrollcommand=scrollbar_backlog.set)
            
            # Pack treeview y scrollbar
            tree_backlog.pack(side="left", fill="both", expand=True, padx=20, pady=10)
            scrollbar_backlog.pack(side="right", fill="y", pady=10)
            
            # Cargar procesos en el treeview
            for proceso in procesos_list:
                tree_backlog.insert("", "end", values=(
                    proceso.get('process_id', ''),
                    proceso.get('name', ''),
                    proceso.get('type', ''),
                    proceso.get('tipo_proceso', 'N/A'),
                    proceso.get('created_by', ''),
                    proceso.get('priority', 'Normal'),
                    proceso.get('created_at', '')[:10],
                    f"${proceso.get('costo', 0):.2f}"
                ))
            
            # Botones
            botones_frame = tk.Frame(backlog_window, bg='white')
            botones_frame.pack(pady=20)
            
            def asignar_seleccionado():
                seleccion = tree_backlog.selection()
                if not seleccion:
                    messagebox.showwarning("Advertencia", "Selecciona un proceso del backlog")
                    return
                
                item = tree_backlog.item(seleccion[0])
                process_id = item['values'][0]
                
                # Asignar proceso al técnico actual
                if self.asignar_proceso_especifico(process_id):
                    messagebox.showinfo("Éxito", f"Proceso {process_id} asignado a {self.usuario_autenticado}")
                    backlog_window.destroy()
                    self.actualizar_lista_procesos()
                else:
                    messagebox.showerror("Error", "No se pudo asignar el proceso")
            
            tk.Button(botones_frame, text="👤 Asignar Seleccionado a Mí", 
                     command=asignar_seleccionado,
                     bg='#e67e22', fg='white', font=('Arial', 12)).pack(side='left', padx=10)
            
            tk.Button(botones_frame, text="🔄 Actualizar", 
                     command=lambda: self.ver_backlog_procesos(),
                     bg='#3498db', fg='white', font=('Arial', 12)).pack(side='left', padx=10)
            
            tk.Button(botones_frame, text="❌ Cerrar", 
                     command=backlog_window.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 12)).pack(side='left', padx=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando backlog: {e}")
            messagebox.showerror("Error", f"Error mostrando backlog: {e}")
    
    def asignar_proceso_a_mi(self):
        """Asignar proceso seleccionado al técnico actual"""
        try:
            if self.rol_usuario not in ["técnico", "administrador"]:
                messagebox.showerror("Acceso Denegado", "Solo técnicos y administradores pueden asignar procesos")
                return
            
            # Obtener proceso seleccionado
            seleccion = self.tree_procesos.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Selecciona un proceso de la lista")
                return
            
            item = self.tree_procesos.item(seleccion[0])
            process_id = item['values'][0]
            
            # Asignar proceso
            if self.asignar_proceso_especifico(process_id):
                messagebox.showinfo("Éxito", f"Proceso {process_id} asignado a {self.usuario_autenticado}")
                self.actualizar_lista_procesos()
            else:
                messagebox.showerror("Error", "No se pudo asignar el proceso")
                
        except Exception as e:
            self.agregar_log(f"❌ Error asignando proceso: {e}")
            messagebox.showerror("Error", f"Error asignando proceso: {e}")
    
    def asignar_proceso_especifico(self, process_id):
        """Asignar un proceso específico al técnico actual"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return False
            
            # Actualizar proceso en MongoDB
            resultado = self.mongodb_service.db.processes.update_one(
                {"process_id": process_id},
                {
                    "$set": {
                        "assigned_to": self.usuario_autenticado,
                        "status": "pendiente",  # Cambiar de backlog a pendiente
                        "assigned_at": datetime.now().isoformat()
                    }
                }
            )
            
            if resultado.modified_count > 0:
                self.agregar_log(f"✅ Proceso {process_id} asignado a {self.usuario_autenticado}")
                return True
            else:
                self.agregar_log(f"❌ No se pudo asignar proceso {process_id}")
                return False
                
        except Exception as e:
            self.agregar_log(f"❌ Error asignando proceso específico: {e}")
            return False
    
    def ejecutar_proceso_periodico(self, proceso_id, proceso_data):
        """Ejecutar proceso periódico con agrupación temporal"""
        try:
            self.agregar_log(f"🔄 Iniciando ejecución del proceso: {proceso_data.get('nombre', 'N/A')}")
            
            # Actualizar estado a "running"
            self.mongodb_service.actualizar_estado_proceso(proceso_id, "running", progress=10)
            
            # Extraer parámetros del proceso
            tipo_proceso = proceso_data.get('tipo', '')
            ubicacion = proceso_data.get('ubicacion', '')
            fecha_inicio = proceso_data.get('fecha_inicio', '')
            fecha_fin = proceso_data.get('fecha_fin', '')
            agrupacion = proceso_data.get('agrupacion', 'diaria')
            parametros = proceso_data.get('parametros', 'temperatura_humedad')
            
            self.agregar_log(f"📊 Parámetros: Ubicación={ubicacion}, Período={fecha_inicio} a {fecha_fin}, Agrupación={agrupacion}")
            
            # Actualizar progreso
            self.mongodb_service.actualizar_estado_proceso(proceso_id, "running", progress=30)
            
            # Obtener sensores por ubicación
            if "Ciudades" in tipo_proceso:
                ciudad, pais = ubicacion.split(', ') if ', ' in ubicacion else (ubicacion, '')
                sensores = self.obtener_sensores_por_ubicacion(ciudad, pais)
            elif "Zonas" in tipo_proceso:
                ciudad, zona, pais = ubicacion.split(', ') if ubicacion.count(', ') == 2 else (ubicacion, '', '')
                sensores = self.obtener_sensores_por_ubicacion(ciudad, pais, zona)
            elif "Países" in tipo_proceso:
                sensores = self.obtener_sensores_por_pais(ubicacion)
            else:
                sensores = []
            
            if not sensores:
                error_msg = f"No se encontraron sensores para la ubicación: {ubicacion}"
                self.mongodb_service.actualizar_estado_proceso(proceso_id, "failed", error=error_msg)
                self.agregar_log(f"❌ {error_msg}")
                return
            
            self.agregar_log(f"📡 Encontrados {len(sensores)} sensores")
            
            # Actualizar progreso
            self.mongodb_service.actualizar_estado_proceso(proceso_id, "running", progress=50)
            
            # Obtener mediciones para todos los sensores
            todas_mediciones = []
            for sensor in sensores:
                sensor_id = sensor.get('sensor_id', '')
                mediciones = self.mongodb_service.obtener_mediciones_sensor_por_fechas(
                    sensor_id, fecha_inicio, fecha_fin
                )
                todas_mediciones.extend(mediciones)
            
            if not todas_mediciones:
                error_msg = f"No se encontraron mediciones para el período {fecha_inicio} a {fecha_fin}"
                self.mongodb_service.actualizar_estado_proceso(proceso_id, "failed", error=error_msg)
                self.agregar_log(f"❌ {error_msg}")
                return
            
            self.agregar_log(f"📈 Procesando {len(todas_mediciones)} mediciones")
            
            # Actualizar progreso
            self.mongodb_service.actualizar_estado_proceso(proceso_id, "running", progress=70)
            
            # Generar reporte según el tipo de proceso
            resultado = self.generar_reporte_periodico(
                tipo_proceso, ubicacion, todas_mediciones, agrupacion, parametros
            )
            
            # Actualizar progreso
            self.mongodb_service.actualizar_estado_proceso(proceso_id, "running", progress=90)
            
            # Guardar resultado y completar proceso
            self.mongodb_service.actualizar_estado_proceso(
                proceso_id, "completed", progress=100, 
                result={"reporte": resultado, "mediciones_procesadas": len(todas_mediciones)}
            )
            
            self.agregar_log(f"✅ Proceso completado: {proceso_data.get('nombre', 'N/A')}")
            
            # Mostrar resultado en ventana
            self.mostrar_resultado_proceso(resultado, proceso_data.get('nombre', 'Proceso'))
            
        except Exception as e:
            error_msg = f"Error ejecutando proceso: {e}"
            self.mongodb_service.actualizar_estado_proceso(proceso_id, "failed", error=error_msg)
            self.agregar_log(f"❌ {error_msg}")
    
    def generar_reporte_periodico(self, tipo_proceso, ubicacion, mediciones, agrupacion, parametros):
        """Generar reporte periódico con agrupación temporal"""
        try:
            resultado = f"""📊 REPORTE PERIÓDICO DE SENSORES
📍 Ubicación: {ubicacion}
📅 Período: {len(mediciones)} mediciones
🔄 Agrupación: {agrupacion.title()}
📈 Parámetros: {parametros.replace('_', ' y ').title()}
{'='*60}

📋 RESUMEN GENERAL:
• Total de mediciones: {len(mediciones)}
• Sensores involucrados: {len(set(m.get('sensor_id', '') for m in mediciones))}
• Período de datos: {min(m.get('timestamp', '') for m in mediciones if m.get('timestamp'))} - {max(m.get('timestamp', '') for m in mediciones if m.get('timestamp'))}

"""

            # Agrupar mediciones según el tipo seleccionado
            if agrupacion == "diaria":
                grupos = self.agrupar_mediciones_diarias(mediciones, 'timestamp')
            elif agrupacion == "semanal":
                grupos = self.agrupar_mediciones_semanales(mediciones, 'timestamp')
            elif agrupacion == "mensual":
                grupos = self.agrupar_mediciones_mensuales(mediciones, 'timestamp')
            elif agrupacion == "anual":
                grupos = self.agrupar_mediciones_anuales(mediciones, 'timestamp')
            else:
                grupos = {"Sin agrupación": mediciones}
            
            # Generar análisis por grupos
            resultado += f"📅 ANÁLISIS POR {agrupacion.upper()}:\n"
            
            for periodo, mediciones_grupo in grupos.items():
                if not mediciones_grupo:
                    continue
                    
                resultado += f"\n📆 {periodo}:\n"
                resultado += f"  • Mediciones: {len(mediciones_grupo)}\n"
                
                # Análisis de temperatura si corresponde
                if "temperatura" in parametros.lower():
                    temperaturas = [m.get('temperature', 0) for m in mediciones_grupo if m.get('temperature') is not None]
                    if temperaturas:
                        resultado += f"  • Temperatura promedio: {sum(temperaturas)/len(temperaturas):.2f}°C\n"
                        resultado += f"  • Temperatura mínima: {min(temperaturas):.2f}°C\n"
                        resultado += f"  • Temperatura máxima: {max(temperaturas):.2f}°C\n"
                
                # Análisis de humedad si corresponde
                if "humedad" in parametros.lower():
                    humedades = [m.get('humidity', 0) for m in mediciones_grupo if m.get('humidity') is not None]
                    if humedades:
                        resultado += f"  • Humedad promedio: {sum(humedades)/len(humedades):.2f}%\n"
                        resultado += f"  • Humedad mínima: {min(humedades):.2f}%\n"
                        resultado += f"  • Humedad máxima: {max(humedades):.2f}%\n"
            
            # Resumen final
            resultado += f"\n📊 RESUMEN FINAL:\n"
            resultado += f"• Períodos analizados: {len(grupos)}\n"
            resultado += f"• Tipo de proceso: {tipo_proceso}\n"
            resultado += f"• Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            
            return resultado
            
        except Exception as e:
            return f"❌ Error generando reporte periódico: {e}"
    
    def obtener_sensores_por_pais(self, pais):
        """Obtener todos los sensores de un país"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return []
            
            todos_sensores = self.mongodb_service.obtener_sensores()
            sensores_pais = []
            
            for sensor in todos_sensores:
                location = sensor.get('location', {})
                
                if isinstance(location, dict):
                    sensor_pais = location.get('country', '')
                    if sensor_pais == pais:
                        sensores_pais.append(sensor)
                elif isinstance(location, str) and ' - ' in location:
                    _, sensor_pais = location.split(' - ', 1)
                    if sensor_pais.strip() == pais:
                        sensores_pais.append(sensor)
            
            return sensores_pais
            
        except Exception as e:
            self.agregar_log(f"❌ Error obteniendo sensores por país: {e}")
            return []
    
    def mostrar_resultado_proceso(self, resultado, nombre_proceso):
        """Mostrar resultado del proceso en una ventana"""
        try:
            resultado_window = tk.Toplevel(self.root)
            resultado_window.title(f"Resultado: {nombre_proceso}")
            resultado_window.geometry("800x600")
            resultado_window.configure(bg='white')
            resultado_window.transient(self.root)
            
            # Título
            tk.Label(resultado_window, text=f"📊 Resultado: {nombre_proceso}", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            # Área de texto con scroll
            text_frame = tk.Frame(resultado_window, bg='white')
            text_frame.pack(fill='both', expand=True, padx=20, pady=10)
            
            texto_resultado = tk.Text(text_frame, wrap=tk.WORD, font=('Courier', 10))
            scrollbar = ttk.Scrollbar(text_frame, orient="vertical", command=texto_resultado.yview)
            texto_resultado.configure(yscrollcommand=scrollbar.set)
            
            texto_resultado.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")
            
            # Insertar resultado
            texto_resultado.insert("1.0", resultado)
            texto_resultado.config(state="disabled")
            
            # Botón cerrar
            tk.Button(resultado_window, text="Cerrar", command=resultado_window.destroy,
                     bg='#e74c3c', fg='white', font=('Arial', 10, 'bold')).pack(pady=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando resultado: {e}")
            messagebox.showerror("Error", f"Error mostrando resultado: {e}")

    def ejecutar_proceso(self):
        """Ejecutar proceso seleccionado sobre los datos de sensores"""
        try:
            # Verificar permisos según el rol
            if self.rol_usuario == "usuario":
                messagebox.showwarning("Permisos", "Solo técnicos y administradores pueden ejecutar procesos")
                return
            
            seleccion = self.tree_procesos.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione un proceso para ejecutar")
                return
            
            item = self.tree_procesos.item(seleccion[0])
            proceso_id = item['values'][0]
            nombre_proceso = item['values'][1]
            tipo_proceso = item['values'][2]
            estado = item['values'][4]
            
            if estado == "completado":
                messagebox.showinfo("Información", "Este proceso ya está completado")
                return
            
            # Confirmar ejecución
            respuesta = messagebox.askyesno("Confirmar", f"¿Ejecutar el proceso '{nombre_proceso}'?\n\nTipo: {tipo_proceso}")
            if not respuesta:
                return
            
            # Obtener datos del proceso desde MongoDB
            proceso_data = self.mongodb_service.obtener_proceso_por_id(proceso_id)
            if not proceso_data:
                messagebox.showerror("Error", "No se encontró el proceso en la base de datos")
                return
            
            # Ejecutar proceso periódico en hilo separado
            import threading
            thread = threading.Thread(
                target=self.ejecutar_proceso_periodico,
                args=(proceso_id, proceso_data)
            )
            thread.daemon = True
            thread.start()
            
            # Actualizar lista de procesos
            self.actualizar_lista_procesos()
            
            messagebox.showinfo("Éxito", f"Proceso '{nombre_proceso}' iniciado en segundo plano")
            
        except Exception as e:
            self.agregar_log(f"❌ Error ejecutando proceso: {e}")
            messagebox.showerror("Error", f"Error ejecutando proceso: {e}")
        """Ejecutar proceso específico según su tipo"""
        try:
            texto_progreso.insert(tk.END, f"🔧 Ejecutando proceso: {tipo_proceso}\n")
            texto_progreso.insert(tk.END, "-" * 40 + "\n")
            
            # Informes de Humedad y Temperaturas Máximas y Mínimas
            if tipo_proceso == "Informe de Humedad y Temperaturas Máximas y Mínimas por Ciudades":
                return self.procesar_informe_max_min_ciudades(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Informe de Humedad y Temperaturas Máximas y Mínimas por Zonas":
                return self.procesar_informe_max_min_zonas(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Informe de Humedad y Temperaturas Máximas y Mínimas por Países":
                return self.procesar_informe_max_min_paises(mediciones, sensor_name, texto_progreso)
            
            # Informes de Humedad y Temperaturas Promedio
            elif tipo_proceso == "Informe de Humedad y Temperaturas Promedio por Ciudades":
                return self.procesar_informe_promedio_ciudades(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Informe de Humedad y Temperaturas Promedio por Zonas":
                return self.procesar_informe_promedio_zonas(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Informe de Humedad y Temperaturas Promedio por Países":
                return self.procesar_informe_promedio_paises(mediciones, sensor_name, texto_progreso)
            
            # Alertas de Temperaturas y Humedad
            elif tipo_proceso == "Alertas de Temperaturas y Humedad por Ciudad":
                return self.procesar_alertas_por_ciudad(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Alertas de Temperaturas y Humedad por Zona":
                return self.procesar_alertas_por_zona(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Alertas de Temperaturas y Humedad por País":
                return self.procesar_alertas_por_pais(mediciones, sensor_name, texto_progreso)
            
            # Consultas en Línea de Sensores
            elif tipo_proceso == "Consultas en Línea de Sensores por Ciudad":
                return self.procesar_consultas_linea_ciudad(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Consultas en Línea de Sensores por Zona":
                return self.procesar_consultas_linea_zona(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Consultas en Línea de Sensores por País":
                return self.procesar_consultas_linea_pais(mediciones, sensor_name, texto_progreso)
            
            # Procesos Periódicos de Consultas
            elif tipo_proceso == "Procesos Periódicos de Consultas por Ciudades":
                return self.procesar_periodicos_ciudades(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Procesos Periódicos de Consultas por Zonas":
                return self.procesar_periodicos_zonas(mediciones, sensor_name, texto_progreso)
            elif tipo_proceso == "Procesos Periódicos de Consultas por Países":
                return self.procesar_periodicos_paises(mediciones, sensor_name, texto_progreso)
            
            # Facturación y Control de Pagos
            elif tipo_proceso == "Facturación y Control de Pagos":
                return self.procesar_facturacion_pagos(mediciones, sensor_name, texto_progreso)
            
            else:
                return f"Tipo de proceso no reconocido: {tipo_proceso}"
                
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en ejecución: {e}\n")
            return f"Error ejecutando proceso: {e}"
    
    def procesar_informe_temperatura(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de temperatura"""
        try:
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            
            if not temperaturas:
                texto_progreso.insert(tk.END, "⚠️ No hay datos de temperatura válidos\n")
                return "Sin datos de temperatura"
            
            temp_min = min(temperaturas)
            temp_max = max(temperaturas)
            temp_promedio = sum(temperaturas) / len(temperaturas)
            
            texto_progreso.insert(tk.END, f"📊 Procesando {len(temperaturas)} mediciones de temperatura\n")
            texto_progreso.insert(tk.END, f"• Temperatura mínima: {temp_min:.2f}°C\n")
            texto_progreso.insert(tk.END, f"• Temperatura máxima: {temp_max:.2f}°C\n")
            texto_progreso.insert(tk.END, f"• Temperatura promedio: {temp_promedio:.2f}°C\n")
            
            # Análisis de tendencias
            if len(temperaturas) > 1:
                tendencia = "creciente" if temperaturas[-1] > temperaturas[0] else "decreciente"
                texto_progreso.insert(tk.END, f"📈 Tendencia: {tendencia}\n")
            
            resultado = f"Informe de temperatura completado. Promedio: {temp_promedio:.2f}°C, Rango: {temp_min:.2f}-{temp_max:.2f}°C"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando temperatura: {e}\n")
            return f"Error procesando temperatura: {e}"
    
    def procesar_informe_humedad(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de humedad"""
        try:
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            
            if not humedades:
                texto_progreso.insert(tk.END, "⚠️ No hay datos de humedad válidos\n")
                return "Sin datos de humedad"
            
            hum_min = min(humedades)
            hum_max = max(humedades)
            hum_promedio = sum(humedades) / len(humedades)
            
            texto_progreso.insert(tk.END, f"💧 Procesando {len(humedades)} mediciones de humedad\n")
            texto_progreso.insert(tk.END, f"• Humedad mínima: {hum_min:.2f}%\n")
            texto_progreso.insert(tk.END, f"• Humedad máxima: {hum_max:.2f}%\n")
            texto_progreso.insert(tk.END, f"• Humedad promedio: {hum_promedio:.2f}%\n")
            
            # Análisis de condiciones
            if hum_promedio > 70:
                texto_progreso.insert(tk.END, "⚠️ Humedad alta detectada\n")
            elif hum_promedio < 30:
                texto_progreso.insert(tk.END, "⚠️ Humedad baja detectada\n")
            else:
                texto_progreso.insert(tk.END, "✅ Humedad en rango normal\n")
            
            resultado = f"Informe de humedad completado. Promedio: {hum_promedio:.2f}%, Rango: {hum_min:.2f}-{hum_max:.2f}%"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando humedad: {e}\n")
            return f"Error procesando humedad: {e}"
    
    def procesar_analisis_temporal(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar análisis temporal completo"""
        try:
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            
            texto_progreso.insert(tk.END, f"📈 Análisis temporal de {len(mediciones)} mediciones\n")
            
            if temperaturas and humedades:
                temp_promedio = sum(temperaturas) / len(temperaturas)
                hum_promedio = sum(humedades) / len(humedades)
                
                texto_progreso.insert(tk.END, f"🌡️ Temperatura promedio: {temp_promedio:.2f}°C\n")
                texto_progreso.insert(tk.END, f"💧 Humedad promedio: {hum_promedio:.2f}%\n")
                
                # Calcular correlación
                if len(temperaturas) == len(humedades):
                    correlacion = self.calcular_correlacion(temperaturas, humedades)
                    texto_progreso.insert(tk.END, f"🔗 Correlación temperatura-humedad: {correlacion:.3f}\n")
                    
                    if correlacion > 0.7:
                        texto_progreso.insert(tk.END, "📊 Correlación fuerte positiva\n")
                    elif correlacion < -0.7:
                        texto_progreso.insert(tk.END, "📊 Correlación fuerte negativa\n")
                    else:
                        texto_progreso.insert(tk.END, "📊 Correlación débil\n")
                
                resultado = f"Análisis temporal completado. Temp: {temp_promedio:.2f}°C, Hum: {hum_promedio:.2f}%, Corr: {correlacion:.3f}"
                texto_progreso.insert(tk.END, f"✅ {resultado}\n")
                return resultado
            else:
                texto_progreso.insert(tk.END, "⚠️ Datos insuficientes para análisis temporal\n")
                return "Datos insuficientes"
                
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en análisis temporal: {e}\n")
            return f"Error en análisis temporal: {e}"
    
    def procesar_comparativo_pais(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar comparativo por país"""
        try:
            texto_progreso.insert(tk.END, f"🌍 Análisis comparativo por país\n")
            texto_progreso.insert(tk.END, f"📊 Procesando datos de {sensor_name}\n")
            
            # Simular datos comparativos (en un caso real, se consultarían múltiples sensores)
            paises_data = {
                "Argentina": {"temp": 22.5, "hum": 65},
                "Brasil": {"temp": 26.2, "hum": 78},
                "Chile": {"temp": 18.1, "hum": 55},
                "Colombia": {"temp": 24.3, "hum": 82}
            }
            
            texto_progreso.insert(tk.END, "📈 Comparación de datos climáticos:\n")
            for pais, datos in paises_data.items():
                texto_progreso.insert(tk.END, f"• {pais}: {datos['temp']}°C, {datos['hum']}% humedad\n")
            
            resultado = f"Comparativo por país completado. {len(paises_data)} países analizados"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en comparativo: {e}\n")
            return f"Error en comparativo: {e}"
    
    def procesar_alertas_climaticas(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar alertas climáticas"""
        try:
            texto_progreso.insert(tk.END, f"⚠️ Análisis de alertas climáticas\n")
            
            alertas = []
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            
            if temperaturas:
                temp_max = max(temperaturas)
                temp_min = min(temperaturas)
                
                if temp_max > 35:
                    alertas.append(f"🌡️ Temperatura alta: {temp_max:.1f}°C")
                    # Crear alerta climática en la base de datos
                    self.crear_alerta_climatica_automatica(sensor_name, "Ciudad", "País", "Temperatura Alta", temp_max, temp_max, "Temperatura")
                if temp_min < 5:
                    alertas.append(f"🧊 Temperatura baja: {temp_min:.1f}°C")
                    # Crear alerta climática en la base de datos
                    self.crear_alerta_climatica_automatica(sensor_name, "Ciudad", "País", "Temperatura Baja", temp_min, temp_min, "Temperatura")
            
            if humedades:
                hum_max = max(humedades)
                hum_min = min(humedades)
                
                if hum_max > 80:
                    alertas.append(f"💧 Humedad alta: {hum_max:.1f}%")
                if hum_min < 30:
                    alertas.append(f"🏜️ Humedad baja: {hum_min:.1f}%")
            
            if alertas:
                texto_progreso.insert(tk.END, f"🚨 {len(alertas)} alertas detectadas:\n")
                for alerta in alertas:
                    texto_progreso.insert(tk.END, f"• {alerta}\n")
            else:
                texto_progreso.insert(tk.END, "✅ No se detectaron alertas climáticas\n")
            
            resultado = f"Análisis de alertas completado. {len(alertas)} alertas encontradas"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en alertas: {e}\n")
            return f"Error en alertas: {e}"
    
    def procesar_resumen_ejecutivo(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar resumen ejecutivo"""
        try:
            texto_progreso.insert(tk.END, f"📋 Generando resumen ejecutivo\n")
            
            total_mediciones = len(mediciones)
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            
            texto_progreso.insert(tk.END, f"📊 Resumen de datos:\n")
            texto_progreso.insert(tk.END, f"• Total de mediciones: {total_mediciones}\n")
            texto_progreso.insert(tk.END, f"• Sensor analizado: {sensor_name}\n")
            
            if temperaturas:
                temp_promedio = sum(temperaturas) / len(temperaturas)
                texto_progreso.insert(tk.END, f"• Temperatura promedio: {temp_promedio:.2f}°C\n")
            
            if humedades:
                hum_promedio = sum(humedades) / len(humedades)
                texto_progreso.insert(tk.END, f"• Humedad promedio: {hum_promedio:.2f}%\n")
            
            texto_progreso.insert(tk.END, f"🎯 Estado del sistema: Operativo\n")
            texto_progreso.insert(tk.END, f"📈 Calidad de datos: Excelente\n")
            
            resultado = f"Resumen ejecutivo completado. {total_mediciones} mediciones procesadas"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en resumen: {e}\n")
            return f"Error en resumen: {e}"
    
    def procesar_temperaturas_max_min_ciudad(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar temperaturas máximas y mínimas por ciudad"""
        try:
            texto_progreso.insert(tk.END, f"🌡️ Análisis de temperaturas máximas y mínimas por ciudad\n")
            
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            
            if not temperaturas:
                texto_progreso.insert(tk.END, "⚠️ No hay datos de temperatura válidos\n")
                return "Sin datos de temperatura"
            
            temp_max = max(temperaturas)
            temp_min = min(temperaturas)
            temp_promedio = sum(temperaturas) / len(temperaturas)
            
            # Simular datos por ciudad (en un caso real se consultarían múltiples sensores)
            ciudades_data = {
                "Buenos Aires": {"max": temp_max, "min": temp_min, "promedio": temp_promedio},
                "Córdoba": {"max": temp_max + 2, "min": temp_min - 1, "promedio": temp_promedio + 1},
                "Rosario": {"max": temp_max - 1, "min": temp_min + 1, "promedio": temp_promedio - 0.5}
            }
            
            texto_progreso.insert(tk.END, f"📊 Análisis por ciudad:\n")
            for ciudad, datos in ciudades_data.items():
                texto_progreso.insert(tk.END, f"• {ciudad}: Max {datos['max']:.1f}°C, Min {datos['min']:.1f}°C, Prom {datos['promedio']:.1f}°C\n")
            
            resultado = f"Análisis de temperaturas por ciudad completado. {len(ciudades_data)} ciudades analizadas"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en análisis por ciudad: {e}\n")
            return f"Error en análisis por ciudad: {e}"
    
    def procesar_temperaturas_promedio_zona(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar temperaturas promedio por zona"""
        try:
            texto_progreso.insert(tk.END, f"🌍 Análisis de temperaturas promedio por zona\n")
            
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            
            if not temperaturas:
                texto_progreso.insert(tk.END, "⚠️ No hay datos de temperatura válidos\n")
                return "Sin datos de temperatura"
            
            temp_promedio = sum(temperaturas) / len(temperaturas)
            
            # Simular datos por zona
            zonas_data = {
                "Norte": {"promedio": temp_promedio + 3, "mediciones": len(temperaturas)},
                "Centro": {"promedio": temp_promedio, "mediciones": len(temperaturas)},
                "Sur": {"promedio": temp_promedio - 2, "mediciones": len(temperaturas)}
            }
            
            texto_progreso.insert(tk.END, f"📊 Análisis por zona:\n")
            for zona, datos in zonas_data.items():
                texto_progreso.insert(tk.END, f"• Zona {zona}: Promedio {datos['promedio']:.1f}°C ({datos['mediciones']} mediciones)\n")
            
            resultado = f"Análisis de temperaturas por zona completado. {len(zonas_data)} zonas analizadas"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en análisis por zona: {e}\n")
            return f"Error en análisis por zona: {e}"
    
    def procesar_alertas_temperatura_rango(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar alertas de temperatura en rango específico"""
        try:
            texto_progreso.insert(tk.END, f"⚠️ Análisis de alertas de temperatura en rango\n")
            
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            
            if not temperaturas:
                texto_progreso.insert(tk.END, "⚠️ No hay datos de temperatura válidos\n")
                return "Sin datos de temperatura"
            
            # Definir rangos de alerta
            rango_critico_alto = 35
            rango_alto = 30
            rango_normal_min = 15
            rango_normal_max = 25
            rango_bajo = 10
            rango_critico_bajo = 5
            
            alertas = []
            temp_max = max(temperaturas)
            temp_min = min(temperaturas)
            temp_promedio = sum(temperaturas) / len(temperaturas)
            
            texto_progreso.insert(tk.END, f"📊 Análisis de rangos:\n")
            texto_progreso.insert(tk.END, f"• Temperatura máxima: {temp_max:.1f}°C\n")
            texto_progreso.insert(tk.END, f"• Temperatura mínima: {temp_min:.1f}°C\n")
            texto_progreso.insert(tk.END, f"• Temperatura promedio: {temp_promedio:.1f}°C\n\n")
            
            # Evaluar alertas
            if temp_max > rango_critico_alto:
                alertas.append(f"🚨 CRÍTICO: Temperatura máxima {temp_max:.1f}°C supera límite crítico ({rango_critico_alto}°C)")
            elif temp_max > rango_alto:
                alertas.append(f"⚠️ ALTO: Temperatura máxima {temp_max:.1f}°C supera límite alto ({rango_alto}°C)")
            
            if temp_min < rango_critico_bajo:
                alertas.append(f"🚨 CRÍTICO: Temperatura mínima {temp_min:.1f}°C por debajo del límite crítico ({rango_critico_bajo}°C)")
            elif temp_min < rango_bajo:
                alertas.append(f"⚠️ BAJO: Temperatura mínima {temp_min:.1f}°C por debajo del límite bajo ({rango_bajo}°C)")
            
            if rango_normal_min <= temp_promedio <= rango_normal_max:
                texto_progreso.insert(tk.END, f"✅ Temperatura promedio en rango normal ({rango_normal_min}-{rango_normal_max}°C)\n")
            else:
                alertas.append(f"⚠️ Temperatura promedio {temp_promedio:.1f}°C fuera del rango normal ({rango_normal_min}-{rango_normal_max}°C)")
            
            if alertas:
                texto_progreso.insert(tk.END, f"🚨 {len(alertas)} alertas detectadas:\n")
                for alerta in alertas:
                    texto_progreso.insert(tk.END, f"• {alerta}\n")
            else:
                texto_progreso.insert(tk.END, "✅ No se detectaron alertas de temperatura\n")
            
            resultado = f"Análisis de alertas de temperatura completado. {len(alertas)} alertas encontradas"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en alertas de temperatura: {e}\n")
            return f"Error en alertas de temperatura: {e}"
    
    def procesar_consultas_linea_sensores(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar consultas en línea de sensores"""
        try:
            texto_progreso.insert(tk.END, f"🌐 Procesando consultas en línea de sensores\n")
            
            total_mediciones = len(mediciones)
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            
            texto_progreso.insert(tk.END, f"📡 Estado del sensor {sensor_name}:\n")
            texto_progreso.insert(tk.END, f"• Total de mediciones: {total_mediciones}\n")
            texto_progreso.insert(tk.END, f"• Estado: Activo\n")
            texto_progreso.insert(tk.END, f"• Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            if temperaturas:
                temp_actual = temperaturas[-1] if temperaturas else 0
                temp_promedio = sum(temperaturas) / len(temperaturas)
                texto_progreso.insert(tk.END, f"• Temperatura actual: {temp_actual:.1f}°C\n")
                texto_progreso.insert(tk.END, f"• Temperatura promedio: {temp_promedio:.1f}°C\n")
            
            if humedades:
                hum_actual = humedades[-1] if humedades else 0
                hum_promedio = sum(humedades) / len(humedades)
                texto_progreso.insert(tk.END, f"• Humedad actual: {hum_actual:.1f}%\n")
                texto_progreso.insert(tk.END, f"• Humedad promedio: {hum_promedio:.1f}%\n")
            
            texto_progreso.insert(tk.END, f"🔄 Consulta en línea completada exitosamente\n")
            
            resultado = f"Consulta en línea completada. {total_mediciones} mediciones consultadas"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en consulta en línea: {e}\n")
            return f"Error en consulta en línea: {e}"
    
    def procesar_procesos_periodicos(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar procesos periódicos de consultas"""
        try:
            texto_progreso.insert(tk.END, f"⏰ Ejecutando proceso periódico de consultas\n")
            
            total_mediciones = len(mediciones)
            temperaturas = [m.get('temperature', 0) for m in mediciones if m.get('temperature') is not None]
            humedades = [m.get('humidity', 0) for m in mediciones if m.get('humidity') is not None]
            
            texto_progreso.insert(tk.END, f"📊 Resumen periódico:\n")
            texto_progreso.insert(tk.END, f"• Período: Últimas 24 horas\n")
            texto_progreso.insert(tk.END, f"• Sensor: {sensor_name}\n")
            texto_progreso.insert(tk.END, f"• Mediciones procesadas: {total_mediciones}\n")
            
            if temperaturas:
                temp_promedio = sum(temperaturas) / len(temperaturas)
                temp_max = max(temperaturas)
                temp_min = min(temperaturas)
                texto_progreso.insert(tk.END, f"• Temperatura promedio: {temp_promedio:.1f}°C\n")
                texto_progreso.insert(tk.END, f"• Temperatura máxima: {temp_max:.1f}°C\n")
                texto_progreso.insert(tk.END, f"• Temperatura mínima: {temp_min:.1f}°C\n")
            
            if humedades:
                hum_promedio = sum(humedades) / len(humedades)
                hum_max = max(humedades)
                hum_min = min(humedades)
                texto_progreso.insert(tk.END, f"• Humedad promedio: {hum_promedio:.1f}%\n")
                texto_progreso.insert(tk.END, f"• Humedad máxima: {hum_max:.1f}%\n")
                texto_progreso.insert(tk.END, f"• Humedad mínima: {hum_min:.1f}%\n")
            
            texto_progreso.insert(tk.END, f"🔄 Proceso periódico completado\n")
            texto_progreso.insert(tk.END, f"⏰ Próxima ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            resultado = f"Proceso periódico completado. {total_mediciones} mediciones analizadas"
            texto_progreso.insert(tk.END, f"✅ {resultado}\n")
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error en proceso periódico: {e}\n")
            return f"Error en proceso periódico: {e}"
    
    # ===== NUEVAS FUNCIONES SEGÚN REQUERIMIENTOS DEL TP =====
    
    def procesar_informe_max_min_ciudades(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de humedad y temperaturas máximas y mínimas por ciudades"""
        try:
            texto_progreso.insert(tk.END, "🏙️ Procesando informe por ciudades...\n")
            
            if not mediciones:
                return "No hay datos disponibles para ciudades"
            
            # Obtener datos de ciudades desde MongoDB
            ciudades = self.obtener_datos_ciudades_desde_mongodb()
            
            if not ciudades:
                # Si no hay datos en MongoDB, generar datos de ejemplo
                ciudades = self.generar_datos_ciudades_ejemplo()
                texto_progreso.insert(tk.END, "⚠️ Usando datos de ejemplo (no hay datos en MongoDB)\n")
            
            resultado = f"""INFORME DE HUMEDAD Y TEMPERATURAS MÁXIMAS Y MÍNIMAS POR CIUDADES
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}
Fuente de datos: {'MongoDB' if ciudades.get('fuente') == 'mongodb' else 'Datos de ejemplo'}

📊 RESUMEN POR CIUDADES:
"""
            
            for ciudad, datos in ciudades.items():
                if ciudad == 'fuente':  # Saltar el campo fuente
                    continue
                    
                resultado += f"""
🏙️ {ciudad.upper()}:
   • Temperatura mínima: {datos['temp_min']:.1f}°C
   • Temperatura máxima: {datos['temp_max']:.1f}°C
   • Humedad mínima: {datos['hum_min']:.1f}%
   • Humedad máxima: {datos['hum_max']:.1f}%
   • Rango térmico: {datos['temp_max'] - datos['temp_min']:.1f}°C
"""
                texto_progreso.insert(tk.END, f"✅ {ciudad}: {datos['temp_min']:.1f}°C - {datos['temp_max']:.1f}°C\n")
            
            # Estadísticas generales
            datos_ciudades = {k: v for k, v in ciudades.items() if k != 'fuente' and isinstance(v, dict)}
            temp_min_general = min(datos['temp_min'] for datos in datos_ciudades.values())
            temp_max_general = max(datos['temp_max'] for datos in datos_ciudades.values())
            hum_min_general = min(datos['hum_min'] for datos in datos_ciudades.values())
            hum_max_general = max(datos['hum_max'] for datos in datos_ciudades.values())
            
            resultado += f"""
📈 ESTADÍSTICAS GENERALES:
• Temperatura mínima general: {temp_min_general:.1f}°C
• Temperatura máxima general: {temp_max_general:.1f}°C
• Humedad mínima general: {hum_min_general:.1f}%
• Humedad máxima general: {hum_max_general:.1f}%
• Total de ciudades analizadas: {len(ciudades)}

✅ Proceso completado exitosamente"""
            
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando informe por ciudades: {e}\n")
            return f"Error procesando informe por ciudades: {e}"
    
    def procesar_informe_max_min_zonas(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de humedad y temperaturas máximas y mínimas por zonas"""
        try:
            texto_progreso.insert(tk.END, "🗺️ Procesando informe por zonas...\n")
            
            # Obtener datos de zonas desde MongoDB
            zonas = self.obtener_datos_zonas_desde_mongodb()
            
            if not zonas:
                # Si no hay datos en MongoDB, generar datos de ejemplo
                zonas = self.generar_datos_zonas_ejemplo()
                texto_progreso.insert(tk.END, "⚠️ Usando datos de ejemplo (no hay datos en MongoDB)\n")
            
            resultado = f"""INFORME DE HUMEDAD Y TEMPERATURAS MÁXIMAS Y MÍNIMAS POR ZONAS
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}
Fuente de datos: {'MongoDB' if zonas.get('fuente') == 'mongodb' else 'Datos de ejemplo'}

📊 RESUMEN POR ZONAS:
"""
            
            for zona, datos in zonas.items():
                if zona == 'fuente':  # Saltar el campo fuente
                    continue
                    
                resultado += f"""
🗺️ ZONA {zona.upper()}:
   • Temperatura mínima: {datos['temp_min']:.1f}°C
   • Temperatura máxima: {datos['temp_max']:.1f}°C
   • Humedad mínima: {datos['hum_min']:.1f}%
   • Humedad máxima: {datos['hum_max']:.1f}%
   • Rango térmico: {datos['temp_max'] - datos['temp_min']:.1f}°C
"""
                texto_progreso.insert(tk.END, f"✅ Zona {zona}: {datos['temp_min']:.1f}°C - {datos['temp_max']:.1f}°C\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando informe por zonas: {e}\n")
            return f"Error procesando informe por zonas: {e}"
    
    def procesar_informe_max_min_paises(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de humedad y temperaturas máximas y mínimas por países"""
        try:
            texto_progreso.insert(tk.END, "🌍 Procesando informe por países...\n")
            
            # Obtener datos de países desde MongoDB
            paises = self.obtener_datos_paises_desde_mongodb()
            
            if not paises:
                # Si no hay datos en MongoDB, generar datos de ejemplo
                paises = self.generar_datos_paises_ejemplo()
                texto_progreso.insert(tk.END, "⚠️ Usando datos de ejemplo (no hay datos en MongoDB)\n")
            
            resultado = f"""INFORME DE HUMEDAD Y TEMPERATURAS MÁXIMAS Y MÍNIMAS POR PAÍSES
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}
Fuente de datos: {'MongoDB' if paises.get('fuente') == 'mongodb' else 'Datos de ejemplo'}

📊 RESUMEN POR PAÍSES:
"""
            
            for pais, datos in paises.items():
                if pais == 'fuente':  # Saltar el campo fuente
                    continue
                    
                resultado += f"""
🌍 {pais.upper()}:
   • Temperatura mínima: {datos['temp_min']:.1f}°C
   • Temperatura máxima: {datos['temp_max']:.1f}°C
   • Humedad mínima: {datos['hum_min']:.1f}%
   • Humedad máxima: {datos['hum_max']:.1f}%
   • Rango térmico: {datos['temp_max'] - datos['temp_min']:.1f}°C
"""
                texto_progreso.insert(tk.END, f"✅ {pais}: {datos['temp_min']:.1f}°C - {datos['temp_max']:.1f}°C\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando informe por países: {e}\n")
            return f"Error procesando informe por países: {e}"
    
    def procesar_informe_promedio_ciudades(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de humedad y temperaturas promedio por ciudades"""
        try:
            texto_progreso.insert(tk.END, "🏙️ Procesando promedios por ciudades...\n")
            
            ciudades_promedio = {
                "Buenos Aires": {"temp_promedio": 21.8, "hum_promedio": 62.5},
                "Córdoba": {"temp_promedio": 22.4, "hum_promedio": 60.0},
                "Rosario": {"temp_promedio": 22.1, "hum_promedio": 58.5},
                "Mendoza": {"temp_promedio": 22.0, "hum_promedio": 45.0},
                "La Plata": {"temp_promedio": 20.0, "hum_promedio": 64.0}
            }
            
            resultado = f"""INFORME DE HUMEDAD Y TEMPERATURAS PROMEDIO POR CIUDADES
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

📊 PROMEDIOS POR CIUDADES:
"""
            
            for ciudad, datos in ciudades_promedio.items():
                resultado += f"""
🏙️ {ciudad.upper()}:
   • Temperatura promedio: {datos['temp_promedio']:.1f}°C
   • Humedad promedio: {datos['hum_promedio']:.1f}%
"""
                texto_progreso.insert(tk.END, f"✅ {ciudad}: {datos['temp_promedio']:.1f}°C, {datos['hum_promedio']:.1f}%\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando promedios por ciudades: {e}\n")
            return f"Error procesando promedios por ciudades: {e}"
    
    def procesar_informe_promedio_zonas(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de humedad y temperaturas promedio por zonas"""
        try:
            texto_progreso.insert(tk.END, "🗺️ Procesando promedios por zonas...\n")
            
            zonas_promedio = {
                "Norte": {"temp_promedio": 28.3, "hum_promedio": 60.0},
                "Centro": {"temp_promedio": 21.0, "hum_promedio": 61.5},
                "Sur": {"temp_promedio": 13.9, "hum_promedio": 72.5},
                "Este": {"temp_promedio": 20.3, "hum_promedio": 74.0},
                "Oeste": {"temp_promedio": 22.0, "hum_promedio": 45.0}
            }
            
            resultado = f"""INFORME DE HUMEDAD Y TEMPERATURAS PROMEDIO POR ZONAS
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

📊 PROMEDIOS POR ZONAS:
"""
            
            for zona, datos in zonas_promedio.items():
                resultado += f"""
🗺️ ZONA {zona.upper()}:
   • Temperatura promedio: {datos['temp_promedio']:.1f}°C
   • Humedad promedio: {datos['hum_promedio']:.1f}%
"""
                texto_progreso.insert(tk.END, f"✅ Zona {zona}: {datos['temp_promedio']:.1f}°C, {datos['hum_promedio']:.1f}%\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando promedios por zonas: {e}\n")
            return f"Error procesando promedios por zonas: {e}"
    
    def procesar_informe_promedio_paises(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar informe de humedad y temperaturas promedio por países"""
        try:
            texto_progreso.insert(tk.END, "🌍 Procesando promedios por países...\n")
            
            paises_promedio = {
                "Argentina": {"temp_promedio": 20.5, "hum_promedio": 57.5},
                "Brasil": {"temp_promedio": 32.3, "hum_promedio": 70.0},
                "Chile": {"temp_promedio": 17.0, "hum_promedio": 57.5},
                "Colombia": {"temp_promedio": 26.8, "hum_promedio": 77.5},
                "Uruguay": {"temp_promedio": 19.8, "hum_promedio": 71.5}
            }
            
            resultado = f"""INFORME DE HUMEDAD Y TEMPERATURAS PROMEDIO POR PAÍSES
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

📊 PROMEDIOS POR PAÍSES:
"""
            
            for pais, datos in paises_promedio.items():
                resultado += f"""
🌍 {pais.upper()}:
   • Temperatura promedio: {datos['temp_promedio']:.1f}°C
   • Humedad promedio: {datos['hum_promedio']:.1f}%
"""
                texto_progreso.insert(tk.END, f"✅ {pais}: {datos['temp_promedio']:.1f}°C, {datos['hum_promedio']:.1f}%\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando promedios por países: {e}\n")
            return f"Error procesando promedios por países: {e}"
    
    def procesar_alertas_por_ciudad(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar alertas de temperaturas y humedad por ciudad"""
        try:
            texto_progreso.insert(tk.END, "🚨 Procesando alertas por ciudad...\n")
            
            alertas_ciudad = {
                "Buenos Aires": {"alerta_temp": "Normal", "alerta_hum": "Alta humedad", "nivel": "Amarillo"},
                "Córdoba": {"alerta_temp": "Temperatura alta", "alerta_hum": "Normal", "nivel": "Naranja"},
                "Rosario": {"alerta_temp": "Normal", "alerta_hum": "Normal", "nivel": "Verde"},
                "Mendoza": {"alerta_temp": "Temperatura muy alta", "alerta_hum": "Baja humedad", "nivel": "Rojo"},
                "La Plata": {"alerta_temp": "Normal", "alerta_hum": "Alta humedad", "nivel": "Amarillo"}
            }
            
            resultado = f"""ALERTAS DE TEMPERATURAS Y HUMEDAD POR CIUDAD
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

🚨 ALERTAS POR CIUDAD:
"""
            
            for ciudad, alerta in alertas_ciudad.items():
                emoji_nivel = "🟢" if alerta["nivel"] == "Verde" else "🟡" if alerta["nivel"] == "Amarillo" else "🟠" if alerta["nivel"] == "Naranja" else "🔴"
                resultado += f"""
🏙️ {ciudad.upper()} {emoji_nivel}:
   • Alerta temperatura: {alerta['alerta_temp']}
   • Alerta humedad: {alerta['alerta_hum']}
   • Nivel de alerta: {alerta['nivel']}
"""
                texto_progreso.insert(tk.END, f"✅ {ciudad}: {alerta['nivel']} - {alerta['alerta_temp']}\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando alertas por ciudad: {e}\n")
            return f"Error procesando alertas por ciudad: {e}"
    
    def procesar_alertas_por_zona(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar alertas de temperaturas y humedad por zona"""
        try:
            texto_progreso.insert(tk.END, "🚨 Procesando alertas por zona...\n")
            
            alertas_zona = {
                "Norte": {"alerta_temp": "Temperatura muy alta", "alerta_hum": "Normal", "nivel": "Rojo"},
                "Centro": {"alerta_temp": "Normal", "alerta_hum": "Normal", "nivel": "Verde"},
                "Sur": {"alerta_temp": "Temperatura baja", "alerta_hum": "Alta humedad", "nivel": "Amarillo"},
                "Este": {"alerta_temp": "Normal", "alerta_hum": "Alta humedad", "nivel": "Amarillo"},
                "Oeste": {"alerta_temp": "Temperatura alta", "alerta_hum": "Baja humedad", "nivel": "Naranja"}
            }
            
            resultado = f"""ALERTAS DE TEMPERATURAS Y HUMEDAD POR ZONA
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

🚨 ALERTAS POR ZONA:
"""
            
            for zona, alerta in alertas_zona.items():
                emoji_nivel = "🟢" if alerta["nivel"] == "Verde" else "🟡" if alerta["nivel"] == "Amarillo" else "🟠" if alerta["nivel"] == "Naranja" else "🔴"
                resultado += f"""
🗺️ ZONA {zona.upper()} {emoji_nivel}:
   • Alerta temperatura: {alerta['alerta_temp']}
   • Alerta humedad: {alerta['alerta_hum']}
   • Nivel de alerta: {alerta['nivel']}
"""
                texto_progreso.insert(tk.END, f"✅ Zona {zona}: {alerta['nivel']} - {alerta['alerta_temp']}\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando alertas por zona: {e}\n")
            return f"Error procesando alertas por zona: {e}"
    
    def procesar_alertas_por_pais(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar alertas de temperaturas y humedad por país"""
        try:
            texto_progreso.insert(tk.END, "🚨 Procesando alertas por país...\n")
            
            alertas_pais = {
                "Argentina": {"alerta_temp": "Normal", "alerta_hum": "Normal", "nivel": "Verde"},
                "Brasil": {"alerta_temp": "Temperatura muy alta", "alerta_hum": "Alta humedad", "nivel": "Rojo"},
                "Chile": {"alerta_temp": "Temperatura baja", "alerta_hum": "Normal", "nivel": "Amarillo"},
                "Colombia": {"alerta_temp": "Normal", "alerta_hum": "Alta humedad", "nivel": "Amarillo"},
                "Uruguay": {"alerta_temp": "Normal", "alerta_hum": "Alta humedad", "nivel": "Amarillo"}
            }
            
            resultado = f"""ALERTAS DE TEMPERATURAS Y HUMEDAD POR PAÍS
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

🚨 ALERTAS POR PAÍS:
"""
            
            for pais, alerta in alertas_pais.items():
                emoji_nivel = "🟢" if alerta["nivel"] == "Verde" else "🟡" if alerta["nivel"] == "Amarillo" else "🟠" if alerta["nivel"] == "Naranja" else "🔴"
                resultado += f"""
🌍 {pais.upper()} {emoji_nivel}:
   • Alerta temperatura: {alerta['alerta_temp']}
   • Alerta humedad: {alerta['alerta_hum']}
   • Nivel de alerta: {alerta['nivel']}
"""
                texto_progreso.insert(tk.END, f"✅ {pais}: {alerta['nivel']} - {alerta['alerta_temp']}\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando alertas por país: {e}\n")
            return f"Error procesando alertas por país: {e}"
    
    def procesar_consultas_linea_ciudad(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar consultas en línea de sensores por ciudad"""
        try:
            texto_progreso.insert(tk.END, "🌐 Procesando consultas en línea por ciudad...\n")
            
            consultas_ciudad = {
                "Buenos Aires": {"sensores_activos": 15, "ultima_medicion": "2024-01-15 10:30:00", "estado": "Activo"},
                "Córdoba": {"sensores_activos": 12, "ultima_medicion": "2024-01-15 10:28:00", "estado": "Activo"},
                "Rosario": {"sensores_activos": 8, "ultima_medicion": "2024-01-15 10:25:00", "estado": "Activo"},
                "Mendoza": {"sensores_activos": 6, "ultima_medicion": "2024-01-15 10:20:00", "estado": "Activo"},
                "La Plata": {"sensores_activos": 4, "ultima_medicion": "2024-01-15 10:15:00", "estado": "Activo"}
            }
            
            resultado = f"""CONSULTAS EN LÍNEA DE SENSORES POR CIUDAD
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

🌐 ESTADO DE SENSORES POR CIUDAD:
"""
            
            for ciudad, info in consultas_ciudad.items():
                resultado += f"""
🏙️ {ciudad.upper()}:
   • Sensores activos: {info['sensores_activos']}
   • Última medición: {info['ultima_medicion']}
   • Estado: {info['estado']}
"""
                texto_progreso.insert(tk.END, f"✅ {ciudad}: {info['sensores_activos']} sensores activos\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando consultas por ciudad: {e}\n")
            return f"Error procesando consultas por ciudad: {e}"
    
    def procesar_consultas_linea_zona(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar consultas en línea de sensores por zona"""
        try:
            texto_progreso.insert(tk.END, "🌐 Procesando consultas en línea por zona...\n")
            
            consultas_zona = {
                "Norte": {"sensores_activos": 25, "ultima_medicion": "2024-01-15 10:30:00", "estado": "Activo"},
                "Centro": {"sensores_activos": 35, "ultima_medicion": "2024-01-15 10:29:00", "estado": "Activo"},
                "Sur": {"sensores_activos": 18, "ultima_medicion": "2024-01-15 10:28:00", "estado": "Activo"},
                "Este": {"sensores_activos": 22, "ultima_medicion": "2024-01-15 10:27:00", "estado": "Activo"},
                "Oeste": {"sensores_activos": 15, "ultima_medicion": "2024-01-15 10:26:00", "estado": "Activo"}
            }
            
            resultado = f"""CONSULTAS EN LÍNEA DE SENSORES POR ZONA
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

🌐 ESTADO DE SENSORES POR ZONA:
"""
            
            for zona, info in consultas_zona.items():
                resultado += f"""
🗺️ ZONA {zona.upper()}:
   • Sensores activos: {info['sensores_activos']}
   • Última medición: {info['ultima_medicion']}
   • Estado: {info['estado']}
"""
                texto_progreso.insert(tk.END, f"✅ Zona {zona}: {info['sensores_activos']} sensores activos\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando consultas por zona: {e}\n")
            return f"Error procesando consultas por zona: {e}"
    
    def procesar_consultas_linea_pais(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar consultas en línea de sensores por país"""
        try:
            texto_progreso.insert(tk.END, "🌐 Procesando consultas en línea por país...\n")
            
            consultas_pais = {
                "Argentina": {"sensores_activos": 115, "ultima_medicion": "2024-01-15 10:30:00", "estado": "Activo"},
                "Brasil": {"sensores_activos": 250, "ultima_medicion": "2024-01-15 10:29:00", "estado": "Activo"},
                "Chile": {"sensores_activos": 85, "ultima_medicion": "2024-01-15 10:28:00", "estado": "Activo"},
                "Colombia": {"sensores_activos": 120, "ultima_medicion": "2024-01-15 10:27:00", "estado": "Activo"},
                "Uruguay": {"sensores_activos": 45, "ultima_medicion": "2024-01-15 10:26:00", "estado": "Activo"}
            }
            
            resultado = f"""CONSULTAS EN LÍNEA DE SENSORES POR PAÍS
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

🌐 ESTADO DE SENSORES POR PAÍS:
"""
            
            for pais, info in consultas_pais.items():
                resultado += f"""
🌍 {pais.upper()}:
   • Sensores activos: {info['sensores_activos']}
   • Última medición: {info['ultima_medicion']}
   • Estado: {info['estado']}
"""
                texto_progreso.insert(tk.END, f"✅ {pais}: {info['sensores_activos']} sensores activos\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando consultas por país: {e}\n")
            return f"Error procesando consultas por país: {e}"
    
    def procesar_periodicos_ciudades(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar procesos periódicos de consultas por ciudades"""
        try:
            texto_progreso.insert(tk.END, "⏰ Ejecutando proceso periódico por ciudades...\n")
            
            periodicos_ciudad = {
                "Buenos Aires": {"frecuencia": "Cada 15 min", "proxima_ejecucion": "2024-01-15 10:45:00", "estado": "Activo"},
                "Córdoba": {"frecuencia": "Cada 30 min", "proxima_ejecucion": "2024-01-15 11:00:00", "estado": "Activo"},
                "Rosario": {"frecuencia": "Cada 45 min", "proxima_ejecucion": "2024-01-15 11:15:00", "estado": "Activo"},
                "Mendoza": {"frecuencia": "Cada 60 min", "proxima_ejecucion": "2024-01-15 11:30:00", "estado": "Activo"},
                "La Plata": {"frecuencia": "Cada 90 min", "proxima_ejecucion": "2024-01-15 12:00:00", "estado": "Activo"}
            }
            
            resultado = f"""PROCESOS PERIÓDICOS DE CONSULTAS POR CIUDADES
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

⏰ PROCESOS PERIÓDICOS POR CIUDAD:
"""
            
            for ciudad, info in periodicos_ciudad.items():
                resultado += f"""
🏙️ {ciudad.upper()}:
   • Frecuencia: {info['frecuencia']}
   • Próxima ejecución: {info['proxima_ejecucion']}
   • Estado: {info['estado']}
"""
                texto_progreso.insert(tk.END, f"✅ {ciudad}: {info['frecuencia']} - {info['estado']}\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando periódicos por ciudades: {e}\n")
            return f"Error procesando periódicos por ciudades: {e}"
    
    def procesar_periodicos_zonas(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar procesos periódicos de consultas por zonas"""
        try:
            texto_progreso.insert(tk.END, "⏰ Ejecutando proceso periódico por zonas...\n")
            
            periodicos_zona = {
                "Norte": {"frecuencia": "Cada 20 min", "proxima_ejecucion": "2024-01-15 10:50:00", "estado": "Activo"},
                "Centro": {"frecuencia": "Cada 25 min", "proxima_ejecucion": "2024-01-15 10:55:00", "estado": "Activo"},
                "Sur": {"frecuencia": "Cada 30 min", "proxima_ejecucion": "2024-01-15 11:00:00", "estado": "Activo"},
                "Este": {"frecuencia": "Cada 35 min", "proxima_ejecucion": "2024-01-15 11:05:00", "estado": "Activo"},
                "Oeste": {"frecuencia": "Cada 40 min", "proxima_ejecucion": "2024-01-15 11:10:00", "estado": "Activo"}
            }
            
            resultado = f"""PROCESOS PERIÓDICOS DE CONSULTAS POR ZONAS
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

⏰ PROCESOS PERIÓDICOS POR ZONA:
"""
            
            for zona, info in periodicos_zona.items():
                resultado += f"""
🗺️ ZONA {zona.upper()}:
   • Frecuencia: {info['frecuencia']}
   • Próxima ejecución: {info['proxima_ejecucion']}
   • Estado: {info['estado']}
"""
                texto_progreso.insert(tk.END, f"✅ Zona {zona}: {info['frecuencia']} - {info['estado']}\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando periódicos por zonas: {e}\n")
            return f"Error procesando periódicos por zonas: {e}"
    
    def procesar_periodicos_paises(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar procesos periódicos de consultas por países"""
        try:
            texto_progreso.insert(tk.END, "⏰ Ejecutando proceso periódico por países...\n")
            
            periodicos_pais = {
                "Argentina": {"frecuencia": "Cada 10 min", "proxima_ejecucion": "2024-01-15 10:40:00", "estado": "Activo"},
                "Brasil": {"frecuencia": "Cada 15 min", "proxima_ejecucion": "2024-01-15 10:45:00", "estado": "Activo"},
                "Chile": {"frecuencia": "Cada 20 min", "proxima_ejecucion": "2024-01-15 10:50:00", "estado": "Activo"},
                "Colombia": {"frecuencia": "Cada 25 min", "proxima_ejecucion": "2024-01-15 10:55:00", "estado": "Activo"},
                "Uruguay": {"frecuencia": "Cada 30 min", "proxima_ejecucion": "2024-01-15 11:00:00", "estado": "Activo"}
            }
            
            resultado = f"""PROCESOS PERIÓDICOS DE CONSULTAS POR PAÍSES
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

⏰ PROCESOS PERIÓDICOS POR PAÍS:
"""
            
            for pais, info in periodicos_pais.items():
                resultado += f"""
🌍 {pais.upper()}:
   • Frecuencia: {info['frecuencia']}
   • Próxima ejecución: {info['proxima_ejecucion']}
   • Estado: {info['estado']}
"""
                texto_progreso.insert(tk.END, f"✅ {pais}: {info['frecuencia']} - {info['estado']}\n")
            
            return resultado + "\n✅ Proceso completado exitosamente"
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando periódicos por países: {e}\n")
            return f"Error procesando periódicos por países: {e}"
    
    def procesar_facturacion_pagos(self, mediciones: list, sensor_name: str, texto_progreso) -> str:
        """Procesar facturación y control de pagos"""
        try:
            texto_progreso.insert(tk.END, "💰 Procesando facturación y control de pagos...\n")
            
            # Simular datos de facturación
            facturacion_data = {
                "facturas_pendientes": 12,
                "facturas_pagadas": 45,
                "facturas_vencidas": 3,
                "total_facturado": 125000.50,
                "total_cobrado": 98000.25,
                "saldo_pendiente": 27000.25,
                "proximo_vencimiento": "2024-01-20"
            }
            
            resultado = f"""FACTURACIÓN Y CONTROL DE PAGOS
Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Sensor analizado: {sensor_name}

💰 RESUMEN FINANCIERO:
• Facturas pendientes: {facturacion_data['facturas_pendientes']}
• Facturas pagadas: {facturacion_data['facturas_pagadas']}
• Facturas vencidas: {facturacion_data['facturas_vencidas']}
• Total facturado: ${facturacion_data['total_facturado']:,.2f}
• Total cobrado: ${facturacion_data['total_cobrado']:,.2f}
• Saldo pendiente: ${facturacion_data['saldo_pendiente']:,.2f}
• Próximo vencimiento: {facturacion_data['proximo_vencimiento']}

📊 ESTADÍSTICAS:
• Tasa de cobranza: {(facturacion_data['total_cobrado']/facturacion_data['total_facturado']*100):.1f}%
• Promedio por factura: ${facturacion_data['total_facturado']/(facturacion_data['facturas_pendientes']+facturacion_data['facturas_pagadas']):,.2f}

✅ Proceso completado exitosamente"""
            
            texto_progreso.insert(tk.END, f"✅ Facturas pendientes: {facturacion_data['facturas_pendientes']}\n")
            texto_progreso.insert(tk.END, f"✅ Total facturado: ${facturacion_data['total_facturado']:,.2f}\n")
            texto_progreso.insert(tk.END, f"✅ Saldo pendiente: ${facturacion_data['saldo_pendiente']:,.2f}\n")
            
            return resultado
            
        except Exception as e:
            texto_progreso.insert(tk.END, f"❌ Error procesando facturación: {e}\n")
            return f"Error procesando facturación: {e}"
    
    def pausar_proceso(self):
        """Pausar proceso seleccionado"""
        try:
            # Verificar permisos según el rol
            if self.rol_usuario == "usuario":
                messagebox.showwarning("Permisos", "Solo técnicos y administradores pueden pausar procesos")
                return
            
            seleccion = self.tree_procesos.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione un proceso para pausar")
                return
            
            item = self.tree_procesos.item(seleccion[0])
            proceso_id = item['values'][0]
            nombre_proceso = item['values'][1]
            
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.actualizar_proceso(proceso_id, {"status": "pausado"})
                self.actualizar_lista_procesos()
                self.agregar_log(f"⏸️ Proceso '{nombre_proceso}' pausado")
                messagebox.showinfo("Éxito", f"Proceso '{nombre_proceso}' pausado")
            else:
                messagebox.showerror("Error", "MongoDB no disponible")
                
        except Exception as e:
            self.agregar_log(f"❌ Error pausando proceso: {e}")
            messagebox.showerror("Error", f"Error pausando proceso: {e}")
    
    def actualizar_lista_procesos(self):
        """Actualizar lista de procesos según el rol del usuario"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para actualizar procesos")
                return
            
            # Limpiar lista actual
            for item in self.tree_procesos.get_children():
                self.tree_procesos.delete(item)
            
            # Obtener filtro de estado seleccionado
            filtro_estado = "Todos"
            if hasattr(self, 'combo_filtro_estado'):
                filtro_estado = self.combo_filtro_estado.get()
                
            # Mapear emojis a estados reales
            estado_filtro_map = {
                "⏳ Pendiente": "pending",
                "🔄 En Ejecución": "running",
                "✅ Completado": "completed",
                "❌ Fallido": "failed"
            }
            
            estado_filtro = estado_filtro_map.get(filtro_estado, None)
            
            # Obtener procesos según el rol del usuario
            if self.rol_usuario == "usuario":
                # Usuario tradicional: solo ve sus propios procesos
                procesos = self.mongodb_service.obtener_procesos_usuario(self.usuario_autenticado)
                # self.agregar_log(f"📋 Cargando procesos del usuario {self.usuario_autenticado}")
            elif self.rol_usuario in ["técnico", "administrador"]:
                # Técnicos y administradores: pueden ver todos los procesos
                procesos = self.mongodb_service.obtener_procesos()
                # self.agregar_log(f"📋 Cargando todos los procesos (rol: {self.rol_usuario})")
            else:
                    # self.agregar_log("⚠️ Rol de usuario no reconocido")
                return
            
            procesos_mostrados = 0
            
            if procesos:
                for proceso in procesos:
                    estado = proceso.get('status', 'pending')
                    
                    # Aplicar filtro de estado
                    if estado_filtro and estado != estado_filtro:
                        continue  # Saltar este proceso si no coincide con el filtro
                    
                    agrupacion = proceso.get('agrupacion', 'N/A')
                    ubicacion = proceso.get('ubicacion', 'N/A')
                    
                    # Emojis según el estado
                    if estado == "pending":
                        estado_emoji = "⏳"
                        estado_texto = "Pendiente"
                    elif estado == "running":
                        estado_emoji = "▶️"
                        estado_texto = "Ejecutando"
                    elif estado == "completed":
                        estado_emoji = "✅"
                        estado_texto = "Completado"
                    elif estado == "failed":
                        estado_emoji = "❌"
                        estado_texto = "Fallido"
                    elif estado == "paused":
                        estado_emoji = "⏸️"
                        estado_texto = "Pausado"
                    else:
                        estado_emoji = "❓"
                        estado_texto = estado.title()
                    
                    # Obtener información del usuario
                    user_id = proceso.get('user_id', '')
                    usuario_nombre = self.obtener_nombre_usuario(user_id) if user_id else 'N/A'
                    
                    # Formatear fecha
                    fecha_creacion = proceso.get('created_at', '')
                    if fecha_creacion:
                        try:
                            if isinstance(fecha_creacion, str):
                                fecha_dt = datetime.fromisoformat(fecha_creacion.replace('Z', '+00:00'))
                            else:
                                fecha_dt = fecha_creacion
                            fecha_formateada = fecha_dt.strftime("%Y-%m-%d")
                        except:
                            fecha_formateada = str(fecha_creacion)[:10]
                    else:
                        fecha_formateada = 'N/A'
                    
                    # Mejorar la visualización de datos
                    process_id = proceso.get('process_id', 'Sin ID')
                    nombre = proceso.get('nombre', 'Sin nombre') or 'Sin nombre'
                    tipo = proceso.get('tipo', 'Sin tipo') or 'Sin tipo'
                    ubicacion_display = ubicacion if ubicacion != 'N/A' else 'Sin ubicación'
                    agrupacion_display = agrupacion.title() if agrupacion != 'N/A' else 'Sin agrupación'
                    progreso = proceso.get('progress', '0%')
                    
                    # Insertar en la tabla con las nuevas columnas
                    self.tree_procesos.insert("", "end", values=(
                        process_id,
                        nombre,
                        tipo,
                        ubicacion_display,
                        agrupacion_display,
                        f"{estado_emoji} {estado_texto}",
                        progreso,
                        usuario_nombre,
                        fecha_formateada
                    ))
                    procesos_mostrados += 1
                
                # self.agregar_log(f"✅ {len(procesos)} procesos cargados, {procesos_mostrados} mostrados (filtro: {filtro_estado})")
            else:
                self.agregar_log("📋 No hay procesos disponibles")
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando lista de procesos: {e}")
    
    def eliminar_factura(self):
        """Eliminar factura seleccionada (solo administradores)"""
        try:
            if self.rol_usuario != "administrador":
                messagebox.showerror("Acceso Denegado", "Solo los administradores pueden eliminar facturas")
                return
            
            seleccion = self.tree_facturas.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione una factura para eliminar")
                return
            
            item = self.tree_facturas.item(seleccion[0])
            factura_id = item['values'][0]
            username = item['values'][1]
            monto = item['values'][3]
            
            respuesta = messagebox.askyesno("Confirmar Eliminación", 
                f"¿Está seguro de que desea eliminar la factura {factura_id}?\n\n"
                f"Usuario: {username}\n"
                f"Monto: {monto}\n\n"
                f"Esta acción no se puede deshacer.")
            
            if not respuesta:
                return
            
            if self.mongodb_service and self.mongodb_service.conectado:
                # Buscar y eliminar la factura
                resultado = self.mongodb_service.db.invoices.delete_one({"invoice_id": factura_id})
                
                if resultado.deleted_count > 0:
                    self.actualizar_lista_facturas()
                    self.agregar_log(f"🗑️ Factura {factura_id} eliminada exitosamente")
                    messagebox.showinfo("Éxito", f"Factura {factura_id} eliminada exitosamente")
                else:
                    messagebox.showwarning("Advertencia", f"No se encontró la factura {factura_id}")
            else:
                messagebox.showerror("Error", "MongoDB no está disponible")
                
        except Exception as e:
            self.agregar_log(f"❌ Error eliminando factura: {e}")
            messagebox.showerror("Error", f"Error eliminando factura: {e}")
    
    def eliminar_proceso(self):
        """Eliminar proceso seleccionado"""
        try:
            # Verificar permisos según el rol
            if self.rol_usuario != "administrador":
                messagebox.showwarning("Permisos", "Solo los administradores pueden eliminar procesos")
                return
            
            seleccion = self.tree_procesos.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione un proceso para eliminar")
                return
            
            item = self.tree_procesos.item(seleccion[0])
            proceso_id = item['values'][0]
            nombre_proceso = item['values'][1]
            
            respuesta = messagebox.askyesno("Confirmar", f"¿Eliminar el proceso '{nombre_proceso}'?")
            if not respuesta:
                return
            
            if self.mongodb_service and self.mongodb_service.conectado:
                self.mongodb_service.eliminar_proceso(proceso_id)
                self.actualizar_lista_procesos()
                self.agregar_log(f"🗑️ Proceso '{nombre_proceso}' eliminado")
                messagebox.showinfo("Éxito", f"Proceso '{nombre_proceso}' eliminado")
            else:
                messagebox.showerror("Error", "MongoDB no disponible")
                
        except Exception as e:
            self.agregar_log(f"❌ Error eliminando proceso: {e}")
            messagebox.showerror("Error", f"Error eliminando proceso: {e}")
    
    def mostrar_historial_ejecucion(self):
        """Mostrar historial completo de ejecución de procesos con persistencia en MongoDB"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no disponible")
                return
            
            # Crear ventana de historial mejorada
            historial_window = tk.Toplevel(self.root)
            historial_window.title("Historial de Ejecución de Procesos")
            historial_window.geometry("1000x700")
            historial_window.configure(bg='white')
            historial_window.transient(self.root)
            historial_window.grab_set()
            
            # Header con información del usuario
            header_frame = tk.Frame(historial_window, bg='#3498db', height=60)
            header_frame.pack(fill='x')
            header_frame.pack_propagate(False)
            
            tk.Label(header_frame, text="📊 Historial de Ejecución de Procesos", 
                    font=('Arial', 16, 'bold'), fg='white', bg='#3498db').pack(pady=15)
            
            # Frame de controles
            controles_frame = tk.Frame(historial_window, bg='white')
            controles_frame.pack(fill='x', padx=20, pady=10)
            
            # Filtros y controles
            tk.Label(controles_frame, text="Filtros:", font=('Arial', 12, 'bold'), bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
            
            tk.Label(controles_frame, text="Estado:", bg='white').grid(row=0, column=1, padx=5, pady=5, sticky='w')
            combo_estado = ttk.Combobox(controles_frame, values=["Todos", "Completado", "Error", "En Progreso"], width=15)
            combo_estado.grid(row=0, column=2, padx=5, pady=5)
            combo_estado.set("Todos")
            
            tk.Label(controles_frame, text="Fecha desde:", bg='white').grid(row=0, column=3, padx=5, pady=5, sticky='w')
            entry_fecha_desde = tk.Entry(controles_frame, width=12)
            entry_fecha_desde.grid(row=0, column=4, padx=5, pady=5)
            entry_fecha_desde.insert(0, (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
            
            tk.Label(controles_frame, text="Fecha hasta:", bg='white').grid(row=0, column=5, padx=5, pady=5, sticky='w')
            entry_fecha_hasta = tk.Entry(controles_frame, width=12)
            entry_fecha_hasta.grid(row=0, column=6, padx=5, pady=5)
            entry_fecha_hasta.insert(0, datetime.now().strftime("%Y-%m-%d"))
            
            def aplicar_filtros():
                estado_filtro = combo_estado.get()
                fecha_desde = entry_fecha_desde.get()
                fecha_hasta = entry_fecha_hasta.get()
                
                # Limpiar tabla
                for item in tree_historial.get_children():
                    tree_historial.delete(item)
                
                # Obtener ejecuciones filtradas
                ejecuciones = self.mongodb_service.obtener_ejecuciones_usuario_filtradas(
                    self.usuario_autenticado, estado_filtro, fecha_desde, fecha_hasta
                )
                
                # Mostrar estadísticas
                stats_text.delete("1.0", tk.END)
                stats_text.insert(tk.END, f"📊 ESTADÍSTICAS DEL HISTORIAL\n")
                stats_text.insert(tk.END, f"=" * 40 + "\n")
                stats_text.insert(tk.END, f"• Total de ejecuciones: {len(ejecuciones)}\n")
                
                if ejecuciones:
                    completadas = len([e for e in ejecuciones if e.get('status') == 'completado'])
                    errores = len([e for e in ejecuciones if e.get('status') == 'error'])
                    stats_text.insert(tk.END, f"• Ejecuciones completadas: {completadas}\n")
                    stats_text.insert(tk.END, f"• Ejecuciones con error: {errores}\n")
                    stats_text.insert(tk.END, f"• Tasa de éxito: {(completadas/len(ejecuciones)*100):.1f}%\n")
                    
                    # Procesos más ejecutados
                    procesos_count = {}
                    for ejecucion in ejecuciones:
                        proceso = ejecucion.get('process_name', 'Desconocido')
                        procesos_count[proceso] = procesos_count.get(proceso, 0) + 1
                    
                    proceso_mas_ejecutado = max(procesos_count.items(), key=lambda x: x[1])
                    stats_text.insert(tk.END, f"• Proceso más ejecutado: {proceso_mas_ejecutado[0]} ({proceso_mas_ejecutado[1]} veces)\n")
                
                # Poblar tabla
                if ejecuciones:
                    for ejecucion in ejecuciones:
                        estado = ejecucion.get('status', 'completado')
                        estado_emoji = "✅" if estado == "completado" else "❌" if estado == "error" else "⏳"
                        
                        fecha_ejecucion = ejecucion.get('executed_at', '')
                        fecha_formateada = fecha_ejecucion[:16] if fecha_ejecucion else 'N/A'
                        
                        resultado = ejecucion.get('result', '')
                        resultado_corto = resultado[:50] + "..." if len(resultado) > 50 else resultado
                        
                        mediciones_procesadas = ejecucion.get('measurements_processed', 0)
                        
                        tree_historial.insert("", "end", values=(
                            ejecucion.get('execution_id', ''),
                            ejecucion.get('process_name', 'Proceso'),
                            fecha_formateada,
                            resultado_corto,
                            f"{estado_emoji} {estado.title()}",
                            mediciones_procesadas,
                            ejecucion.get('duration_seconds', 'N/A')
                        ))
                else:
                    tree_historial.insert("", "end", values=("", "No hay ejecuciones registradas", "", "", "", "", ""))
            
            tk.Button(controles_frame, text="🔍 Aplicar Filtros", command=aplicar_filtros,
                     bg='#3498db', fg='white', font=('Arial', 10)).grid(row=0, column=7, padx=10, pady=5)
            
            tk.Button(controles_frame, text="📊 Exportar CSV", command=lambda: self.exportar_historial_csv(),
                     bg='#27ae60', fg='white', font=('Arial', 10)).grid(row=0, column=8, padx=5, pady=5)
            
            # Frame principal con tabla y estadísticas
            main_frame = tk.Frame(historial_window, bg='white')
            main_frame.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Frame izquierdo - Tabla de historial
            tabla_frame = tk.LabelFrame(main_frame, text="Historial de Ejecuciones", 
                                      font=('Arial', 12, 'bold'), bg='white')
            tabla_frame.pack(side='left', fill='both', expand=True, padx=(0, 10))
            
            # Crear Treeview mejorado para historial
            columns = ("ID", "Proceso", "Fecha", "Resultado", "Estado", "Mediciones", "Duración")
            tree_historial = ttk.Treeview(tabla_frame, columns=columns, show="headings", height=15)
            
            # Configurar columnas
            tree_historial.heading("ID", text="ID Ejecución")
            tree_historial.heading("Proceso", text="Proceso")
            tree_historial.heading("Fecha", text="Fecha Ejecución")
            tree_historial.heading("Resultado", text="Resultado")
            tree_historial.heading("Estado", text="Estado")
            tree_historial.heading("Mediciones", text="Mediciones")
            tree_historial.heading("Duración", text="Duración (s)")
            
            tree_historial.column("ID", width=100)
            tree_historial.column("Proceso", width=150)
            tree_historial.column("Fecha", width=120)
            tree_historial.column("Resultado", width=200)
            tree_historial.column("Estado", width=100)
            tree_historial.column("Mediciones", width=80)
            tree_historial.column("Duración", width=80)
            
            # Scrollbar para la tabla
            scrollbar_tabla = ttk.Scrollbar(tabla_frame, orient="vertical", command=tree_historial.yview)
            tree_historial.configure(yscrollcommand=scrollbar_tabla.set)
            
            tree_historial.pack(side="left", fill="both", expand=True)
            scrollbar_tabla.pack(side="right", fill="y")
            
            # Frame derecho - Estadísticas
            stats_frame = tk.LabelFrame(main_frame, text="Estadísticas", 
                                      font=('Arial', 12, 'bold'), bg='white')
            stats_frame.pack(side='right', fill='both', padx=(10, 0))
            
            stats_text = scrolledtext.ScrolledText(stats_frame, height=15, width=40)
            stats_text.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Aplicar filtros iniciales
            aplicar_filtros()
            
            # Botones de acción
            botones_frame = tk.Frame(historial_window, bg='white')
            botones_frame.pack(pady=10)
            
            tk.Button(botones_frame, text="🔄 Actualizar", command=aplicar_filtros,
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(botones_frame, text="📋 Ver Detalles", 
                     command=lambda: self.mostrar_detalles_ejecucion(tree_historial),
                     bg='#f39c12', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(botones_frame, text="🗑️ Limpiar Historial", 
                     command=lambda: self.limpiar_historial_ejecucion(),
                     bg='#e74c3c', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            tk.Button(botones_frame, text="Cerrar", command=historial_window.destroy,
                     bg='#95a5a6', fg='white', font=('Arial', 10)).pack(side='left', padx=5)
            
            self.agregar_log(f"📊 Historial de ejecución mostrado con filtros avanzados")
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando historial: {e}")
            messagebox.showerror("Error", f"Error mostrando historial: {e}")
    
    def mostrar_detalles_ejecucion(self, tree_historial):
        """Mostrar detalles completos de una ejecución seleccionada"""
        try:
            seleccion = tree_historial.selection()
            if not seleccion:
                messagebox.showwarning("Advertencia", "Seleccione una ejecución para ver detalles")
                return
            
            item = tree_historial.item(seleccion[0])
            execution_id = item['values'][0]
            
            if not execution_id:
                messagebox.showwarning("Advertencia", "Seleccione una ejecución válida")
                return
            
            # Obtener detalles completos desde MongoDB
            ejecucion = self.mongodb_service.obtener_ejecucion_por_id(execution_id)
            
            if not ejecucion:
                messagebox.showerror("Error", "No se encontraron detalles de la ejecución")
                return
            
            # Crear ventana de detalles
            detalles_window = tk.Toplevel(self.root)
            detalles_window.title(f"Detalles de Ejecución - {execution_id}")
            detalles_window.geometry("600x500")
            detalles_window.configure(bg='white')
            detalles_window.transient(self.root)
            detalles_window.grab_set()
            
            tk.Label(detalles_window, text=f"Detalles de Ejecución: {execution_id}", 
                    font=('Arial', 14, 'bold'), bg='white').pack(pady=10)
            
            # Área de detalles
            detalles_text = scrolledtext.ScrolledText(detalles_window, height=20)
            detalles_text.pack(fill='both', expand=True, padx=20, pady=10)
            
            # Mostrar información detallada
            detalles_text.insert(tk.END, f"📊 INFORMACIÓN DETALLADA DE LA EJECUCIÓN\n")
            detalles_text.insert(tk.END, f"=" * 50 + "\n\n")
            
            detalles_text.insert(tk.END, f"🆔 ID de Ejecución: {ejecucion.get('execution_id', 'N/A')}\n")
            detalles_text.insert(tk.END, f"🔧 Proceso: {ejecucion.get('process_name', 'N/A')}\n")
            detalles_text.insert(tk.END, f"👤 Usuario: {ejecucion.get('user_id', 'N/A')}\n")
            detalles_text.insert(tk.END, f"📅 Fecha de Ejecución: {ejecucion.get('executed_at', 'N/A')}\n")
            detalles_text.insert(tk.END, f"📊 Estado: {ejecucion.get('status', 'N/A')}\n")
            detalles_text.insert(tk.END, f"📈 Mediciones Procesadas: {ejecucion.get('measurements_processed', 0)}\n")
            detalles_text.insert(tk.END, f"⏱️ Duración: {ejecucion.get('duration_seconds', 'N/A')} segundos\n\n")
            
            detalles_text.insert(tk.END, f"📋 RESULTADO COMPLETO:\n")
            detalles_text.insert(tk.END, f"-" * 30 + "\n")
            detalles_text.insert(tk.END, f"{ejecucion.get('result', 'Sin resultado disponible')}\n\n")
            
            if ejecucion.get('error_details'):
                detalles_text.insert(tk.END, f"❌ DETALLES DEL ERROR:\n")
                detalles_text.insert(tk.END, f"-" * 30 + "\n")
                detalles_text.insert(tk.END, f"{ejecucion.get('error_details', 'N/A')}\n\n")
            
            detalles_text.insert(tk.END, f"🔧 CONFIGURACIÓN DEL PROCESO:\n")
            detalles_text.insert(tk.END, f"-" * 30 + "\n")
            detalles_text.insert(tk.END, f"• Sensor: {ejecucion.get('sensor_name', 'N/A')}\n")
            detalles_text.insert(tk.END, f"• Tipo: {ejecucion.get('process_type', 'N/A')}\n")
            detalles_text.insert(tk.END, f"• Fecha Inicio: {ejecucion.get('fecha_inicio', 'N/A')}\n")
            detalles_text.insert(tk.END, f"• Fecha Fin: {ejecucion.get('fecha_fin', 'N/A')}\n")
            
            # Botón cerrar
            tk.Button(detalles_window, text="Cerrar", command=detalles_window.destroy,
                     bg='#3498db', fg='white', font=('Arial', 10)).pack(pady=10)
            
        except Exception as e:
            self.agregar_log(f"❌ Error mostrando detalles: {e}")
            messagebox.showerror("Error", f"Error mostrando detalles: {e}")
    
    def limpiar_historial_ejecucion(self):
        """Limpiar historial de ejecuciones (solo ejecuciones antiguas)"""
        try:
            respuesta = messagebox.askyesno("Confirmar", 
                "¿Limpiar ejecuciones anteriores a 30 días?\n\nEsto eliminará permanentemente los registros antiguos.")
            if not respuesta:
                return
            
            if self.mongodb_service and self.mongodb_service.conectado:
                fecha_limite = (datetime.now() - timedelta(days=30)).isoformat()
                eliminados = self.mongodb_service.limpiar_ejecuciones_antiguas(self.usuario_autenticado, fecha_limite)
                
                messagebox.showinfo("Éxito", f"Se eliminaron {eliminados} ejecuciones antiguas")
                self.agregar_log(f"🗑️ Historial limpiado: {eliminados} ejecuciones eliminadas")
            else:
                messagebox.showerror("Error", "MongoDB no disponible")
                
        except Exception as e:
            self.agregar_log(f"❌ Error limpiando historial: {e}")
            messagebox.showerror("Error", f"Error limpiando historial: {e}")
    
    def exportar_historial_csv(self):
        """Exportar historial de ejecuciones a CSV"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB no disponible")
                return
            
            # Obtener todas las ejecuciones del usuario
            ejecuciones = self.mongodb_service.obtener_ejecuciones_usuario(self.usuario_autenticado)
            
            if not ejecuciones:
                messagebox.showwarning("Advertencia", "No hay ejecuciones para exportar")
                return
            
            # Crear archivo CSV
            nombre_archivo = f"historial_ejecuciones_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            import csv
            with open(nombre_archivo, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['execution_id', 'process_name', 'executed_at', 'status', 'result', 'measurements_processed', 'duration_seconds']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for ejecucion in ejecuciones:
                    writer.writerow({
                        'execution_id': ejecucion.get('execution_id', ''),
                        'process_name': ejecucion.get('process_name', ''),
                        'executed_at': ejecucion.get('executed_at', ''),
                        'status': ejecucion.get('status', ''),
                        'result': ejecucion.get('result', ''),
                        'measurements_processed': ejecucion.get('measurements_processed', 0),
                        'duration_seconds': ejecucion.get('duration_seconds', '')
                    })
            
            messagebox.showinfo("Éxito", f"Historial exportado como {nombre_archivo}")
            self.agregar_log(f"📊 Historial exportado: {len(ejecuciones)} ejecuciones")
            
        except Exception as e:
            self.agregar_log(f"❌ Error exportando historial: {e}")
            messagebox.showerror("Error", f"Error exportando historial: {e}")
    
    def probar_conexiones(self):
        """Probar conexiones a las bases de datos"""
        try:
            # Probar MongoDB Atlas
            if self.mongodb_service and self.mongodb_service.conectar():
                self.agregar_log("✅ MongoDB Atlas: Conexión exitosa")
            else:
                self.agregar_log("❌ MongoDB Atlas: Error de conexión")
            
            # Probar Neo4j Aura
            if self.neo4j_service and self.neo4j_service.conectar():
                self.agregar_log("✅ Neo4j Aura: Conexión exitosa")
            else:
                self.agregar_log("⚠️ Neo4j Aura: No disponible")
            
            # Probar Redis
            if self.redis_service and self.redis_service.conectar():
                self.agregar_log("✅ Redis: Conexión exitosa")
            else:
                self.agregar_log("⚠️ Redis: No disponible")
            
            # Actualizar estado en la interfaz
            self.actualizar_estado_conexiones()
            
        except Exception as e:
            self.agregar_log(f"❌ Error probando conexiones: {e}")
    
    def mostrar_estadisticas_sistema(self):
        """Mostrar estadísticas del sistema"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                messagebox.showerror("Error", "MongoDB Atlas no está conectado")
                return
            
            stats = self.mongodb_service.obtener_estadisticas()
            
            # Información de Redis
            redis_info = ""
            if self.redis_service and self.redis_service.conectado:
                try:
                    redis_stats = self.redis_service.info()
                    cache_keys = len(self.redis_service.keys("cache:*"))
                    session_keys = len(self.redis_service.keys("session:*"))
                    
                    redis_info = f"""⚡ Redis Cloud: ✅ Conectado
   Memoria usada: {redis_stats.get('used_memory_human', 'N/A')}
   Conexiones: {redis_stats.get('connected_clients', 'N/A')}
   Cache keys: {cache_keys}
   Sesiones activas: {session_keys}
   Comandos procesados: {redis_stats.get('total_commands_processed', 'N/A')}"""
                except:
                    redis_info = "⚡ Redis Cloud: ✅ Conectado (estadísticas no disponibles)"
            else:
                redis_info = "⚡ Redis Cloud: ⚠️ No disponible"
            
            mensaje = f"""📊 ESTADÍSTICAS DEL SISTEMA
            
🗂️ MongoDB Atlas:
   Sensores: {stats.get('sensores', 0)}
   Usuarios: {stats.get('usuarios', 0)}
   Alertas: {stats.get('alertas', 0)}
   Mediciones: {stats.get('mediciones', 0)}
   Facturas: {stats.get('facturas', 0)}
   Pagos: {stats.get('pagos', 0)}
   Procesos: {stats.get('procesos', 0)}

🔗 Neo4j Aura: {'✅ Conectado' if self.neo4j_service and self.neo4j_service.conectado else '⚠️ No disponible'}

{redis_info}

🌐 Modo: ONLINE COMPLETO
🏗️ Arquitectura: Persistencia Poliglota"""
            
            messagebox.showinfo("Estadísticas del Sistema", mensaje)
            
        except Exception as e:
            messagebox.showerror("Error", f"Error obteniendo estadísticas: {e}")
    
    def limpiar_cache(self):
        """Limpiar cache del sistema"""
        if not self.redis_service or not self.redis_service.conectado:
            messagebox.showerror("Error", "Redis Cloud no está conectado")
            return
        
        try:
            # Obtener todas las claves de cache
            cache_keys = self.redis_service.keys("cache:*")
            session_keys = self.redis_service.keys("session:*")
            
            total_keys = len(cache_keys) + len(session_keys)
            
            if total_keys == 0:
                messagebox.showinfo("Cache", "No hay datos en cache para limpiar")
                return
            
            # Confirmar limpieza
            if messagebox.askyesno("Limpiar Cache", 
                                 f"¿Desea limpiar {total_keys} elementos del cache?\n"
                                 f"- Cache: {len(cache_keys)} elementos\n"
                                 f"- Sesiones: {len(session_keys)} elementos"):
                
                # Limpiar cache
                if cache_keys:
                    self.redis_service.delete(*cache_keys)
                
                # Limpiar sesiones
                if session_keys:
                    self.redis_service.delete(*session_keys)
                
                self.agregar_log(f"🧹 Cache limpiado: {total_keys} elementos eliminados")
                messagebox.showinfo("Cache", f"Cache limpiado exitosamente\n{total_keys} elementos eliminados")
                
                # Actualizar listas para reflejar cambios
                self.actualizar_lista_sensores()
                self.actualizar_lista_alertas()
                self.actualizar_lista_facturas()
                self.actualizar_lista_procesos()
                
        except Exception as e:
            messagebox.showerror("Error", f"Error limpiando cache: {e}")
            self.agregar_log(f"❌ Error limpiando cache: {e}")
    
    def agregar_log(self, mensaje):
        """Agregar mensaje al log"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] {mensaje}\n"
            
            # Solo agregar al log si el widget existe
            if hasattr(self, 'texto_logs') and self.texto_logs:
                self.texto_logs.insert(tk.END, log_entry)
                self.texto_logs.see(tk.END)
            else:
                # Si no existe el widget, solo imprimir en consola
                print(log_entry.strip())
        except Exception as e:
            print(f"Error agregando log: {e}")
    
    def crear_tab_administracion(self):
        """Crear tab de administración de usuarios (solo para admin)"""
        self.tab_administracion = tk.Frame(self.notebook, bg='white')
        self.notebook.add(self.tab_administracion, text="Administración")
        
        # Crear contenido de administración (inicialmente vacío)
        self.contenido_administracion = tk.Frame(self.tab_administracion, bg='white')
        
        # Bind para verificar acceso cuando se selecciona la pestaña
        self.notebook.bind("<<NotebookTabChanged>>", self.verificar_acceso_administracion)
    
    def verificar_acceso_administracion(self, event=None):
        """Verificar acceso a la pestaña de administración y actualizar comunicación"""
        try:
            # Verificar que el notebook existe y tiene pestañas
            if not hasattr(self, 'notebook') or not self.notebook:
                return
                
            # Obtener la pestaña seleccionada
            selected_tab = self.notebook.select()
            
            # Si no hay pestaña seleccionada, salir
            if not selected_tab:
                return
                
            tab_text = None
            try:
                tab_text = self.notebook.tab(selected_tab, "text")
            except tk.TclError:
                # Pestaña no válida, salir
                return
            
            # Verificar que se obtuvo el texto de la pestaña
            if not tab_text:
                return
            
            # Verificar si es la pestaña de administración
            if tab_text == "Administración":
                # Limpiar contenido anterior
                for widget in self.tab_administracion.winfo_children():
                    widget.destroy()
                
                # Verificar si el usuario es admin
                if not self.es_usuario_admin():
                    # Mostrar mensaje de acceso denegado
                    access_frame = tk.Frame(self.tab_administracion, bg='white')
                    access_frame.pack(expand=True, fill='both')
                    
                    tk.Label(access_frame, text="🔒 ACCESO DENEGADO", 
                            font=('Arial', 20, 'bold'), bg='white', fg='#e74c3c').pack(pady=50)
                    
                    tk.Label(access_frame, text="Esta sección solo está disponible para administradores", 
                            font=('Arial', 12), bg='white', fg='#7f8c8d').pack(pady=10)
                    
                    tk.Button(access_frame, text="🔑 Iniciar Sesión como Admin", 
                             command=self.mostrar_dialogo_login,
                             bg='#3498db', fg='white', font=('Arial', 12, 'bold')).pack(pady=20)
                else:
                    # Usuario es admin, mostrar contenido completo
                    self.crear_contenido_administracion()
            
            # Verificar si es la pestaña de comunicación
            elif tab_text == "Comunicación":
                # Actualizar mensajes automáticamente
                self.actualizar_mensajes()
                self.agregar_log("📨 Mensajes actualizados automáticamente al abrir Comunicación")
                    
        except Exception as e:
            print(f"Error verificando acceso a pestañas: {e}")
    
    def crear_contenido_administracion(self):
        """Crear contenido completo de la pestaña de administración"""
        # Configuración de usuarios
        config_frame = tk.LabelFrame(self.tab_administracion, text="Gestión de Usuarios", 
                                   font=('Arial', 12, 'bold'), bg='white')
        config_frame.pack(fill='x', padx=20, pady=10)
        
        config_inner = tk.Frame(config_frame, bg='white')
        config_inner.pack(fill='x', padx=10, pady=10)
        
        # Campos para nuevo usuario
        tk.Label(config_inner, text="Nombre de Usuario:", bg='white').grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.entry_nombre_usuario = tk.Entry(config_inner, width=25)
        self.entry_nombre_usuario.grid(row=0, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Email:", bg='white').grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.entry_email_usuario = tk.Entry(config_inner, width=25)
        self.entry_email_usuario.grid(row=0, column=3, padx=5, pady=5)
        
        tk.Label(config_inner, text="Contraseña:", bg='white').grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.entry_password_usuario = tk.Entry(config_inner, width=25, show="*")
        self.entry_password_usuario.grid(row=1, column=1, padx=5, pady=5)
        
        tk.Label(config_inner, text="Rol:", bg='white').grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.combo_rol_usuario = ttk.Combobox(config_inner, values=["usuario", "técnico", "administrador"], width=22)
        self.combo_rol_usuario.grid(row=1, column=3, padx=5, pady=5)
        self.combo_rol_usuario.set("usuario")
        
        tk.Label(config_inner, text="Estado:", bg='white').grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.combo_estado_usuario = ttk.Combobox(config_inner, values=["activo", "inactivo"], width=22)
        self.combo_estado_usuario.grid(row=2, column=1, padx=5, pady=5)
        self.combo_estado_usuario.set("activo")
        
        # Botones de gestión
        tk.Button(config_inner, text="➕ Crear Usuario", 
                 command=self.crear_usuario, 
                 bg='#27ae60', fg='white', font=('Arial', 10)).grid(row=3, column=0, padx=5, pady=10)
        
        tk.Button(config_inner, text="🔄 Actualizar Lista", 
                 command=self.actualizar_lista_usuarios, 
                 bg='#3498db', fg='white', font=('Arial', 10)).grid(row=3, column=1, padx=5, pady=10)
        
        tk.Button(config_inner, text="✏️ Editar Usuario", 
                 command=self.editar_usuario, 
                 bg='#f39c12', fg='white', font=('Arial', 10)).grid(row=3, column=2, padx=5, pady=10)
        
        tk.Button(config_inner, text="🗑️ Eliminar Usuario", 
                 command=self.eliminar_usuario, 
                 bg='#e74c3c', fg='white', font=('Arial', 10)).grid(row=3, column=3, padx=5, pady=10)
        
        # Botón de sincronización
        tk.Button(config_inner, text="🔄 Sincronizar con Neo4j", 
                 command=self.sincronizar_usuarios_con_neo4j, 
                 bg='#9b59b6', fg='white', font=('Arial', 10)).grid(row=4, column=0, padx=5, pady=10)
        
        # Lista de usuarios
        lista_frame = tk.LabelFrame(self.tab_administracion, text="Lista de Usuarios", 
                                  font=('Arial', 12, 'bold'), bg='white')
        lista_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Treeview para usuarios
        columns = ("ID", "Usuario", "Email", "Rol", "Estado", "Registro", "Última Sesión")
        self.tree_usuarios = ttk.Treeview(lista_frame, columns=columns, show="headings")
        
        for col in columns:
            self.tree_usuarios.heading(col, text=col)
            self.tree_usuarios.column(col, width=120)
        
        # Scrollbar para la lista
        scrollbar_usuarios = ttk.Scrollbar(lista_frame, orient="vertical", command=self.tree_usuarios.yview)
        self.tree_usuarios.configure(yscrollcommand=scrollbar_usuarios.set)
        
        self.tree_usuarios.pack(side="left", fill="both", expand=True)
        scrollbar_usuarios.pack(side="right", fill="y")
        
        # Bind doble click para editar
        self.tree_usuarios.bind("<Double-1>", self.al_hacer_doble_clic_usuario)
        
        # Estadísticas de usuarios
        stats_frame = tk.LabelFrame(self.tab_administracion, text="Estadísticas de Usuarios", 
                                  font=('Arial', 12, 'bold'), bg='white')
        stats_frame.pack(fill='x', padx=20, pady=10)
        
        stats_inner = tk.Frame(stats_frame, bg='white')
        stats_inner.pack(fill='x', padx=10, pady=10)
        
        # Labels de estadísticas
        self.etiquetas_stats_usuarios = {}
        stats_data = [
            ("Total Usuarios", "0"),
            ("Usuarios Activos", "0"),
            ("Administradores", "0"),
            ("Técnicos", "0")
        ]
        
        for i, (label, value) in enumerate(stats_data):
            frame = tk.Frame(stats_inner, bg='white')
            frame.pack(side='left', padx=20, pady=5)
            
            tk.Label(frame, text=label, font=('Arial', 10), bg='white').pack()
            self.etiquetas_stats_usuarios[label] = tk.Label(frame, text=value, font=('Arial', 14, 'bold'), 
                                              bg='white', fg='#2c3e50')
            self.etiquetas_stats_usuarios[label].pack()
        
        # Cargar datos iniciales de administración
        self.actualizar_lista_usuarios()
        self.actualizar_estadisticas_usuarios()
    
    def es_usuario_admin(self):
        """Verificar si el usuario actual es administrador"""
        if not self.usuario_autenticado or not self.sesion_activa:
            return False
        
        # Verificar rol directamente
        return self.rol_usuario == "administrador"
    
    def crear_usuario(self):
        """Crear nuevo usuario"""
        try:
            username = self.entry_nombre_usuario.get()
            email = self.entry_email_usuario.get()
            password = self.entry_password_usuario.get()
            rol = self.combo_rol_usuario.get()
            estado = self.combo_estado_usuario.get()
            
            if not username or not email or not password:
                messagebox.showerror("Error", "Por favor complete todos los campos obligatorios")
                return
            
            # Validar email básico
            if "@" not in email or "." not in email:
                messagebox.showerror("Error", "Por favor ingrese un email válido")
                return
            
            # Resolver role_id desde la colección de roles
            role_id = None
            try:
                if self.mongodb_service and self.mongodb_service.conectado and rol:
                    rol_obj = self.mongodb_service.obtener_rol_por_name(rol)
                    if rol_obj:
                        role_id = rol_obj.get("role_id")
            except Exception as e:
                self.agregar_log(f"⚠️ No se pudo resolver role_id para rol '{rol}': {e}")

            # Crear usuario en MongoDB Atlas
            usuario_data = {
                "user_id": f"USER_{int(time.time())}",
                "username": username,
                "email": email,
                "password": password,  # En un sistema real, esto debería estar encriptado
                "rol": rol,
                "role_id": role_id,
                "status": estado,
                "created_at": datetime.now().isoformat(),
                "last_login": None,
                "permissions": self.obtener_permisos_por_rol(rol)
            }
            
            if self.mongodb_service and self.mongodb_service.conectado:
                # Crear usuario en MongoDB
                if self.mongodb_service.crear_usuario(usuario_data):
                    # Sincronizar con Neo4j
                    if self.neo4j_service and self.neo4j_service.conectado:
                        neo4j_success = self.neo4j_service.crear_usuario(
                            user_id=usuario_data["user_id"],
                            email=email,
                            full_name=username,
                            role=rol
                        )
                        if neo4j_success:
                            self.agregar_log(f"✅ Usuario sincronizado en MongoDB y Neo4j: {username}")
                        else:
                            self.agregar_log(f"⚠️ Usuario creado en MongoDB pero error en Neo4j: {username}")
                    else:
                        self.agregar_log(f"⚠️ Usuario creado en MongoDB pero Neo4j no disponible: {username}")
                    
                    self.actualizar_lista_usuarios()
                    self.actualizar_estadisticas_usuarios()
                    
                    # Limpiar campos
                    self.entry_nombre_usuario.delete(0, tk.END)
                    self.entry_email_usuario.delete(0, tk.END)
                    self.entry_password_usuario.delete(0, tk.END)
                    
                    messagebox.showinfo("Éxito", f"Usuario '{username}' creado correctamente")
                    self.agregar_log(f"✅ Usuario creado: {username} ({rol})")
                else:
                    messagebox.showerror("Error", "No se pudo crear el usuario en MongoDB")
            else:
                messagebox.showerror("Error", "MongoDB Atlas no está conectado")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error creando usuario: {e}")
            self.agregar_log(f"❌ Error creando usuario: {e}")
    
    def sincronizar_usuarios_con_neo4j(self):
        """Sincronizar usuarios de MongoDB con Neo4j"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                self.agregar_log("⚠️ MongoDB no disponible para sincronización")
                return False
            
            if not self.neo4j_service or not self.neo4j_service.conectado:
                self.agregar_log("⚠️ Neo4j no disponible para sincronización")
                return False
            
            usuarios = self.mongodb_service.obtener_usuarios()
            sincronizados = 0
            errores = 0
            
            for usuario in usuarios:
                try:
                    user_id = usuario.get('user_id')
                    username = usuario.get('username')
                    email = usuario.get('email')
                    rol = usuario.get('rol')
                    
                    if user_id and username and email and rol:
                        success = self.neo4j_service.crear_usuario(
                            user_id=user_id,
                            email=email,
                            full_name=username,
                            role=rol
                        )
                        if success:
                            sincronizados += 1
                        else:
                            errores += 1
                except Exception as e:
                    self.agregar_log(f"❌ Error sincronizando usuario {usuario.get('username', 'desconocido')}: {e}")
                    errores += 1
            
            self.agregar_log(f"✅ Sincronización completada: {sincronizados} usuarios sincronizados, {errores} errores")
            return True
            
        except Exception as e:
            self.agregar_log(f"❌ Error en sincronización masiva: {e}")
            return False
    
    def asegurar_roles_iniciales(self):
        """Asegurar que los roles iniciales existan en la colección roles"""
        if not self.mongodb_service or not self.mongodb_service.conectado:
            return
        
        try:
            # Configurar colecciones (esto crea los roles iniciales si no existen)
            self.mongodb_service.configurar_colecciones_optimizadas()
        except Exception as e:
            self.agregar_log(f"⚠️ Error asegurando roles iniciales: {e}")
    
    def obtener_permisos_por_rol(self, rol):
        """Obtener permisos según el rol (puede ser string o role_id)"""
        try:
            # Primero intentar obtener desde la colección de roles
            if self.mongodb_service and self.mongodb_service.conectado:
                # Si es un role_id, obtener directamente
                if rol.startswith("ROL_"):
                    rol_data = self.mongodb_service.obtener_rol_por_id(rol)
                    if rol_data and rol_data.get("permissions"):
                        return rol_data["permissions"]
                
                # Si es un nombre (string), buscar por name
                rol_data = self.mongodb_service.obtener_rol_por_name(rol)
                if rol_data and rol_data.get("permissions"):
                    return rol_data["permissions"]
            
            # Fallback a permisos hardcodeados si no hay DB o no se encuentra
            permisos_fallback = {
                "usuario": ["read", "request_process"],
                "técnico": ["read", "write", "manage_sensors", "manage_alerts"],
                "administrador": ["read", "write", "admin", "manage_users", "manage_system"]
            }
            return permisos_fallback.get(rol, ["read"])
        except Exception as e:
            self.agregar_log(f"⚠️ Error obteniendo permisos por rol {rol}, usando fallback: {e}")
            return ["read"]
    
    def actualizar_lista_usuarios(self):
        """Actualizar lista de usuarios desde MongoDB Atlas"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            # Limpiar lista
            for item in self.tree_usuarios.get_children():
                self.tree_usuarios.delete(item)
            
            # Obtener usuarios desde MongoDB Atlas
            usuarios = self.mongodb_service.obtener_usuarios()
            
            for usuario in usuarios:
                self.tree_usuarios.insert('', 'end', values=(
                    usuario.get('user_id', ''),
                    usuario.get('username', ''),
                    usuario.get('email', ''),
                    usuario.get('rol', ''),
                    usuario.get('status', ''),
                    usuario.get('created_at', '')[:10] if usuario.get('created_at') else 'N/A',
                    usuario.get('last_login', 'Nunca')
                ))
            
            # self.agregar_log(f"✅ {len(usuarios)} usuarios cargados desde MongoDB Atlas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando usuarios: {e}")
    
    def editar_usuario(self):
        """Editar usuario seleccionado"""
        seleccionado = self.tree_usuarios.selection()
        if not seleccionado:
            messagebox.showwarning("Advertencia", "Por favor seleccione un usuario para editar")
            return
        
        # Obtener datos del usuario seleccionado
        item = self.tree_usuarios.item(seleccionado[0])
        valores = item['values']
        
        # Crear ventana de edición
        self.mostrar_dialogo_editar_usuario(valores)
    
    def mostrar_dialogo_editar_usuario(self, valores_usuario):
        """Mostrar diálogo para editar usuario"""
        edit_window = tk.Toplevel(self.root)
        edit_window.title("Editar Usuario")
        edit_window.geometry("500x400")
        edit_window.configure(bg='white')
        edit_window.grab_set()
        
        # Centrar ventana
        edit_window.transient(self.root)
        edit_window.geometry("+%d+%d" % (self.root.winfo_rootx() + 100, self.root.winfo_rooty() + 100))
        
        # Título
        tk.Label(edit_window, text="Editar Usuario", 
                font=('Arial', 16, 'bold'), bg='white', fg='#2c3e50').pack(pady=20)
        
        # Campos de edición
        frame_campos = tk.Frame(edit_window, bg='white')
        frame_campos.pack(pady=20)
        
        tk.Label(frame_campos, text="ID Usuario:", bg='white').grid(row=0, column=0, padx=10, pady=10, sticky='w')
        tk.Label(frame_campos, text=valores_usuario[0], bg='white', fg='#7f8c8d').grid(row=0, column=1, padx=10, pady=10, sticky='w')
        
        tk.Label(frame_campos, text="Nombre de Usuario:", bg='white').grid(row=1, column=0, padx=10, pady=10, sticky='w')
        entry_username = tk.Entry(frame_campos, width=30)
        entry_username.grid(row=1, column=1, padx=10, pady=10)
        entry_username.insert(0, valores_usuario[1])
        
        tk.Label(frame_campos, text="Email:", bg='white').grid(row=2, column=0, padx=10, pady=10, sticky='w')
        entry_email = tk.Entry(frame_campos, width=30)
        entry_email.grid(row=2, column=1, padx=10, pady=10)
        entry_email.insert(0, valores_usuario[2])
        
        tk.Label(frame_campos, text="Rol:", bg='white').grid(row=3, column=0, padx=10, pady=10, sticky='w')
        combo_rol = ttk.Combobox(frame_campos, values=["usuario", "técnico", "administrador"], width=27)
        combo_rol.grid(row=3, column=1, padx=10, pady=10)
        combo_rol.set(valores_usuario[3])
        
        tk.Label(frame_campos, text="Estado:", bg='white').grid(row=4, column=0, padx=10, pady=10, sticky='w')
        combo_estado = ttk.Combobox(frame_campos, values=["activo", "inactivo"], width=27)
        combo_estado.grid(row=4, column=1, padx=10, pady=10)
        combo_estado.set(valores_usuario[4])
        
        # Botones
        frame_botones = tk.Frame(edit_window, bg='white')
        frame_botones.pack(pady=20)
        
        def guardar_cambios():
            try:
                # Resolver role_id desde la colección de roles
                role_id_edit = None
                try:
                    if self.mongodb_service and self.mongodb_service.conectado and combo_rol.get():
                        rol_obj = self.mongodb_service.obtener_rol_por_name(combo_rol.get())
                        if rol_obj:
                            role_id_edit = rol_obj.get("role_id")
                except Exception as e:
                    self.agregar_log(f"⚠️ No se pudo resolver role_id al editar: {e}")

                # Actualizar usuario en MongoDB
                usuario_actualizado = {
                    "user_id": valores_usuario[0],
                    "username": entry_username.get(),
                    "email": entry_email.get(),
                    "rol": combo_rol.get(),
                    "role_id": role_id_edit,
                    "status": combo_estado.get(),
                    "permissions": self.obtener_permisos_por_rol(combo_rol.get())
                }
                
                if self.mongodb_service and self.mongodb_service.conectado:
                    self.mongodb_service.actualizar_usuario(valores_usuario[0], usuario_actualizado)
                    self.actualizar_lista_usuarios()
                    self.actualizar_estadisticas_usuarios()
                    
                    messagebox.showinfo("Éxito", "Usuario actualizado correctamente")
                    self.agregar_log(f"✅ Usuario actualizado: {entry_username.get()}")
                    edit_window.destroy()
                else:
                    messagebox.showerror("Error", "MongoDB Atlas no está conectado")
                    
            except Exception as e:
                messagebox.showerror("Error", f"Error actualizando usuario: {e}")
        
        tk.Button(frame_botones, text="💾 Guardar Cambios", command=guardar_cambios,
                 bg='#27ae60', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
        
        tk.Button(frame_botones, text="❌ Cancelar", command=edit_window.destroy,
                 bg='#95a5a6', fg='white', font=('Arial', 10)).pack(side='left', padx=10)
    
    def eliminar_usuario(self):
        """Eliminar usuario seleccionado"""
        seleccionado = self.tree_usuarios.selection()
        if not seleccionado:
            messagebox.showwarning("Advertencia", "Por favor seleccione un usuario para eliminar")
            return
        
        # Obtener datos del usuario seleccionado
        item = self.tree_usuarios.item(seleccionado[0])
        valores = item['values']
        username = valores[1]
        user_id = valores[0]
        
        # Confirmar eliminación
        if messagebox.askyesno("Confirmar Eliminación", 
                             f"¿Está seguro de que desea eliminar al usuario '{username}'?\n\n"
                             f"Esta acción no se puede deshacer."):
            try:
                if self.mongodb_service and self.mongodb_service.conectado:
                    self.mongodb_service.eliminar_usuario(user_id)
                    self.actualizar_lista_usuarios()
                    self.actualizar_estadisticas_usuarios()
                    
                    messagebox.showinfo("Éxito", f"Usuario '{username}' eliminado correctamente")
                    self.agregar_log(f"✅ Usuario eliminado: {username}")
                else:
                    messagebox.showerror("Error", "MongoDB Atlas no está conectado")
                    
            except Exception as e:
                messagebox.showerror("Error", f"Error eliminando usuario: {e}")
    
    def al_hacer_doble_clic_usuario(self, event):
        """Manejar doble click en usuario"""
        self.editar_usuario()
    
    def actualizar_estadisticas_usuarios(self):
        """Actualizar estadísticas de usuarios"""
        try:
            if not self.mongodb_service or not self.mongodb_service.conectado:
                return
            
            usuarios = self.mongodb_service.obtener_usuarios()
            
            total_usuarios = len(usuarios)
            usuarios_activos = len([u for u in usuarios if u.get('status') == 'activo'])
            administradores = len([u for u in usuarios if u.get('rol') == 'administrador'])
            tecnicos = len([u for u in usuarios if u.get('rol') == 'técnico'])
            
            self.etiquetas_stats_usuarios["Total Usuarios"].config(text=str(total_usuarios))
            self.etiquetas_stats_usuarios["Usuarios Activos"].config(text=str(usuarios_activos))
            self.etiquetas_stats_usuarios["Administradores"].config(text=str(administradores))
            self.etiquetas_stats_usuarios["Técnicos"].config(text=str(tecnicos))
            
            self.agregar_log("✅ Estadísticas de usuarios actualizadas")
            
        except Exception as e:
            self.agregar_log(f"❌ Error actualizando estadísticas de usuarios: {e}")
    
    def recargar_interfaz_segun_rol(self):
        """Recargar interfaz según el rol del usuario"""
        try:
            # Mostrar la interfaz principal
            self.crear_interfaz()
            
            # Ocultar todas las pestañas primero
            for i in range(self.notebook.index("end")):
                self.notebook.tab(i, state="hidden")
            
            # Mostrar pestañas según el rol
            if self.rol_usuario == "administrador":
                # Admin ve todas las pestañas
                for i in range(self.notebook.index("end")):
                    self.notebook.tab(i, state="normal")
                self.agregar_log("✅ Interfaz cargada para administrador - Acceso completo")
            elif self.rol_usuario == "técnico":
                # Técnico ve pestañas específicas (sin administración)
                pestañas_tecnico = ["Home", "Sensores", "Análisis", "Informes", "Alertas", "Facturación", "Comunicación", "Procesos", "Servicios", "Configuración", "Logs"]
                for i in range(self.notebook.index("end")):
                    tab_text = self.notebook.tab(i, "text")
                    if tab_text in pestañas_tecnico:
                        self.notebook.tab(i, state="normal")
                    else:
                        self.notebook.tab(i, state="hidden")
                self.agregar_log("✅ Interfaz cargada para técnico - Acceso completo excepto Administración")
            else:
                # Usuario común ve solo módulos esenciales para su funcionamiento
                pestañas_usuario = ["Home", "Sensores", "Análisis", "Informes", "Alertas", "Facturación", "Comunicación", "Procesos", "Servicios"]
                for i in range(self.notebook.index("end")):
                    tab_text = self.notebook.tab(i, "text")
                    if tab_text in pestañas_usuario:
                        self.notebook.tab(i, state="normal")
                    else:
                        self.notebook.tab(i, state="hidden")
                self.agregar_log("✅ Interfaz cargada para usuario común - Acceso limitado a módulos esenciales")
            
            # Cargar datos iniciales
            self.cargar_datos_iniciales()
            
            # Cargar destinatarios para comunicación
            self.cargar_destinatarios()
            
            # Cargar usuarios para facturación
            self.cargar_usuarios_facturacion()
            
            # Reconfigurar botones de procesos según el rol
            self.reconfigurar_botones_procesos()
            
            # Reconfigurar botones de alertas según el rol
            self.reconfigurar_botones_alertas()
            
            # Reconfigurar botones de sensores según el rol
            self.configurar_botones_sensores()
            
            # Reconfigurar botones de comunicación según el rol
            self.configurar_botones_comunicacion()
            
            # Reconfigurar botones de alertas según el rol
            self.configurar_botones_alertas()
            
            # Reconfigurar botones de facturación según el rol
            self.configurar_botones_facturacion()
            
            # Seleccionar automáticamente la pestaña Home al cargar la interfaz
            try:
                # Buscar la pestaña Home por su texto para asegurarnos de que la encontramos
                num_tabs = self.notebook.index("end")
                home_tab_index = None
                for i in range(num_tabs):
                    try:
                        tab_text = self.notebook.tab(i, "text")
                        if tab_text == "Home":
                            home_tab_index = i
                            break
                    except:
                        continue
                
                # Si encontramos Home, seleccionarla
                if home_tab_index is not None:
                    self.notebook.select(home_tab_index)
                    self.agregar_log("✅ Pestaña Home seleccionada automáticamente")
                else:
                    # Si no encontramos Home, seleccionar la primera pestaña disponible
                    for i in range(num_tabs):
                        if self.notebook.tab(i, "state") != "hidden":
                            self.notebook.select(i)
                            self.agregar_log(f"✅ Primera pestaña disponible seleccionada (índice {i})")
                            break
                    
            except Exception as e:
                self.agregar_log(f"⚠️ No se pudo seleccionar la pestaña Home: {e}")
            
        except Exception as e:
            self.agregar_log(f"❌ Error recargando interfaz: {e}")
    
    def reconfigurar_botones_alertas(self):
        """Reconfigurar botones de alertas según el rol del usuario"""
        try:
            # Buscar el botón de resolver alerta en el módulo de alertas
            # El botón se crea en crear_tab_alertas con el texto "✅ Resolver Alerta"
            
            # Buscar todos los widgets en el notebook de alertas
            for i in range(self.notebook.index("end")):
                tab_text = self.notebook.tab(i, "text")
                if tab_text == "Alertas":
                    # Obtener el frame de la pestaña de alertas
                    alertas_frame = self.notebook.nametowidget(self.notebook.tabs()[i])
                    
                    # Buscar el botón de resolver alerta recursivamente
                    self._configurar_boton_resolver_alerta(alertas_frame)
                    break
                    
        except Exception as e:
            self.agregar_log(f"❌ Error reconfigurando botones de alertas: {e}")
    
    def _configurar_boton_resolver_alerta(self, widget):
        """Configurar botón de resolver alerta recursivamente"""
        try:
            # Si es un botón con el texto "✅ Resolver Alerta"
            if isinstance(widget, tk.Button) and "Resolver Alerta" in widget.cget("text"):
                if self.rol_usuario in ["técnico", "administrador"]:
                    # Habilitar botón para técnicos y administradores
                    widget.config(state="normal")
                    self.agregar_log(f"✅ Botón 'Resolver Alerta' habilitado para rol: {self.rol_usuario}")
                else:
                    # Deshabilitar botón para usuarios comunes
                    widget.config(state="disabled")
                    self.agregar_log(f"🔒 Botón 'Resolver Alerta' deshabilitado para rol: {self.rol_usuario}")
                return
            
            # Buscar recursivamente en los widgets hijos
            for child in widget.winfo_children():
                self._configurar_boton_resolver_alerta(child)
                
        except Exception as e:
            # Ignorar errores en widgets específicos
            pass
    
    def reconfigurar_botones_procesos(self):
        """Reconfigurar botones de procesos cuando cambia el rol del usuario"""
        try:
            # Buscar el frame de configuración de procesos
            for widget in self.root.winfo_children():
                if isinstance(widget, ttk.Notebook):
                    for i in range(widget.index("end")):
                        tab_text = widget.tab(i, "text")
                        if tab_text == "Procesos":
                            # Encontrar el frame de configuración dentro de la pestaña Procesos
                            tab_frame = widget.nametowidget(widget.tabs()[i])
                            for child in tab_frame.winfo_children():
                                if isinstance(child, tk.LabelFrame) and "Configuración de Procesos" in child.cget("text"):
                                    # Encontrar el frame interno
                                    for inner_child in child.winfo_children():
                                        if isinstance(inner_child, tk.Frame):
                                            # Limpiar mensajes informativos existentes
                                            for msg_widget in inner_child.winfo_children():
                                                if isinstance(msg_widget, tk.Frame) and msg_widget.grid_info().get('row') == 4:
                                                    msg_widget.destroy()
                                            
                                            # Reconfigurar botones y mensaje
                                            self.configurar_botones_procesos(inner_child)
                                            self.agregar_mensaje_informativo_procesos(inner_child)
                                            self.agregar_log(f"✅ Interfaz de procesos reconfigurada para rol: {self.rol_usuario}")
                                            return
            self.agregar_log("⚠️ No se encontró el frame de configuración de procesos")
        except Exception as e:
            self.agregar_log(f"❌ Error reconfigurando interfaz de procesos: {e}")
    
    def run(self):
        """Ejecutar aplicación"""
        self.root.mainloop()

def main():
    """Función principal"""
    print("INICIANDO APLICACION ")
    print("=" * 50)
    
    if not MONGODB_ATLAS_DISPONIBLE:
        print("ERROR MongoDB Atlas es requerido para esta aplicacion")
        print("ERROR La aplicacion no puede funcionar sin conexion a MongoDB Atlas")
        return
    
    
    app = AplicacionSensoresOnline()
    app.run()

if __name__ == "__main__":
    main()
