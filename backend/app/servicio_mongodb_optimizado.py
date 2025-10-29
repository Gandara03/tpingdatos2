"""
Servicio MongoDB Atlas Optimizado
Sistema de Gestión de Sensores - Trabajo Práctico Ingeniería de Datos II

ARQUITECTURA OPTIMIZADA:
- MongoDB Atlas: Sensores, Mediciones (Time Series), Usuarios, Transacciones ACID, Alertas
- Neo4j: Mensajes, Grupos, Relaciones complejas
- Redis: Sesiones, Cache
"""

import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import math
import random

class ServicioMongoDBOptimizado:
    """Servicio optimizado para MongoDB Atlas con arquitectura especializada"""
    
    def __init__(self, connection_string: str, database_name: str):
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
        self.conectado = False
        
    def conectar(self) -> bool:
        """Conectar a MongoDB Atlas"""
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.database_name]
            
            # Probar conexión
            self.client.admin.command('ping')
            self.conectado = True
            return True
            
        except Exception as e:
            print(f"❌ Error conectando a MongoDB Atlas: {e}")
            self.conectado = False
            return False
    
    def desconectar(self):
        """Desconectar de MongoDB Atlas"""
        if self.client:
            self.client.close()
            self.conectado = False
            print("🔌 Desconectado de MongoDB Atlas")
    
    def configurar_colecciones_optimizadas(self):
        """Configurar colecciones con arquitectura optimizada"""
        if not self.conectado:
            return False
        
        try:
            print("🏗️ Configurando colecciones optimizadas...")
            
            # NOTA: Ya NO eliminamos colecciones existentes para preservar datos
            # Las colecciones 'users' y 'roles' se mantienen intactas para migración segura
            # Si se necesita resetear, hacerlo manualmente o con una función específica
            
            # 1. SENSORS - Documentos con metadatos completos
            sensors_collection = self.db.sensors
            
            # Índices optimizados para sensores
            sensors_collection.create_index("sensor_id", unique=True)
            sensors_collection.create_index("location.city")
            sensors_collection.create_index("location.country")
            sensors_collection.create_index("status")
            sensors_collection.create_index([("location.latitude", 1), ("location.longitude", 1)])
            
            print("   ✅ Colección 'sensors' configurada con índices geográficos")
            
            # 2. MEASUREMENTS - Time Series Collection optimizada
            try:
                # Intentar crear Time Series Collection (MongoDB 5.0+)
                self.db.create_collection(
                    "measurements",
                    timeseries={
                        "timeField": "timestamp",
                        "metaField": "sensor_id",
                        "granularity": "minutes"
                    }
                )
                print("   ✅ Time Series Collection 'measurements' creada")
                print("   🚀 Optimizada para alta ingesta y consultas temporales")
                
            except Exception as e:
                # Fallback a colección normal con índices optimizados
                measurements_collection = self.db.measurements
                measurements_collection.create_index([("sensor_id", 1), ("timestamp", -1)])
                measurements_collection.create_index("timestamp")
                measurements_collection.create_index("sensor_id")
                print(f"   ⚠️ Time Series no disponible, usando colección normal: {e}")
            
            # 3. ROLES - Colección de roles del sistema
            roles_collection = self.db.roles
            roles_collection.create_index("role_id", unique=True)
            roles_collection.create_index("name", unique=True)
            print("   ✅ Colección 'roles' configurada")
            
            # Crear roles iniciales si no existen
            roles_iniciales = [
                {
                    "role_id": "ROL_USUARIO_001",
                    "name": "usuario",
                    "description": "Usuario estándar del sistema. Puede leer datos y solicitar procesos.",
                    "permissions": ["read", "request_process"],
                    "created_at": datetime.now(),
                    "status": "active"
                },
                {
                    "role_id": "ROL_TECNICO_001",
                    "name": "técnico",
                    "description": "Técnico del sistema. Puede gestionar sensores, alertas y ejecutar procesos.",
                    "permissions": ["read", "write", "manage_sensors", "manage_alerts"],
                    "created_at": datetime.now(),
                    "status": "active"
                },
                {
                    "role_id": "ROL_ADMIN_001",
                    "name": "administrador",
                    "description": "Administrador del sistema. Acceso completo incluyendo gestión de usuarios y sistema.",
                    "permissions": ["read", "write", "admin", "manage_users", "manage_system"],
                    "created_at": datetime.now(),
                    "status": "active"
                }
            ]
            
            for rol in roles_iniciales:
                if not roles_collection.find_one({"role_id": rol["role_id"]}):
                    roles_collection.insert_one(rol)
                    print(f"   ✅ Rol inicial creado: {rol['name']}")
            
            # 4. USERS - Documentos flexibles (actualizado para usar role_id)
            users_collection = self.db.users
            users_collection.create_index("user_id", unique=True)
            users_collection.create_index("email", unique=True)
            users_collection.create_index("role_id")  # Nuevo índice para role_id
            users_collection.create_index("role")     # Mantener para compatibilidad durante migración
            users_collection.create_index("status")
            print("   ✅ Colección 'users' configurada con soporte para role_id")
            
            # 5. INVOICES - Transacciones ACID
            invoices_collection = self.db.invoices
            invoices_collection.create_index("invoice_id", unique=True)
            invoices_collection.create_index("user_id")
            invoices_collection.create_index("status")
            invoices_collection.create_index("created_at")
            print("   ✅ Colección 'invoices' configurada para transacciones ACID")
            
            # 6. PAYMENTS - Transacciones ACID
            payments_collection = self.db.payments
            payments_collection.create_index("payment_id", unique=True)
            payments_collection.create_index("invoice_id")
            payments_collection.create_index("user_id")
            payments_collection.create_index("created_at")
            print("   ✅ Colección 'payments' configurada para transacciones ACID")
            
            # 7. ACCOUNTS - Cuentas corrientes
            accounts_collection = self.db.accounts
            accounts_collection.create_index("account_id", unique=True)
            accounts_collection.create_index("user_id", unique=True)
            accounts_collection.create_index("balance")
            print("   ✅ Colección 'accounts' configurada")
            
            # 8. ALERTS - Sistema de alertas
            alerts_collection = self.db.alerts
            alerts_collection.create_index("alert_id", unique=True)
            alerts_collection.create_index("sensor_id")
            alerts_collection.create_index("type")
            alerts_collection.create_index("severity")
            alerts_collection.create_index("status")
            alerts_collection.create_index("created_at")
            print("   ✅ Colección 'alerts' configurada")
            
            return True
            
        except Exception as e:
            print(f"❌ Error configurando colecciones: {e}")
            return False
    
    def poblar_datos_optimizados(self):
        """Poblar con datos optimizados según la arquitectura"""
        if not self.conectado:
            return False
        
        try:
            print("📊 Poblando datos optimizados...")
            
            # 1. SENSORES con metadatos completos
            sensores_ejemplo = [
                {
                    "sensor_id": "SENSOR_BA_001",
                    "name": "Sensor Buenos Aires Centro",
                    "type": "temperature",
                    "location": {
                        "city": "Buenos Aires",
                        "country": "Argentina",
                        "latitude": -34.6037,
                        "longitude": -58.3816,
                        "zone": "Centro",
                        "address": "Av. Corrientes 1000"
                    },
                    "status": "active",
                    "metadata": {
                        "manufacturer": "SensorTech",
                        "model": "ST-2024",
                        "serial_number": "ST2024BA001",
                        "calibration_date": "2024-01-15",
                        "last_maintenance": "2024-10-01",
                        "battery_level": 95,
                        "signal_strength": 98
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "sensor_id": "SENSOR_CBA_001",
                    "name": "Sensor Córdoba Norte",
                    "type": "temperature",
                    "location": {
                        "city": "Córdoba",
                        "country": "Argentina",
                        "latitude": -31.4201,
                        "longitude": -64.1888,
                        "zone": "Norte",
                        "address": "Av. Colón 2000"
                    },
                    "status": "active",
                    "metadata": {
                        "manufacturer": "SensorTech",
                        "model": "ST-2024",
                        "serial_number": "ST2024CBA001",
                        "calibration_date": "2024-02-10",
                        "last_maintenance": "2024-09-15",
                        "battery_level": 88,
                        "signal_strength": 92
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "sensor_id": "SENSOR_MEN_001",
                    "name": "Sensor Mendoza Este",
                    "type": "temperature",
                    "location": {
                        "city": "Mendoza",
                        "country": "Argentina",
                        "latitude": -32.8908,
                        "longitude": -68.8272,
                        "zone": "Este",
                        "address": "Av. San Martín 3000"
                    },
                    "status": "active",
                    "metadata": {
                        "manufacturer": "SensorTech",
                        "model": "ST-2024",
                        "serial_number": "ST2024MEN001",
                        "calibration_date": "2024-03-05",
                        "last_maintenance": "2024-08-20",
                        "battery_level": 92,
                        "signal_strength": 95
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "sensor_id": "SENSOR_ROS_001",
                    "name": "Sensor Rosario Sur",
                    "type": "temperature",
                    "location": {
                        "city": "Rosario",
                        "country": "Argentina",
                        "latitude": -32.9442,
                        "longitude": -60.6505,
                        "zone": "Sur",
                        "address": "Av. Pellegrini 4000"
                    },
                    "status": "active",
                    "metadata": {
                        "manufacturer": "SensorTech",
                        "model": "ST-2024",
                        "serial_number": "ST2024ROS001",
                        "calibration_date": "2024-04-12",
                        "last_maintenance": "2024-07-10",
                        "battery_level": 85,
                        "signal_strength": 90
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "sensor_id": "SENSOR_TUC_001",
                    "name": "Sensor Tucumán Oeste",
                    "type": "temperature",
                    "location": {
                        "city": "San Miguel de Tucumán",
                        "country": "Argentina",
                        "latitude": -26.8083,
                        "longitude": -65.2176,
                        "zone": "Oeste",
                        "address": "Av. Sarmiento 5000"
                    },
                    "status": "active",
                    "metadata": {
                        "manufacturer": "SensorTech",
                        "model": "ST-2024",
                        "serial_number": "ST2024TUC001",
                        "calibration_date": "2024-05-20",
                        "last_maintenance": "2024-06-25",
                        "battery_level": 90,
                        "signal_strength": 93
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
            ]
            
            for sensor in sensores_ejemplo:
                self.db.sensors.insert_one(sensor)
                print(f"   ✅ Sensor {sensor['name']} insertado")
            
            # 2. MEDICIONES optimizadas para Time Series
            print("\n📈 Insertando mediciones optimizadas (Time Series)...")
            mediciones_insertadas = 0
            
            for sensor in sensores_ejemplo:
                sensor_id = sensor['sensor_id']
                ciudad = sensor['location']['city']
                
                # Generar mediciones de los últimos 30 días
                fecha_inicio = datetime.now() - timedelta(days=30)
                
                for i in range(720):  # 720 horas = 30 días
                    timestamp = fecha_inicio + timedelta(hours=i)
                    
                    # Simular datos realistas según la ciudad
                    temperatura_base = {
                        'Buenos Aires': 22.0,
                        'Córdoba': 18.0,
                        'Mendoza': 16.0,
                        'Rosario': 20.0,
                        'San Miguel de Tucumán': 24.0
                    }.get(ciudad, 20.0)
                    
                    # Variación diaria y estacional
                    hora = timestamp.hour
                    dia_año = timestamp.timetuple().tm_yday
                    
                    # Variación diaria (más frío en la madrugada, más caliente al mediodía)
                    variacion_diaria = 8 * math.sin((hora - 6) * math.pi / 12)
                    
                    # Variación estacional (más caliente en verano)
                    variacion_estacional = 5 * math.sin((dia_año - 80) * 2 * math.pi / 365)
                    
                    temperatura = round(temperatura_base + variacion_diaria + variacion_estacional + random.uniform(-2, 2), 2)
                    humedad = round(random.uniform(40, 80), 2)
                    
                    medicion = {
                        "sensor_id": sensor_id,
                        "timestamp": timestamp,
                        "temperature": temperatura,
                        "humidity": humedad,
                        "quality": "good",
                        "metadata": {
                            "battery_level": random.uniform(80, 100),
                            "signal_strength": random.uniform(85, 100),
                            "data_quality_score": random.uniform(0.85, 1.0)
                        }
                    }
                    
                    self.db.measurements.insert_one(medicion)
                    mediciones_insertadas += 1
                    
                    if mediciones_insertadas % 1000 == 0:
                        print(f"   📊 {mediciones_insertadas} mediciones insertadas...")
            
            print(f"   ✅ {mediciones_insertadas} mediciones insertadas")
            
            # 3. USUARIOS con roles y perfiles
            usuarios_ejemplo = [
                {
                    "user_id": "USER_ADMIN_001",
                    "email": "admin@sensors.com",
                    "password_hash": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4Z8Z8Z8Z8Z",
                    "full_name": "Administrador Sistema",
                    "role": "admin",
                    "status": "active",
                    "profile": {
                        "department": "IT",
                        "phone": "+54 11 1234-5678",
                        "preferences": {
                            "language": "es",
                            "notifications": True,
                            "theme": "dark",
                            "timezone": "America/Argentina/Buenos_Aires"
                        },
                        "permissions": ["read", "write", "delete", "admin"]
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "user_id": "USER_TECH_001",
                    "email": "tecnico@sensors.com",
                    "password_hash": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4Z8Z8Z8Z8Z",
                    "full_name": "Técnico Mantenimiento",
                    "role": "technician",
                    "status": "active",
                    "profile": {
                        "department": "Maintenance",
                        "phone": "+54 11 2345-6789",
                        "specializations": ["temperature", "humidity", "calibration"],
                        "certifications": ["SensorTech Certified", "IoT Maintenance"],
                        "work_zones": ["Buenos Aires", "Córdoba", "Mendoza"]
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "user_id": "USER_CLIENT_001",
                    "email": "cliente@sensors.com",
                    "password_hash": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4Z8Z8Z8Z8Z",
                    "full_name": "Cliente Empresarial",
                    "role": "client",
                    "status": "active",
                    "profile": {
                        "company": "Empresa ABC S.A.",
                        "phone": "+54 11 3456-7890",
                        "subscription": "premium",
                        "billing_address": "Av. Corrientes 1234, Buenos Aires",
                        "preferences": {
                            "reports_frequency": "daily",
                            "alert_thresholds": {
                                "temperature_high": 35.0,
                                "temperature_low": 5.0,
                                "humidity_high": 90.0,
                                "humidity_low": 30.0
                            }
                        }
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "user_id": "USER_ANALYST_001",
                    "email": "analista@sensors.com",
                    "password_hash": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4Z8Z8Z8Z8Z",
                    "full_name": "Analista de Datos",
                    "role": "analyst",
                    "status": "active",
                    "profile": {
                        "department": "Analytics",
                        "phone": "+54 11 4567-8901",
                        "specializations": ["data_analysis", "machine_learning", "forecasting"],
                        "tools": ["Python", "R", "Tableau", "Power BI"],
                        "access_level": "read_write"
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
            ]
            
            for usuario in usuarios_ejemplo:
                self.db.users.insert_one(usuario)
                print(f"   ✅ Usuario {usuario['full_name']} insertado")
            
            # 4. CUENTAS CORRIENTES
            cuentas_ejemplo = [
                {
                    "account_id": "ACC_ADMIN_001",
                    "user_id": "USER_ADMIN_001",
                    "balance": 0.0,
                    "currency": "ARS",
                    "status": "active",
                    "transactions": [],
                    "limits": {
                        "daily_limit": 100000.0,
                        "monthly_limit": 1000000.0
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "account_id": "ACC_CLIENT_001",
                    "user_id": "USER_CLIENT_001",
                    "balance": 5000.0,
                    "currency": "ARS",
                    "status": "active",
                    "transactions": [],
                    "limits": {
                        "daily_limit": 5000.0,
                        "monthly_limit": 50000.0
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                {
                    "account_id": "ACC_ANALYST_001",
                    "user_id": "USER_ANALYST_001",
                    "balance": 2000.0,
                    "currency": "ARS",
                    "status": "active",
                    "transactions": [],
                    "limits": {
                        "daily_limit": 2000.0,
                        "monthly_limit": 20000.0
                    },
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
            ]
            
            for cuenta in cuentas_ejemplo:
                self.db.accounts.insert_one(cuenta)
                print(f"   ✅ Cuenta {cuenta['account_id']} insertada")
            
            # 5. FACTURAS con items detallados
            facturas_ejemplo = [
                {
                    "invoice_id": "INV_2024_001",
                    "user_id": "USER_CLIENT_001",
                    "amount": 1250.0,
                    "currency": "ARS",
                    "status": "pending",
                    "period": "Octubre 2024",
                    "period_start": "2024-10-01",
                    "period_end": "2024-10-31",
                    "items": [
                        {
                            "description": "Servicio de monitoreo sensores",
                            "quantity": 1,
                            "unit_price": 1000.0,
                            "total": 1000.0,
                            "category": "monitoring"
                        },
                        {
                            "description": "Reportes avanzados",
                            "quantity": 1,
                            "unit_price": 250.0,
                            "total": 250.0,
                            "category": "reports"
                        }
                    ],
                    "taxes": {
                        "iva": 262.5,
                        "total_with_tax": 1512.5
                    },
                    "created_at": datetime.now(),
                    "due_date": datetime.now() + timedelta(days=30)
                },
                {
                    "invoice_id": "INV_2024_002",
                    "user_id": "USER_ANALYST_001",
                    "amount": 800.0,
                    "currency": "ARS",
                    "status": "paid",
                    "period": "Septiembre 2024",
                    "period_start": "2024-09-01",
                    "period_end": "2024-09-30",
                    "items": [
                        {
                            "description": "Acceso a datos históricos",
                            "quantity": 1,
                            "unit_price": 500.0,
                            "total": 500.0,
                            "category": "data_access"
                        },
                        {
                            "description": "Herramientas de análisis",
                            "quantity": 1,
                            "unit_price": 300.0,
                            "total": 300.0,
                            "category": "tools"
                        }
                    ],
                    "taxes": {
                        "iva": 168.0,
                        "total_with_tax": 968.0
                    },
                    "created_at": datetime.now() - timedelta(days=15),
                    "due_date": datetime.now() - timedelta(days=5),
                    "paid_at": datetime.now() - timedelta(days=3)
                }
            ]
            
            for factura in facturas_ejemplo:
                self.db.invoices.insert_one(factura)
                print(f"   ✅ Factura {factura['invoice_id']} insertada")
            
            # 6. PAGOS
            pagos_ejemplo = [
                {
                    "payment_id": "PAY_2024_001",
                    "invoice_id": "INV_2024_002",
                    "user_id": "USER_ANALYST_001",
                    "amount": 968.0,
                    "currency": "ARS",
                    "method": "credit_card",
                    "status": "completed",
                    "transaction_id": "TXN_2024_001",
                    "payment_details": {
                        "card_last_four": "1234",
                        "card_brand": "Visa",
                        "processor": "MercadoPago"
                    },
                    "created_at": datetime.now() - timedelta(days=3)
                }
            ]
            
            for pago in pagos_ejemplo:
                self.db.payments.insert_one(pago)
                print(f"   ✅ Pago {pago['payment_id']} insertado")
            
            # 7. ALERTAS del sistema
            alertas_ejemplo = [
                {
                    "alert_id": "ALERT_TEMP_HIGH_001",
                    "type": "temperature",
                    "sensor_id": "SENSOR_BA_001",
                    "severity": "high",
                    "title": "Temperatura Alta Detectada",
                    "description": "La temperatura superó el umbral de 35°C",
                    "status": "active",
                    "threshold": 35.0,
                    "current_value": 36.2,
                    "triggered_at": datetime.now(),
                    "created_at": datetime.now(),
                    "metadata": {
                        "alert_rule": "temperature_high",
                        "notification_sent": True,
                        "escalation_level": 1
                    }
                },
                {
                    "alert_id": "ALERT_HUMIDITY_LOW_001",
                    "type": "humidity",
                    "sensor_id": "SENSOR_CBA_001",
                    "severity": "medium",
                    "title": "Humedad Baja Detectada",
                    "description": "La humedad descendió por debajo del 40%",
                    "status": "active",
                    "threshold": 40.0,
                    "current_value": 38.5,
                    "triggered_at": datetime.now(),
                    "created_at": datetime.now(),
                    "metadata": {
                        "alert_rule": "humidity_low",
                        "notification_sent": True,
                        "escalation_level": 1
                    }
                },
                {
                    "alert_id": "ALERT_BATTERY_LOW_001",
                    "type": "maintenance",
                    "sensor_id": "SENSOR_ROS_001",
                    "severity": "low",
                    "title": "Batería Baja",
                    "description": "El nivel de batería está por debajo del 20%",
                    "status": "pending",
                    "threshold": 20.0,
                    "current_value": 18.0,
                    "triggered_at": datetime.now() - timedelta(hours=2),
                    "created_at": datetime.now() - timedelta(hours=2),
                    "metadata": {
                        "alert_rule": "battery_low",
                        "notification_sent": False,
                        "escalation_level": 0,
                        "maintenance_scheduled": True
                    }
                }
            ]
            
            for alerta in alertas_ejemplo:
                self.db.alerts.insert_one(alerta)
                print(f"   ✅ Alerta {alerta['title']} insertada")
            
            # Estadísticas finales
            print("\n📊 ESTADÍSTICAS FINALES:")
            print(f"   Sensores: {self.db.sensors.count_documents({})}")
            print(f"   Mediciones: {self.db.measurements.count_documents({})}")
            print(f"   Usuarios: {self.db.users.count_documents({})}")
            print(f"   Cuentas: {self.db.accounts.count_documents({})}")
            print(f"   Facturas: {self.db.invoices.count_documents({})}")
            print(f"   Pagos: {self.db.payments.count_documents({})}")
            print(f"   Alertas: {self.db.alerts.count_documents({})}")
            
            print("\n🎉 ¡MONGODB ATLAS POBLADO CON ARQUITECTURA OPTIMIZADA!")
            print("✅ Time Series Collections para mediciones")
            print("✅ Transacciones ACID para facturación")
            print("✅ Índices optimizados para consultas rápidas")
            print("✅ Documentos flexibles para evolución")
            
            return True
            
        except Exception as e:
            print(f"❌ Error poblando datos: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtener estadísticas de la base de datos"""
        if not self.conectado:
            return {"error": "No conectado"}
        
        try:
            stats = {
                "database": self.database_name,
                "sensores": self.db.sensors.count_documents({}),
                "mediciones": self.db.measurements.count_documents({}),
                "usuarios": self.db.users.count_documents({}),
                "cuentas": self.db.accounts.count_documents({}),
                "facturas": self.db.invoices.count_documents({}),
                "pagos": self.db.payments.count_documents({}),
                "alertas": self.db.alerts.count_documents({}),
                "timestamp": datetime.now().isoformat()
            }
            
            return stats
            
        except Exception as e:
            return {"error": str(e)}
    
    def obtener_estado_conexion(self) -> Dict[str, Any]:
        """Obtener estado de la conexión"""
        return {
            "conectado": self.conectado,
            "database": self.database_name,
            "arquitectura": "optimizada",
            "timestamp": datetime.now().isoformat()
        }
    
    def obtener_configuracion(self) -> Optional[Dict[str, Any]]:
        """Obtener configuración del sistema"""
        if not self.conectado:
            return None
        
        try:
            config_doc = self.db.config.find_one({"_id": "system_config"})
            if config_doc:
                # Remover _id del documento
                config_doc.pop("_id", None)
                return config_doc
            return None
            
        except Exception as e:
            print(f"❌ Error obteniendo configuración: {e}")
            return None
    
    def guardar_configuracion(self, config_data: Dict[str, Any]) -> bool:
        """Guardar configuración del sistema"""
        if not self.conectado:
            return False
        
        try:
            config_data["_id"] = "system_config"
            config_data["updated_at"] = datetime.now().isoformat()
            
            result = self.db.config.replace_one(
                {"_id": "system_config"},
                config_data,
                upsert=True
            )
            
            if result.acknowledged:
                print("✅ Configuración guardada en MongoDB Atlas")
                return True
            return False
            
        except Exception as e:
            print(f"❌ Error guardando configuración: {e}")
            return False
    
    def crear_proceso(self, proceso_data: Dict[str, Any]) -> bool:
        """Crear proceso"""
        if not self.conectado:
            return False
        
        try:
            self.db.processes.insert_one(proceso_data)
            print(f"✅ Proceso {proceso_data.get('process_id')} creado")
            return True
        except Exception as e:
            print(f"❌ Error creando proceso: {e}")
            return False
    
    def obtener_procesos(self, user_id: str = None, status: str = None) -> List[Dict[str, Any]]:
        """Obtener procesos"""
        if not self.conectado:
            return []
        
        try:
            query = {}
            if user_id:
                query["user_id"] = user_id
            if status:
                query["status"] = status
            
            procesos = list(self.db.processes.find(query).sort("created_at", -1))
            
            # Convertir ObjectId a string
            for proceso in procesos:
                proceso["_id"] = str(proceso["_id"])
            
            return procesos
        except Exception as e:
            print(f"❌ Error obteniendo procesos: {e}")
            return []
    
    def actualizar_estado_proceso(self, process_id: str, status: str, progress: int = None, 
                                 result: Dict[str, Any] = None, error: str = None) -> bool:
        """Actualizar estado de proceso"""
        if not self.conectado:
            return False
        
        try:
            update_data = {
                "status": status,
                "updated_at": datetime.now()
            }
            
            if progress is not None:
                update_data["progress"] = progress
            
            if result is not None:
                update_data["result"] = result
                update_data["completed_at"] = datetime.now()
            
            if error is not None:
                update_data["error"] = error
            
            if status == "running":
                update_data["started_at"] = datetime.now()
            
            result = self.db.processes.update_one(
                {"process_id": process_id},
                {"$set": update_data}
            )
            
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Error actualizando proceso: {e}")
            return False
    
    def crear_factura(self, factura_data: Dict[str, Any]) -> bool:
        """Crear factura"""
        if not self.conectado:
            return False
        
        try:
            self.db.invoices.insert_one(factura_data)
            print(f"✅ Factura {factura_data.get('invoice_id')} creada")
            return True
        except Exception as e:
            print(f"❌ Error creando factura: {e}")
            return False
    
    def obtener_facturas(self, user_id: str = None) -> List[Dict[str, Any]]:
        """Obtener facturas"""
        if not self.conectado:
            return []
        
        try:
            query = {}
            if user_id:
                query["user_id"] = user_id
            
            facturas = list(self.db.invoices.find(query).sort("created_at", -1))
            
            # Convertir ObjectId a string
            for factura in facturas:
                factura["_id"] = str(factura["_id"])
            
            return facturas
        except Exception as e:
            print(f"❌ Error obteniendo facturas: {e}")
            return []
    
    def procesar_pago_transaccion(self, factura_data: Dict[str, Any], pago_data: Dict[str, Any]) -> bool:
        """Procesar pago usando transacción ACID con imputación a cuenta corriente"""
        if not self.conectado:
            return False
        
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    # Actualizar factura
                    self.db.invoices.update_one(
                        {"invoice_id": factura_data["invoice_id"]},
                        {"$set": factura_data},
                        session=session
                    )
                    
                    # Crear pago
                    self.db.payments.insert_one(pago_data, session=session)
                    
                    # Actualizar cuenta corriente del usuario
                    user_id = factura_data.get("user_id")
                    amount = factura_data.get("amount", 0)
                    
                    if user_id and amount > 0:
                        # Buscar cuenta corriente del usuario
                        cuenta = self.db.accounts.find_one({"user_id": user_id}, session=session)
                        
                        if cuenta:
                            # Actualizar saldo de la cuenta corriente
                            nuevo_saldo = cuenta.get("current_balance", 0) + amount
                            
                            # Crear movimiento en el historial
                            movimiento = {
                                "transaction_id": pago_data.get("payment_id"),
                                "type": "credit",
                                "amount": amount,
                                "description": f"Pago de factura {factura_data['invoice_id']}",
                                "date": pago_data.get("processed_at"),
                                "balance_after": nuevo_saldo
                            }
                            
                            # Actualizar cuenta corriente
                            self.db.accounts.update_one(
                                {"user_id": user_id},
                                {
                                    "$set": {"current_balance": nuevo_saldo},
                                    "$push": {"movement_history": movimiento}
                                },
                                session=session
                            )
                            
                            print(f"✅ Cuenta corriente actualizada para usuario {user_id}: +${amount}")
                        else:
                            print(f"⚠️ No se encontró cuenta corriente para usuario {user_id}")
                    
                    print(f"✅ Pago {pago_data.get('payment_id')} procesado con transacción ACID")
                    return True
        except Exception as e:
            print(f"❌ Error procesando pago: {e}")
            return False
    
    def obtener_pagos(self, user_id: str = None, invoice_id: str = None) -> List[Dict[str, Any]]:
        """Obtener pagos"""
        if not self.conectado:
            return []
        
        try:
            query = {}
            if user_id:
                query["user_id"] = user_id
            if invoice_id:
                query["invoice_id"] = invoice_id
            
            pagos = list(self.db.payments.find(query).sort("created_at", -1))
            
            # Convertir ObjectId a string
            for pago in pagos:
                pago["_id"] = str(pago["_id"])
            
            return pagos
        except Exception as e:
            print(f"❌ Error obteniendo pagos: {e}")
            return []
    
    def obtener_cuentas_corrientes(self) -> List[Dict[str, Any]]:
        """Obtener cuentas corrientes"""
        if not self.conectado:
            return []
        
        try:
            cuentas = list(self.db.accounts.find())
            
            # Convertir ObjectId a string
            for cuenta in cuentas:
                cuenta["_id"] = str(cuenta["_id"])
            
            return cuentas
        except Exception as e:
            print(f"❌ Error obteniendo cuentas: {e}")
            return []
    
    def actualizar_cuenta_corriente(self, account_id: str, cuenta_data: Dict[str, Any]) -> bool:
        """Actualizar cuenta corriente"""
        if not self.conectado:
            return False
        
        try:
            result = self.db.accounts.update_one(
                {"account_id": account_id},
                {"$set": cuenta_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Error actualizando cuenta: {e}")
            return False
    
    def crear_cuenta_corriente(self, cuenta_data: Dict[str, Any]) -> bool:
        """Crear nueva cuenta corriente"""
        if not self.conectado:
            return False
        
        try:
            # Verificar que no exista ya una cuenta para este usuario
            existing_account = self.db.accounts.find_one({"user_id": cuenta_data.get("user_id")})
            if existing_account:
                print(f"⚠️ Ya existe una cuenta corriente para el usuario {cuenta_data.get('user_id')}")
                return False
            
            # Insertar nueva cuenta corriente
            result = self.db.accounts.insert_one(cuenta_data)
            if result.inserted_id:
                print(f"✅ Cuenta corriente creada para usuario {cuenta_data.get('user_id')}")
                return True
            return False
            
        except Exception as e:
            print(f"❌ Error creando cuenta corriente: {e}")
            return False
    
    def actualizar_factura(self, invoice_id: str, factura_data: Dict[str, Any]) -> bool:
        """Actualizar factura"""
        if not self.conectado:
            return False
        
        try:
            result = self.db.invoices.update_one(
                {"invoice_id": invoice_id},
                {"$set": factura_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Error actualizando factura: {e}")
            return False
    
    def crear_notificacion(self, notificacion_data: Dict[str, Any]) -> bool:
        """Crear notificación"""
        if not self.conectado:
            return False
        
        try:
            self.db.notifications.insert_one(notificacion_data)
            print(f"✅ Notificación {notificacion_data.get('notification_id')} creada")
            return True
        except Exception as e:
            print(f"❌ Error creando notificación: {e}")
            return False
    
    def obtener_notificaciones(self, user_id: str = None) -> List[Dict[str, Any]]:
        """Obtener notificaciones"""
        if not self.conectado:
            return []
        
        try:
            query = {}
            if user_id:
                query["user_id"] = user_id
            
            notificaciones = list(self.db.notifications.find(query).sort("created_at", -1))
            
            # Convertir ObjectId a string
            for notif in notificaciones:
                notif["_id"] = str(notif["_id"])
            
            return notificaciones
        except Exception as e:
            print(f"❌ Error obteniendo notificaciones: {e}")
            return []
    
    def actualizar_notificacion(self, notification_id: str, notificacion_data: Dict[str, Any]) -> bool:
        """Actualizar notificación"""
        if not self.conectado:
            return False
        
        try:
            result = self.db.notifications.update_one(
                {"notification_id": notification_id},
                {"$set": notificacion_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Error actualizando notificación: {e}")
            return False
    
    def actualizar_alerta(self, alert_id: str, alerta_data: Dict[str, Any]) -> bool:
        """Actualizar alerta"""
        if not self.conectado:
            return False
        
        try:
            result = self.db.alerts.update_one(
                {"alert_id": alert_id},
                {"$set": alerta_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Error actualizando alerta: {e}")
            return False
    
    def crear_sensor(self, sensor_data: Dict[str, Any]) -> bool:
        """Crear nuevo sensor"""
        if not self.conectado:
            return False
        
        try:
            # Insertar sensor en la colección sensors
            result = self.db.sensors.insert_one(sensor_data)
            
            if result.inserted_id:
                print(f"✅ Sensor creado exitosamente: {sensor_data.get('name', 'Sin nombre')}")
                print(f"📊 Sensor ID: {result.inserted_id}")
                print(f"📊 Datos del sensor: {sensor_data}")
                return True
            else:
                print(f"❌ Error creando sensor: {sensor_data.get('name', 'Sin nombre')}")
                return False
                
        except Exception as e:
            print(f"❌ Error creando sensor: {e}")
            import traceback
            print(f"❌ Detalles del error: {traceback.format_exc()}")
            return False
    
    def actualizar_sensor(self, sensor_id: str, sensor_data: Dict[str, Any]) -> bool:
        """Actualizar sensor"""
        if not self.conectado:
            return False
        
        try:
            result = self.db.sensors.update_one(
                {"sensor_id": sensor_id},
                {"$set": sensor_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Error actualizando sensor: {e}")
            return False
    
    def obtener_sensores(self) -> List[Dict[str, Any]]:
        """Obtener todos los sensores"""
        if not self.conectado:
            print("❌ MongoDB no conectado para obtener sensores")
            return []
        
        try:
            # Verificar que la colección existe
            collections = self.db.list_collection_names()
            
            if 'sensors' not in collections:
                return []
            
            # Realizar la consulta
            sensores = list(self.db.sensors.find())
            
            # Convertir ObjectId a string
            for sensor in sensores:
                sensor["_id"] = str(sensor["_id"])
            
            return sensores
        except Exception as e:
            print(f"❌ Error obteniendo sensores: {e}")
            import traceback
            print(f"❌ Detalles del error: {traceback.format_exc()}")
            return []
    
    def eliminar_sensor(self, sensor_id: str) -> bool:
        """Eliminar sensor por ID"""
        if not self.conectado:
            print("❌ MongoDB no conectado para eliminar sensor")
            return False
        
        try:
            print(f"🗑️ Eliminando sensor: {sensor_id}")
            
            # Buscar el sensor por sensor_id
            sensor = self.db.sensors.find_one({"sensor_id": sensor_id})
            if not sensor:
                print(f"❌ Sensor no encontrado: {sensor_id}")
                return False
            
            # Eliminar el sensor
            result = self.db.sensors.delete_one({"sensor_id": sensor_id})
            
            if result.deleted_count > 0:
                print(f"✅ Sensor eliminado exitosamente: {sensor_id}")
                print(f"📊 Sensor eliminado: {sensor.get('name', 'Sin nombre')}")
                return True
            else:
                print(f"❌ No se pudo eliminar el sensor: {sensor_id}")
                return False
                
        except Exception as e:
            print(f"❌ Error eliminando sensor: {e}")
            import traceback
            print(f"❌ Detalles del error: {traceback.format_exc()}")
            return False
    
    def crear_medicion(self, medicion_data: Dict[str, Any]) -> bool:
        """Crear nueva medición"""
        if not self.conectado:
            print("❌ MongoDB no conectado para crear medición")
            return False
        
        try:
            print(f"📊 Creando medición para sensor: {medicion_data.get('sensor_id', 'N/A')}")
            
            # Insertar medición en la colección measurements
            result = self.db.measurements.insert_one(medicion_data)
            
            if result.inserted_id:
                print(f"✅ Medición creada exitosamente: {medicion_data.get('sensor_id', 'Sin ID')}")
                print(f"📊 Valor: {medicion_data.get('value', 'N/A')}")
                print(f"📊 Timestamp: {medicion_data.get('timestamp', 'N/A')}")
                return True
            else:
                print(f"❌ Error creando medición: {medicion_data.get('sensor_id', 'Sin ID')}")
                return False
                
        except Exception as e:
            print(f"❌ Error creando medición: {e}")
            import traceback
            print(f"❌ Detalles del error: {traceback.format_exc()}")
            return False
    
    def obtener_usuarios(self) -> List[Dict[str, Any]]:
        """Obtener todos los usuarios"""
        if not self.conectado:
            return []
        
        try:
            usuarios = list(self.db.users.find())
            
            # Convertir ObjectId a string
            for usuario in usuarios:
                usuario["_id"] = str(usuario["_id"])
            
            return usuarios
        except Exception as e:
            print(f"❌ Error obteniendo usuarios: {e}")
            return []
    
    def obtener_usuario_por_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Obtener un usuario por su ID"""
        if not self.conectado:
            return None
        
        try:
            usuario = self.db.users.find_one({"user_id": user_id})
            
            if usuario:
                # Convertir ObjectId a string
                usuario["_id"] = str(usuario["_id"])
                return usuario
            else:
                return None
        except Exception as e:
            print(f"❌ Error obteniendo usuario {user_id}: {e}")
            return None
    
    def obtener_alertas(self) -> List[Dict[str, Any]]:
        """Obtener todas las alertas"""
        if not self.conectado:
            return []
        
        try:
            alertas = list(self.db.alerts.find())
            
            # Convertir ObjectId a string
            for alerta in alertas:
                alerta["_id"] = str(alerta["_id"])
            
            return alertas
        except Exception as e:
            print(f"❌ Error obteniendo alertas: {e}")
            return []
    
    def crear_alerta(self, alerta_data: Dict[str, Any]) -> bool:
        """Crear nueva alerta"""
        if not self.conectado:
            return False
        
        try:
            # Asegurar campos de resolución presentes
            alerta_data.setdefault("resolved_at", None)
            alerta_data.setdefault("resolved_by", None)
            result = self.db.alerts.insert_one(alerta_data)
            if result.inserted_id:
                print(f"✅ Alerta '{alerta_data['alert_id']}' creada correctamente")
                return True
            else:
                print(f"❌ Error creando alerta '{alerta_data['alert_id']}'")
                return False
                
        except Exception as e:
            print(f"❌ Error creando alerta: {e}")
            return False
    
    def resolver_alerta(self, alert_id: str, resolved_by: Optional[str] = None) -> bool:
        """Resolver alerta cambiando su estado, guardando resolved_at/resolved_by"""
        if not self.conectado:
            return False
        
        try:
            result = self.db.alerts.update_one(
                {"alert_id": alert_id},
                {"$set": {"status": "resolved", "resolved_at": datetime.now().isoformat(), "resolved_by": resolved_by}}
            )
            
            if result.modified_count > 0:
                print(f"✅ Alerta '{alert_id}' resuelta correctamente")
                return True
            else:
                print(f"❌ Alerta '{alert_id}' no encontrada")
                return False
                
        except Exception as e:
            print(f"❌ Error resolviendo alerta: {e}")
            return False

    # --- Controles de Funcionamiento ---
    def crear_control(self, control_data: Dict[str, Any]) -> bool:
        """Crear registro de control de funcionamiento del sensor"""
        if not self.conectado:
            return False
        try:
            result = self.db.controls.insert_one(control_data)
            return result.inserted_id is not None
        except Exception as e:
            print(f"❌ Error creando control: {e}")
            return False

    def obtener_controles(self, sensor_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Obtener controles de funcionamiento (opcional filtrar por sensor)"""
        if not self.conectado:
            return []
        try:
            query = {"sensor_id": sensor_id} if sensor_id else {}
            controles = list(self.db.controls.find(query).sort("reviewed_at", -1))
            for c in controles:
                c["_id"] = str(c["_id"])
            return controles
        except Exception as e:
            print(f"❌ Error obteniendo controles: {e}")
            return []

    # --- Gestión de Roles ---
    def crear_rol(self, rol_data: Dict[str, Any]) -> bool:
        """Crear nuevo rol"""
        if not self.conectado:
            return False
        try:
            # Verificar que role_id y name no existan
            if self.db.roles.find_one({"$or": [{"role_id": rol_data.get("role_id")}, {"name": rol_data.get("name")}]}):
                print(f"❌ Rol con role_id '{rol_data.get('role_id')}' o name '{rol_data.get('name')}' ya existe")
                return False
            
            result = self.db.roles.insert_one(rol_data)
            if result.inserted_id:
                print(f"✅ Rol '{rol_data.get('name')}' creado correctamente")
                return True
            return False
        except Exception as e:
            print(f"❌ Error creando rol: {e}")
            return False

    def obtener_roles(self) -> List[Dict[str, Any]]:
        """Obtener todos los roles activos"""
        if not self.conectado:
            return []
        try:
            roles = list(self.db.roles.find({"status": "active"}).sort("name", 1))
            for r in roles:
                r["_id"] = str(r["_id"])
            return roles
        except Exception as e:
            print(f"❌ Error obteniendo roles: {e}")
            return []

    def obtener_rol_por_id(self, role_id: str) -> Optional[Dict[str, Any]]:
        """Obtener rol por role_id"""
        if not self.conectado:
            return None
        try:
            rol = self.db.roles.find_one({"role_id": role_id})
            if rol:
                rol["_id"] = str(rol["_id"])
            return rol
        except Exception as e:
            print(f"❌ Error obteniendo rol {role_id}: {e}")
            return None

    def obtener_rol_por_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Obtener rol por name (nombre del rol)"""
        if not self.conectado:
            return None
        try:
            rol = self.db.roles.find_one({"name": name, "status": "active"})
            if rol:
                rol["_id"] = str(rol["_id"])
            return rol
        except Exception as e:
            print(f"❌ Error obteniendo rol por nombre {name}: {e}")
            return None

    def actualizar_rol(self, role_id: str, rol_data: Dict[str, Any]) -> bool:
        """Actualizar rol existente"""
        if not self.conectado:
            return False
        try:
            rol_data["updated_at"] = datetime.now()
            result = self.db.roles.update_one(
                {"role_id": role_id},
                {"$set": rol_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"❌ Error actualizando rol: {e}")
            return False

    def migrar_usuarios_a_role_id(self) -> Dict[str, int]:
        """Migrar usuarios existentes de campo 'rol' (string) a 'role_id' (referencia)"""
        if not self.conectado:
            return {"success": 0, "errors": 0, "skipped": 0}
        
        try:
            print("🔄 Iniciando migración de usuarios a role_id...")
            
            # Mapeo de valores antiguos de 'rol' a role_id
            mapeo_roles = {
                "usuario": "ROL_USUARIO_001",
                "técnico": "ROL_TECNICO_001",
                "administrador": "ROL_ADMIN_001",
                "admin": "ROL_ADMIN_001",
                "tech": "ROL_TECNICO_001",
                "user": "ROL_USUARIO_001"
            }
            
            usuarios = list(self.db.users.find({}))
            stats = {"success": 0, "errors": 0, "skipped": 0}
            
            for usuario in usuarios:
                try:
                    user_id = usuario.get("user_id")
                    rol_antiguo = usuario.get("rol")
                    role_id_actual = usuario.get("role_id")
                    
                    # Si ya tiene role_id, saltar
                    if role_id_actual:
                        stats["skipped"] += 1
                        continue
                    
                    # Buscar role_id correspondiente
                    if rol_antiguo and rol_antiguo in mapeo_roles:
                        nuevo_role_id = mapeo_roles[rol_antiguo]
                        
                        # Verificar que el rol existe en la colección roles
                        rol_existe = self.db.roles.find_one({"role_id": nuevo_role_id})
                        if not rol_existe:
                            print(f"⚠️ Rol {nuevo_role_id} no existe para usuario {user_id}, saltando...")
                            stats["errors"] += 1
                            continue
                        
                        # Actualizar usuario: agregar role_id manteniendo rol para compatibilidad
                        self.db.users.update_one(
                            {"user_id": user_id},
                            {"$set": {"role_id": nuevo_role_id}}
                        )
                        stats["success"] += 1
                        print(f"✅ Usuario {user_id} migrado: '{rol_antiguo}' → {nuevo_role_id}")
                    else:
                        # Si no hay mapeo, asignar rol de usuario por defecto
                        default_role_id = "ROL_USUARIO_001"
                        self.db.users.update_one(
                            {"user_id": user_id},
                            {"$set": {"role_id": default_role_id, "rol": "usuario"}}
                        )
                        stats["success"] += 1
                        print(f"⚠️ Usuario {user_id} sin rol válido, asignado: {default_role_id}")
                        
                except Exception as e:
                    print(f"❌ Error migrando usuario {usuario.get('user_id', 'N/A')}: {e}")
                    stats["errors"] += 1
            
            print(f"✅ Migración completada: {stats['success']} exitosos, {stats['skipped']} omitidos, {stats['errors']} errores")
            return stats
            
        except Exception as e:
            print(f"❌ Error en migración: {e}")
            return {"success": 0, "errors": 1, "skipped": 0}
    
    def eliminar_alerta(self, alert_id: str) -> bool:
        """Eliminar alerta"""
        if not self.conectado:
            return False
        
        try:
            result = self.db.alerts.delete_one({"alert_id": alert_id})
            
            if result.deleted_count > 0:
                print(f"✅ Alerta '{alert_id}' eliminada correctamente")
                return True
            else:
                print(f"❌ Alerta '{alert_id}' no encontrada")
                return False
                
        except Exception as e:
            print(f"❌ Error eliminando alerta: {e}")
            return False
    
    def guardar_umbrales_sensor(self, sensor_id: str, umbrales_data: Dict[str, Any]) -> bool:
        """Guardar umbrales específicos para un sensor"""
        try:
            if not self.conectado:
                return False
            
            # Preparar datos de umbrales
            umbrales_doc = {
                "sensor_id": sensor_id,
                "thresholds": umbrales_data,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "created_by": umbrales_data.get("created_by", "system"),
                "status": "active"
            }
            
            # Usar upsert para actualizar o crear
            result = self.db.sensor_thresholds.replace_one(
                {"sensor_id": sensor_id},
                umbrales_doc,
                upsert=True
            )
            
            if result.acknowledged:
                print(f"✅ Umbrales guardados para sensor {sensor_id}")
                return True
            return False
            
        except Exception as e:
            print(f"❌ Error guardando umbrales del sensor {sensor_id}: {e}")
            return False
    
    def obtener_umbrales_sensor(self, sensor_id: str) -> Optional[Dict[str, Any]]:
        """Obtener umbrales específicos de un sensor"""
        try:
            if not self.conectado:
                return None
            
            umbrales_doc = self.db.sensor_thresholds.find_one({"sensor_id": sensor_id})
            
            if umbrales_doc:
                # Convertir ObjectId a string
                umbrales_doc["_id"] = str(umbrales_doc["_id"])
                return umbrales_doc
            return None
            
        except Exception as e:
            print(f"❌ Error obteniendo umbrales del sensor {sensor_id}: {e}")
            return None
    
    def obtener_umbrales_globales(self) -> Optional[Dict[str, Any]]:
        """Obtener umbrales globales del sistema"""
        try:
            if not self.conectado:
                return None
            
            umbrales_doc = self.db.system_config.find_one({"type": "global_thresholds"})
            
            if umbrales_doc:
                # Convertir ObjectId a string
                umbrales_doc["_id"] = str(umbrales_doc["_id"])
                return umbrales_doc
            return None
            
        except Exception as e:
            print(f"❌ Error obteniendo umbrales globales: {e}")
            return None
    
    def guardar_umbrales_globales(self, umbrales_data: Dict[str, Any]) -> bool:
        """Guardar umbrales globales del sistema"""
        try:
            if not self.conectado:
                return False
            
            # Preparar datos de umbrales globales
            umbrales_doc = {
                "type": "global_thresholds",
                "thresholds": umbrales_data,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "created_by": umbrales_data.get("created_by", "system"),
                "status": "active"
            }
            
            # Usar upsert para actualizar o crear
            result = self.db.system_config.replace_one(
                {"type": "global_thresholds"},
                umbrales_doc,
                upsert=True
            )
            
            if result.acknowledged:
                print("✅ Umbrales globales guardados")
                return True
            return False
            
        except Exception as e:
            print(f"❌ Error guardando umbrales globales: {e}")
            return False
    
    def registrar_cambio_umbral(self, sensor_id: str, cambio_data: Dict[str, Any]) -> bool:
        """Registrar cambio en umbrales para historial"""
        try:
            if not self.conectado:
                return False
            
            # Preparar datos del cambio
            cambio_doc = {
                "sensor_id": sensor_id,
                "change_type": cambio_data.get("change_type", "threshold_update"),
                "old_values": cambio_data.get("old_values", {}),
                "new_values": cambio_data.get("new_values", {}),
                "changed_by": cambio_data.get("changed_by", "system"),
                "change_reason": cambio_data.get("change_reason", "Manual update"),
                "timestamp": datetime.now().isoformat(),
                "ip_address": cambio_data.get("ip_address", "unknown"),
                "user_agent": cambio_data.get("user_agent", "unknown")
            }
            
            # Insertar en colección de historial
            result = self.db.threshold_history.insert_one(cambio_doc)
            
            if result.inserted_id:
                print(f"✅ Cambio de umbral registrado para sensor {sensor_id}")
                return True
            return False
            
        except Exception as e:
            print(f"❌ Error registrando cambio de umbral: {e}")
            return False
    
    def obtener_historial_umbrales(self, sensor_id: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Obtener historial de cambios de umbrales"""
        try:
            if not self.conectado:
                return []
            
            # Construir query
            query = {}
            if sensor_id:
                query["sensor_id"] = sensor_id
            
            # Obtener historial ordenado por timestamp descendente
            historial = list(self.db.threshold_history.find(query)
                           .sort("timestamp", -1)
                           .limit(limit))
            
            # Convertir ObjectId a string
            for cambio in historial:
                cambio["_id"] = str(cambio["_id"])
            
            return historial
            
        except Exception as e:
            print(f"❌ Error obteniendo historial de umbrales: {e}")
            return []
    
    def obtener_umbrales_efectivos(self, sensor_id: str) -> Dict[str, Any]:
        """Obtener umbrales efectivos para un sensor (específicos o globales)"""
        try:
            if not self.conectado:
                return {}
            
            # Primero intentar obtener umbrales específicos del sensor
            umbrales_sensor = self.obtener_umbrales_sensor(sensor_id)
            
            if umbrales_sensor and umbrales_sensor.get("thresholds"):
                return umbrales_sensor["thresholds"]
            
            # Si no hay umbrales específicos, usar los globales
            umbrales_globales = self.obtener_umbrales_globales()
            
            if umbrales_globales and umbrales_globales.get("thresholds"):
                return umbrales_globales["thresholds"]
            
            # Si no hay ninguno, usar valores por defecto
            return {
                "Temperatura": {"min": 15, "max": 35},
                "Humedad": {"min": 30, "max": 80}
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo umbrales efectivos para sensor {sensor_id}: {e}")
            return {}
    
    def guardar_umbrales_ubicacion(self, ciudad: str, pais: str, umbrales_data: Dict[str, Any]) -> bool:
        """Guardar umbrales para una ubicación específica (ciudad, país)"""
        try:
            if not self.conectado:
                return False
            
            # Preparar datos de umbrales por ubicación
            umbrales_doc = {
                "ciudad": ciudad,
                "pais": pais,
                "ubicacion": f"{ciudad}, {pais}",
                "thresholds": umbrales_data,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            # Usar upsert para actualizar o crear
            result = self.db.location_thresholds.update_one(
                {"ciudad": ciudad, "pais": pais},
                {"$set": umbrales_doc},
                upsert=True
            )
            
            if result.acknowledged:
                print(f"✅ Umbrales guardados para {ciudad}, {pais}")
                return True
            else:
                print(f"❌ Error guardando umbrales para {ciudad}, {pais}")
                return False
                
        except Exception as e:
            print(f"❌ Error guardando umbrales para {ciudad}, {pais}: {e}")
            return False
    
    def obtener_umbrales_ubicacion(self, ciudad: str, pais: str) -> Optional[Dict[str, Any]]:
        """Obtener umbrales para una ubicación específica"""
        try:
            if not self.conectado:
                return None
            
            umbrales_doc = self.db.location_thresholds.find_one({
                "ciudad": ciudad, 
                "pais": pais
            })
            
            if umbrales_doc:
                # Convertir ObjectId a string
                umbrales_doc["_id"] = str(umbrales_doc["_id"])
                return umbrales_doc.get("thresholds", {})
            
            return None
            
        except Exception as e:
            print(f"❌ Error obteniendo umbrales para {ciudad}, {pais}: {e}")
            return None
    
    def obtener_umbrales_efectivos_por_ubicacion(self, sensor_id: str) -> Dict[str, Any]:
        """Obtener umbrales efectivos para un sensor basados en su ubicación"""
        try:
            if not self.conectado:
                return {}
            
            # Obtener información del sensor para conocer su ubicación
            sensor = self.db.sensors.find_one({"sensor_id": sensor_id})
            if not sensor:
                return {}
            
            location = sensor.get("location", {})
            if isinstance(location, dict):
                ciudad = location.get("city", "")
                pais = location.get("country", "")
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
                    return {}
            else:
                return {}
            
            if not ciudad or not pais:
                return {}
            
            # Intentar obtener umbrales específicos de la ubicación
            umbrales_ubicacion = self.obtener_umbrales_ubicacion(ciudad, pais)
            
            if umbrales_ubicacion:
                return umbrales_ubicacion
            
            # Si no hay umbrales específicos de ubicación, usar globales
            umbrales_globales = self.obtener_umbrales_globales()
            if umbrales_globales:
                return umbrales_globales.get("thresholds", {})
            
            # Valores por defecto si no hay configuración
            return {
                "Temperatura": {"min": 5, "max": 35},
                "Humedad": {"min": 30, "max": 80}
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo umbrales efectivos por ubicación para {sensor_id}: {e}")
            return {}

    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtener estadísticas del sistema"""
        if not self.conectado:
            return {}
        
        try:
            stats = {
                "sensores": self.db.sensors.count_documents({}),
                "usuarios": self.db.users.count_documents({}),
                "alertas": self.db.alerts.count_documents({}),
                "mediciones": self.db.measurements.count_documents({}),
                "facturas": self.db.invoices.count_documents({}),
                "pagos": self.db.payments.count_documents({}),
                "procesos": self.db.processes.count_documents({}),
                "notificaciones": self.db.notifications.count_documents({})
            }
            
            return stats
        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def crear_usuario(self, usuario_data: Dict[str, Any]) -> bool:
        """Crear nuevo usuario"""
        if not self.conectado:
            return False
        
        try:
            # Verificar que el usuario no exista
            existing_user = self.db.users.find_one({"username": usuario_data["username"]})
            if existing_user:
                print(f"❌ Usuario '{usuario_data['username']}' ya existe")
                return False
            
            # Insertar usuario
            result = self.db.users.insert_one(usuario_data)
            if result.inserted_id:
                print(f"✅ Usuario '{usuario_data['username']}' creado correctamente")
                return True
            else:
                print(f"❌ Error creando usuario '{usuario_data['username']}'")
                return False
                
        except Exception as e:
            print(f"❌ Error creando usuario: {e}")
            return False
    
    def actualizar_usuario(self, user_id: str, usuario_data: Dict[str, Any]) -> bool:
        """Actualizar usuario existente"""
        if not self.conectado:
            return False
        
        try:
            # Actualizar usuario
            result = self.db.users.update_one(
                {"user_id": user_id},
                {"$set": usuario_data}
            )
            
            if result.modified_count > 0:
                print(f"✅ Usuario '{user_id}' actualizado correctamente")
                return True
            else:
                print(f"❌ Usuario '{user_id}' no encontrado o sin cambios")
                return False
                
        except Exception as e:
            print(f"❌ Error actualizando usuario: {e}")
            return False
    
    def eliminar_usuario(self, user_id: str) -> bool:
        """Eliminar usuario"""
        if not self.conectado:
            return False
        
        try:
            # Eliminar usuario
            result = self.db.users.delete_one({"user_id": user_id})
            
            if result.deleted_count > 0:
                print(f"✅ Usuario '{user_id}' eliminado correctamente")
                return True
            else:
                print(f"❌ Usuario '{user_id}' no encontrado")
                return False
                
        except Exception as e:
            print(f"❌ Error eliminando usuario: {e}")
            return False
    
    def autenticar_usuario(self, username: str, password: str) -> Dict[str, Any]:
        """Autenticar usuario por username y password"""
        if not self.conectado:
            return None
        
        try:
            # Buscar usuario por username y password
            usuario = self.db.users.find_one({
                "username": username,
                "password": password,
                "status": "activo"
            })
            
            if usuario:
                # Convertir ObjectId a string
                usuario["_id"] = str(usuario["_id"])
                print(f"✅ Usuario '{username}' autenticado correctamente")
                return usuario
            else:
                print(f"❌ Usuario '{username}' no encontrado o credenciales incorrectas")
                return None
                
        except Exception as e:
            print(f"❌ Error autenticando usuario: {e}")
            return None
    
    def crear_proceso(self, proceso_data: Dict[str, Any]) -> bool:
        """Crear nuevo proceso"""
        try:
            if not self.conectado:
                return False
            
            collection = self.db["processes"]
            resultado = collection.insert_one(proceso_data)
            return resultado.inserted_id is not None
            
        except Exception as e:
            print(f"Error creando proceso: {e}")
            return False
    
    def obtener_procesos_usuario(self, user_id: str) -> List[Dict[str, Any]]:
        """Obtener procesos de un usuario específico"""
        try:
            if not self.conectado:
                return []
            
            collection = self.db["processes"]
            procesos = list(collection.find({"user_id": user_id}))
            return procesos
            
        except Exception as e:
            print(f"Error obteniendo procesos del usuario: {e}")
            return []
    
    def actualizar_proceso(self, process_id: str, update_data: Dict[str, Any]) -> bool:
        """Actualizar proceso existente"""
        try:
            if not self.conectado:
                return False
            
            collection = self.db["processes"]
            resultado = collection.update_one(
                {"process_id": process_id},
                {"$set": update_data}
            )
            return resultado.modified_count > 0
            
        except Exception as e:
            print(f"Error actualizando proceso: {e}")
            return False
    
    def eliminar_proceso(self, process_id: str) -> bool:
        """Eliminar proceso"""
        try:
            if not self.conectado:
                return False
            
            collection = self.db["processes"]
            resultado = collection.delete_one({"process_id": process_id})
            return resultado.deleted_count > 0
            
        except Exception as e:
            print(f"Error eliminando proceso: {e}")
            return False
    
    def crear_ejecucion_proceso(self, ejecucion_data: Dict[str, Any]) -> bool:
        """Crear registro de ejecución de proceso"""
        try:
            if not self.conectado:
                return False
            
            collection = self.db["process_executions"]
            resultado = collection.insert_one(ejecucion_data)
            return resultado.inserted_id is not None
            
        except Exception as e:
            print(f"Error creando ejecución de proceso: {e}")
            return False
    
    def obtener_ejecuciones_usuario(self, user_id: str) -> List[Dict[str, Any]]:
        """Obtener ejecuciones de procesos de un usuario"""
        try:
            if not self.conectado:
                return []
            
            collection = self.db["process_executions"]
            ejecuciones = list(collection.find({"user_id": user_id}))
            
            # Enriquecer con nombre del proceso
            for ejecucion in ejecuciones:
                process_id = ejecucion.get('process_id')
                if process_id:
                    proceso = self.db["processes"].find_one({"process_id": process_id})
                    if proceso:
                        ejecucion['process_name'] = proceso.get('name', 'Proceso')
            
            return ejecuciones
            
        except Exception as e:
            print(f"Error obteniendo ejecuciones del usuario: {e}")
            return []
    
    def obtener_mediciones_rango(self, sensor_name: str, fecha_inicio: str, fecha_fin: str) -> List[Dict[str, Any]]:
        """Obtener mediciones de un sensor en un rango de fechas"""
        try:
            if not self.conectado:
                return []
            
            collection = self.db["measurements"]
            
            # Convertir fechas a datetime
            from datetime import datetime
            fecha_inicio_dt = datetime.fromisoformat(fecha_inicio)
            fecha_fin_dt = datetime.fromisoformat(fecha_fin)
            
            # Consulta con filtros
            query = {
                "sensor_name": sensor_name,
                "timestamp": {
                    "$gte": fecha_inicio_dt.isoformat(),
                    "$lte": fecha_fin_dt.isoformat()
                }
            }
            
            mediciones = list(collection.find(query))
            return mediciones
            
        except Exception as e:
            print(f"Error obteniendo mediciones por rango: {e}")
            return []
    
    def obtener_proceso_por_id(self, process_id: str) -> Dict[str, Any]:
        """Obtener proceso por su ID"""
        try:
            if not self.conectado:
                return None
            
            collection = self.db["processes"]
            proceso = collection.find_one({"process_id": process_id})
            return proceso
            
        except Exception as e:
            print(f"Error obteniendo proceso por ID: {e}")
            return None
    
    def obtener_ejecuciones_usuario_filtradas(self, user_id: str, estado_filtro: str, fecha_desde: str, fecha_hasta: str) -> List[Dict[str, Any]]:
        """Obtener ejecuciones de procesos de un usuario con filtros"""
        try:
            if not self.conectado:
                return []
            
            collection = self.db["process_executions"]
            
            # Construir query con filtros
            query = {"user_id": user_id}
            
            # Filtro por estado
            if estado_filtro != "Todos":
                if estado_filtro == "Completado":
                    query["status"] = "completado"
                elif estado_filtro == "Error":
                    query["status"] = "error"
                elif estado_filtro == "En Progreso":
                    query["status"] = "en_progreso"
            
            # Filtro por fecha
            if fecha_desde and fecha_hasta:
                from datetime import datetime
                fecha_desde_dt = datetime.fromisoformat(fecha_desde)
                fecha_hasta_dt = datetime.fromisoformat(fecha_hasta)
                
                query["executed_at"] = {
                    "$gte": fecha_desde_dt.isoformat(),
                    "$lte": fecha_hasta_dt.isoformat()
                }
            
            ejecuciones = list(collection.find(query).sort("executed_at", -1))
            
            # Enriquecer con nombre del proceso
            for ejecucion in ejecuciones:
                process_id = ejecucion.get('process_id')
                if process_id:
                    proceso = self.db["processes"].find_one({"process_id": process_id})
                    if proceso:
                        ejecucion['process_name'] = proceso.get('name', 'Proceso')
            
            return ejecuciones
            
        except Exception as e:
            print(f"Error obteniendo ejecuciones filtradas: {e}")
            return []
    
    def obtener_ejecucion_por_id(self, execution_id: str) -> Dict[str, Any]:
        """Obtener ejecución por su ID"""
        try:
            if not self.conectado:
                return None
            
            collection = self.db["process_executions"]
            ejecucion = collection.find_one({"execution_id": execution_id})
            return ejecucion
            
        except Exception as e:
            print(f"Error obteniendo ejecución por ID: {e}")
            return None
    
    def limpiar_ejecuciones_antiguas(self, user_id: str, fecha_limite: str) -> int:
        """Limpiar ejecuciones antiguas de un usuario"""
        try:
            if not self.conectado:
                return 0
            
            collection = self.db["process_executions"]
            resultado = collection.delete_many({
                "user_id": user_id,
                "executed_at": {"$lt": fecha_limite}
            })
            
            return resultado.deleted_count
            
        except Exception as e:
            print(f"Error limpiando ejecuciones antiguas: {e}")
            return 0
    
    def obtener_ubicaciones_disponibles(self):
        """Obtener todas las ubicaciones únicas de los sensores"""
        try:
            if not self.conectado:
                return []
            
            collection = self.db["sensors"]
            
            # Obtener ubicaciones únicas usando aggregation
            pipeline = [
                {"$group": {"_id": "$location"}},
                {"$sort": {"_id": 1}}
            ]
            
            ubicaciones = []
            for doc in collection.aggregate(pipeline):
                ubicacion = doc["_id"]
                if ubicacion:
                    ubicaciones.append(ubicacion)
            
            return ubicaciones
            
        except Exception as e:
            print(f"Error obteniendo ubicaciones disponibles: {e}")
            return []
    
    def ejecutar_proceso_analisis(self, tipo_analisis, agrupacion, periodicidad, pais, ciudad, zona=""):
        """
        Ejecuta un proceso de análisis de datos de sensores utilizando el framework de agregación de MongoDB.

        Args:
            tipo_analisis (str): El tipo de datos a analizar ('Humedad', 'Temperatura' o 'Ambas').
            agrupacion (str): El campo por el cual agrupar los resultados ('Ciudad', 'País', 'Zona').
            periodicidad (str): La granularidad temporal para la agregación ('Anual', 'Mensual', 'Diario').
            pais (str): El país para filtrar los datos (opcional).
            ciudad (str): La ciudad para filtrar los datos (opcional).
            zona (str): La zona para filtrar los datos (opcional).

        Returns:
            list: Una lista de diccionarios con los resultados de la agregación.
        """
        if not self.conectado:
            return []

        try:
            pipeline = []

            # ETAPA 1: Primero obtener los sensor_ids que coinciden con la ubicación
            # Usar método similar al módulo de servicios que sí funciona
            sensor_ids = []
            
            if pais and ciudad:
                # Buscar en formatos de string - probar variantes comunes
                sensores_str = list(self.db.sensors.find({
                    "$or": [
                        {"location": ciudad},
                        {"location": f"{ciudad}, {pais}"},
                        {"location": f"{ciudad} - {pais}"},
                        {"location": {"$regex": ciudad, "$options": "i"}},
                        {"location": {"$regex": f"{ciudad}.*{pais}", "$options": "i"}}
                    ]
                }))
                # Buscar en formato objeto
                sensores_obj = list(self.db.sensors.find({
                    "$and": [
                        {"location.city": {"$regex": ciudad, "$options": "i"}},
                        {"location.country": {"$regex": pais, "$options": "i"}}
                    ]
                }))
                
                sensor_ids = [s.get("sensor_id") for s in sensores_str + sensores_obj]
            elif pais:
                sensores = list(self.db.sensors.find({"location": pais}))
                sensores_obj = list(self.db.sensors.find({"location.country": pais}))
                sensor_ids = [s.get("sensor_id") for s in sensores + sensores_obj]
            elif ciudad:
                sensores = list(self.db.sensors.find({"location": ciudad}))
                sensores_obj = list(self.db.sensors.find({"location.city": ciudad}))
                sensor_ids = [s.get("sensor_id") for s in sensores + sensores_obj]
            elif zona:
                sensores = list(self.db.sensors.find({"location": zona}))
                sensores_obj = list(self.db.sensors.find({"location.zone": zona}))
                sensor_ids = [s.get("sensor_id") for s in sensores + sensores_obj]
            else:
                # Si no hay filtros, obtener todos los sensores
                sensor_ids = [s.get("sensor_id") for s in list(self.db.sensors.find({}))]
            
            if not sensor_ids:
                return []
            
            # ETAPA 2: Filtrar por sensor_id en lugar de por location
            pipeline.append({"$match": {"sensor_id": {"$in": sensor_ids}}})

            # Etapa de lookup para unir con la colección de mediciones
            pipeline.append({
                "$lookup": {
                    "from": "measurements",
                    "localField": "sensor_id",
                    "foreignField": "sensor_id",
                    "as": "measurements"
                }
            })

            # Etapa de desenrollado (unwind) para las mediciones
            pipeline.append({"$unwind": "$measurements"})

            # Etapa de agrupación (group)
            group_stage = {
                "_id": {},
                "avg_temp": {"$avg": "$measurements.temperature"},
                "avg_humidity": {"$avg": "$measurements.humidity"}
            }

            # Agrupación por ubicación
            if agrupacion == 'Ciudad':
                group_stage["_id"]["ciudad"] = "$location"
            elif agrupacion == 'País':
                group_stage["_id"]["pais"] = "$location"
            elif agrupacion == 'Zona':
                group_stage["_id"]["zona"] = "$location"

            # Agrupación por periodicidad
            if periodicidad == 'Anual':
                group_stage["_id"]["año"] = {"$year": "$measurements.timestamp"}
            elif periodicidad == 'Mensual':
                group_stage["_id"]["año"] = {"$year": "$measurements.timestamp"}
                group_stage["_id"]["mes"] = {"$month": "$measurements.timestamp"}
            elif periodicidad == 'Diario':
                group_stage["_id"]["año"] = {"$year": "$measurements.timestamp"}
                group_stage["_id"]["mes"] = {"$month": "$measurements.timestamp"}
                group_stage["_id"]["dia"] = {"$dayOfMonth": "$measurements.timestamp"}

            pipeline.append({"$group": group_stage})

            # Etapa de proyección (project) para dar formato a la salida
            project_stage = {
                "_id": 0,
                "agrupacion": "$_id"
            }
            if tipo_analisis == 'Temperatura':
                project_stage["temperatura_promedio"] = "$avg_temp"
            elif tipo_analisis == 'Humedad':
                project_stage["humedad_promedio"] = "$avg_humidity"
            else: # 'Ambos'
                project_stage["temperatura_promedio"] = "$avg_temp"
                project_stage["humedad_promedio"] = "$avg_humidity"
            
            pipeline.append({"$project": project_stage})

            # Ejecutar el pipeline de agregación
            resultados = list(self.db.sensors.aggregate(pipeline))

            return resultados

        except Exception as e:
            print(f"❌ Error ejecutando proceso de análisis: {e}")
            return []

    def obtener_datos_temperatura_por_ubicacion(self, ubicacion, fecha_inicio, fecha_fin):
        """Obtener datos de temperatura para una ubicación específica"""
        try:
            if not self.conectado:
                return []
            
            # Verificar si las fechas ya son objetos datetime o strings
            if isinstance(fecha_inicio, str):
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            else:
                fecha_inicio_dt = fecha_inicio
                
            if isinstance(fecha_fin, str):
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            else:
                fecha_fin_dt = fecha_fin
            
            # Buscar sensores en la ubicación con búsqueda estricta
            sensors_collection = self.db["sensors"]
            
            # Búsqueda flexible que encuentra ciudad en diferentes posiciones
            sensores_encontrados = list(sensors_collection.find({
                "$or": [
                    # Coincidencia exacta como string
                    {"location": ubicacion},
                    # Coincidencia exacta case insensitive
                    {"location": {"$regex": f"^{ubicacion}$", "$options": "i"}},
                    # La ciudad al INICIO seguida de espacio, coma o guión
                    {"location": {"$regex": f"^{ubicacion}[ ,-]", "$options": "i"}},
                    # La ciudad en medio: ", Ciudad -"
                    {"location": {"$regex": f", {ubicacion} -", "$options": "i"}},
                    # País al final (ubicacion es el país)
                    {"location": {"$regex": f".* - {ubicacion}$", "$options": "i"}}
                ]
            }))
            
            # También buscar en formato objeto
            sensores_obj = list(sensors_collection.find({
                "$or": [
                    {"location.city": {"$regex": f"^{ubicacion}$", "$options": "i"}},
                    {"location.country": {"$regex": f"^{ubicacion}$", "$options": "i"}}
                ]
            }))
            
            # Combinar resultados y obtener IDs únicos
            todos_sensores = sensores_encontrados + sensores_obj
            sensor_ids = list(set([sensor["sensor_id"] for sensor in todos_sensores if sensor.get("sensor_id")]))
            
            print(f"🔍 Buscando sensores para ubicación: '{ubicacion}', encontrados: {len(sensor_ids)}")
            
            if not sensor_ids:
                print(f"⚠️ No se encontraron sensores para la ubicación: {ubicacion}")
                return []
            
            # Buscar mediciones de temperatura en el rango de fechas
            measurements_collection = self.db["measurements"]
            
            datos_temperatura = []
            mediciones_totales = 0
            
            for sensor_id in sensor_ids:
                mediciones = measurements_collection.find({
                    "sensor_id": sensor_id,
                    "timestamp": {
                        "$gte": fecha_inicio_dt,
                        "$lte": fecha_fin_dt
                    },
                    "temperature": {"$exists": True, "$ne": None}
                }).sort("timestamp", 1)
                
                mediciones_lista = list(mediciones)
                mediciones_totales += len(mediciones_lista)
                
                for medicion in mediciones_lista:
                    temp = medicion["temperature"]
                    datos_temperatura.append({
                        "fecha": medicion["timestamp"].strftime("%Y-%m-%d"),
                        "temp_max": temp,
                        "temp_min": temp,
                        "temperatura": temp,
                        "humedad": medicion.get("humidity", 0),
                        "ubicacion": ubicacion,
                        "sensor_id": sensor_id,
                        "fuente": "mongodb"
                    })
            
            print(f"📊 Total de mediciones encontradas: {mediciones_totales}")
            return datos_temperatura
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de temperatura para {ubicacion}: {e}")
            return []
    
    def obtener_datos_humedad_por_ubicacion(self, ubicacion, fecha_inicio, fecha_fin):
        """Obtener datos de humedad para una ubicación específica"""
        try:
            if not self.conectado:
                return []
            
            # Verificar si las fechas ya son objetos datetime o strings
            if isinstance(fecha_inicio, str):
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            else:
                fecha_inicio_dt = fecha_inicio
                
            if isinstance(fecha_fin, str):
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            else:
                fecha_fin_dt = fecha_fin
            
            # Buscar sensores en la ubicación con búsqueda estricta
            sensors_collection = self.db["sensors"]
            
            # Búsqueda flexible que encuentra ciudad en diferentes posiciones
            sensores_encontrados = list(sensors_collection.find({
                "$or": [
                    # Coincidencia exacta como string
                    {"location": ubicacion},
                    # Coincidencia exacta case insensitive
                    {"location": {"$regex": f"^{ubicacion}$", "$options": "i"}},
                    # La ciudad al INICIO seguida de espacio, coma o guión
                    {"location": {"$regex": f"^{ubicacion}[ ,-]", "$options": "i"}},
                    # La ciudad en medio: ", Ciudad -"
                    {"location": {"$regex": f", {ubicacion} -", "$options": "i"}},
                    # País al final (ubicacion es el país)
                    {"location": {"$regex": f".* - {ubicacion}$", "$options": "i"}}
                ]
            }))
            
            # También buscar en formato objeto
            sensores_obj = list(sensors_collection.find({
                "$or": [
                    {"location.city": {"$regex": f"^{ubicacion}$", "$options": "i"}},
                    {"location.country": {"$regex": f"^{ubicacion}$", "$options": "i"}}
                ]
            }))
            
            # Combinar resultados y obtener IDs únicos
            todos_sensores = sensores_encontrados + sensores_obj
            sensor_ids = list(set([sensor["sensor_id"] for sensor in todos_sensores if sensor.get("sensor_id")]))
            
            print(f"🔍 Buscando sensores para ubicación: '{ubicacion}', encontrados: {len(sensor_ids)}")
            
            if not sensor_ids:
                print(f"⚠️ No se encontraron sensores para la ubicación: {ubicacion}")
                return []
            
            # Buscar mediciones de humedad en el rango de fechas
            measurements_collection = self.db["measurements"]
            
            datos_humedad = []
            mediciones_totales = 0
            
            for sensor_id in sensor_ids:
                mediciones = measurements_collection.find({
                    "sensor_id": sensor_id,
                    "timestamp": {
                        "$gte": fecha_inicio_dt,
                        "$lte": fecha_fin_dt
                    },
                    "humidity": {"$exists": True, "$ne": None}
                }).sort("timestamp", 1)
                
                mediciones_lista = list(mediciones)
                mediciones_totales += len(mediciones_lista)
                
                for medicion in mediciones_lista:
                    datos_humedad.append({
                        "fecha": medicion["timestamp"].strftime("%Y-%m-%d"),
                        "humedad": medicion["humidity"],
                        "ubicacion": ubicacion,
                        "sensor_id": sensor_id,
                        "fuente": "mongodb"
                    })
            
            print(f"📊 Total de mediciones encontradas: {mediciones_totales}")
            return datos_humedad
            
        except Exception as e:
            print(f"Error obteniendo datos de humedad por ubicación: {e}")
            return []
    
    def obtener_ultima_medicion_sensor(self, sensor_id: str) -> Optional[Dict[str, Any]]:
        """Obtener la última medición de un sensor específico"""
        try:
            if not self.conectado:
                return None
            
            # Buscar la última medición del sensor
            measurement = self.db.measurements.find_one(
                {"sensor_id": sensor_id},
                sort=[("timestamp", -1)]
            )
            
            if measurement:
                # Convertir ObjectId a string si existe
                if "_id" in measurement:
                    measurement["_id"] = str(measurement["_id"])
                return measurement
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error obteniendo última medición del sensor {sensor_id}: {e}")
            return None
    
    def obtener_mediciones_sensor_rango(self, sensor_id: str, horas_atras: int = 24) -> List[Dict[str, Any]]:
        """Obtener mediciones de un sensor en las últimas N horas"""
        try:
            if not self.conectado:
                return []
            
            # Calcular fecha de inicio (N horas atrás)
            fecha_inicio = datetime.now() - timedelta(hours=horas_atras)
            
            # Buscar mediciones del sensor en el rango
            mediciones = list(self.db.measurements.find({
                "sensor_id": sensor_id,
                "timestamp": {"$gte": fecha_inicio}
            }).sort("timestamp", -1))
            
            # Convertir ObjectId a string
            for medicion in mediciones:
                if "_id" in medicion:
                    medicion["_id"] = str(medicion["_id"])
            
            return mediciones
            
        except Exception as e:
            print(f"❌ Error obteniendo mediciones del sensor {sensor_id}: {e}")
            return []
    
    def obtener_mediciones_sensor_por_fechas(self, sensor_id: str, fecha_inicio: str, fecha_fin: str) -> List[Dict[str, Any]]:
        """Obtener mediciones de un sensor en un rango de fechas específicas"""
        try:
            if not self.conectado:
                print(f"❌ MongoDB no conectado para sensor {sensor_id}")
                return []
            
            print(f"🔍 DEBUG: Buscando mediciones para sensor {sensor_id}")
            print(f"🔍 DEBUG: Fecha inicio: {fecha_inicio}")
            print(f"🔍 DEBUG: Fecha fin: {fecha_fin}")
            
            # Convertir fechas a datetime
            from datetime import datetime
            fecha_inicio_dt = datetime.fromisoformat(fecha_inicio)
            fecha_fin_dt = datetime.fromisoformat(fecha_fin)
            
            print(f"🔍 DEBUG: Fecha inicio DT: {fecha_inicio_dt}")
            print(f"🔍 DEBUG: Fecha fin DT: {fecha_fin_dt}")
            
            # Primero, verificar si hay mediciones para este sensor
            total_mediciones = self.db.measurements.count_documents({"sensor_id": sensor_id})
            print(f"🔍 DEBUG: Total mediciones para sensor {sensor_id}: {total_mediciones}")
            
            if total_mediciones > 0:
                # Obtener una muestra de mediciones para ver el formato
                muestra = list(self.db.measurements.find({"sensor_id": sensor_id}).limit(3))
                print(f"🔍 DEBUG: Muestra de mediciones:")
                for i, med in enumerate(muestra):
                    print(f"  {i+1}. Timestamp: {med.get('timestamp')} (tipo: {type(med.get('timestamp'))})")
            
            # Buscar mediciones del sensor en el rango de fechas
            query = {
                "sensor_id": sensor_id,
                "timestamp": {
                    "$gte": fecha_inicio_dt,
                    "$lte": fecha_fin_dt
                }
            }
            print(f"🔍 DEBUG: Query: {query}")
            
            mediciones = list(self.db.measurements.find(query).sort("timestamp", -1))
            print(f"🔍 DEBUG: Mediciones encontradas: {len(mediciones)}")
            
            # Convertir ObjectId a string
            for medicion in mediciones:
                if "_id" in medicion:
                    medicion["_id"] = str(medicion["_id"])
            
            return mediciones
            
        except Exception as e:
            print(f"❌ Error obteniendo mediciones del sensor {sensor_id} por fechas: {e}")
            return []
    
    def obtener_mediciones_sensor(self, sensor_id: str) -> List[Dict[str, Any]]:
        """Obtener todas las mediciones de un sensor específico"""
        try:
            if not self.conectado:
                return []
            
            # Buscar todas las mediciones del sensor
            mediciones = list(self.db.measurements.find(
                {"sensor_id": sensor_id}
            ).sort("timestamp", -1))
            
            # Convertir ObjectId a string si existe
            for medicion in mediciones:
                if "_id" in medicion:
                    medicion["_id"] = str(medicion["_id"])
            
            return mediciones
            
        except Exception as e:
            print(f"❌ Error obteniendo mediciones del sensor {sensor_id}: {e}")
            return []
    
        """Obtener datos de temperatura para una ubicación específica"""
        try:
            if not self.conectado:
                return []
            
            # Verificar si las fechas ya son objetos datetime o strings
            if isinstance(fecha_inicio, str):
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            else:
                fecha_inicio_dt = fecha_inicio
                
            if isinstance(fecha_fin, str):
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            else:
                fecha_fin_dt = fecha_fin
            
            # Buscar sensores en la ubicación
            sensors_collection = self.db["sensors"]
            sensores_ubicacion = sensors_collection.find({
                "location": {"$regex": ubicacion, "$options": "i"}
            })
            
            sensor_ids = [sensor["sensor_id"] for sensor in sensores_ubicacion]
            
            if not sensor_ids:
                return []
            
            # Buscar mediciones de temperatura en el rango de fechas
            measurements_collection = self.db["measurements"]
            
            datos_temperatura = []
            for sensor_id in sensor_ids:
                mediciones = measurements_collection.find({
                    "sensor_id": sensor_id,
                    "timestamp": {
                        "$gte": fecha_inicio_dt,
                        "$lte": fecha_fin_dt
                    },
                    "temperature": {"$exists": True, "$ne": None}
                }).sort("timestamp", 1)
                
                for medicion in mediciones:
                    datos_temperatura.append({
                        "fecha": medicion["timestamp"].strftime("%Y-%m-%d"),
                        "temperatura": medicion["temperature"],
                        "ubicacion": ubicacion,
                        "sensor_id": sensor_id,
                        "fuente": "mongodb"
                    })
            
            return datos_temperatura
            
        except Exception as e:
            print(f"Error obteniendo datos de temperatura por ubicación: {e}")
            return []