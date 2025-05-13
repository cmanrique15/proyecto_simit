import pymysql
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import seaborn as sns

# Configuración de conexión a la base de datos
conn = pymysql.connect(
    host="127.0.0.1", 
    port=3306,    
    user="root",  
    password="M@nr1que15+",  
    database="multas_simitbq"  
)
 
cursor = conn.cursor()
cursor.execute("SELECT * FROM multas;")
resultados = cursor.fetchall()

print(" Análisis de Multas de Tránsito – Barranquilla 2006-2020")
#punto 1 Exploracion de datos y Limpieza
 
columnas = ["VIGENCIA", "PLACA", "FECHA_MULTA", "VALOR_MULTA", "CIUDAD", "PAGADO_SI_NO"]
df = pd.DataFrame(resultados, columns=columnas)

print(df)
print(df.shape)
print(df.dtypes)

# Convertir la fecha a datetime
df['FECHA_MULTA'] = pd.to_datetime(df['FECHA_MULTA'], errors='coerce')

df =df.astype (
    {
    'VIGENCIA': 'int',
    'PLACA':'string',
    'VALOR_MULTA': 'int',
    'CIUDAD': 'string',
    'PAGADO_SI_NO': 'string',})
 
print(df.info())

# Crear columna TIPO_VEHICULO según el último carácter de la placa
print("\n Cantidad de registros por tipo de vehiculos")
df['TIPO_VEHICULO'] = df['PLACA'].str.strip().str.upper().apply(
    lambda x: 'MOTO' if x[-1].isalpha() else 'CARRO')
df['TIPO_VEHICULO'] = df['TIPO_VEHICULO'].astype('category')
print(df['TIPO_VEHICULO'].value_counts())

#Antes de eliminar
print(f"Duplicados antes de limpiar: {df.duplicated().sum()}")

print(df.duplicated())
# Eliminar duplicados
df = df.drop_duplicates()
 
# Después de eliminar
print(f"Duplicados después de limpiar: {df.duplicated().sum()}")
# Verificar valores nulos por columna
print(df.isnull().sum())
# Eliminar filas con valores nulos
df = df.dropna()
 
# Verificar que no haya valores nulos
print(df.isnull().sum())

#Guardar base de datos limpia con los cambios aplicados
df.to_csv('multas_simitbq_limpio.csv', index=False)

#Punto 2 - Analisis de datos Calculos

# 1 Cuantas multas se registraron por año?
multas_por_año=df.groupby(df["FECHA_MULTA"].dt.year).size()

print("\nMultas por año:", multas_por_año)

# 2 Cual es el monto total recaudado por año?
monto_por_año = df.groupby(df["FECHA_MULTA"].dt.year)["VALOR_MULTA"].sum()
print("\nMonto por año:",monto_por_año)

# 3 Cuál es la proporción de multas pagadas vs. no pagadas?
proporcion = df["PAGADO_SI_NO"].value_counts(normalize=True) *100
print(proporcion)

df['PAGADO_SI_NO'] = df['PAGADO_SI_NO'].str.strip().str.upper()
# 4 Calcular efectividad (%)
pagadas = (df['PAGADO_SI_NO'] == 'SI').sum()
total = len(df)
efectividad = (pagadas / total) * 100
print(f"\nEfectividad en el cobro de multas: {efectividad:.2f}%")

# 5 Tipo de vehículo que acumula más multa
multas_por_tipo = df.groupby("TIPO_VEHICULO", observed=True)["PLACA"].count()
tipo_con_mas_multas = multas_por_tipo.idxmax()
cantidad = multas_por_tipo.max()
print(f"\nEl tipo de vehículo que tiene más multas es {tipo_con_mas_multas}, con un total de {cantidad} multas.")

# 6 ¿Qué mes del año se imponen más multas?
df["mes"] = df["FECHA_MULTA"].dt.month
df["año"] = df["FECHA_MULTA"].dt.year
multas_por_año_mes = df.groupby(["año", "mes"]).size().reset_index(name="cantidad")
mes_max_por_año = multas_por_año_mes.loc[
multas_por_año_mes.groupby("año")["cantidad"].idxmax()]
print(f"\nEl mes por año que mas impone:{mes_max_por_año}")

# 7 Convertir valores a array de numpy
multas_array = multas_por_año.values
 
# Calcular diferencia entre años consecutivos
cambio_anual = np.diff(multas_array)
print("\nCambio de multas año tras año:")
print(cambio_anual)
 
# Calcular porcentaje de cambio año tras año
porcentaje_cambio = (cambio_anual / multas_array[:-1]) * 100
print("\nPorcentaje de cambio de multas año tras año:")
print(porcentaje_cambio)
 
# Contar cuántos años aumentaron
años_que_aumentaron = np.sum(cambio_anual > 0)
años_totales = len(cambio_anual)
print(f"\nAños que aumentó el número de multas: {años_que_aumentaron} de {años_totales}")

# 8 calculo de porcentaje de vehiculos reincidentes
# Contar cuántas veces aparece cada placa
vehiculos_reincidentes = df["PLACA"].value_counts()
# Total de vehículos únicos (placas distintas)
total_vehiculos_unicos = len(vehiculos_reincidentes)
# Contar cuántos vehículos tienen más de una multa (reincidentes)
cantidad_reincidentes = (vehiculos_reincidentes > 1).sum()
# Calcular el porcentaje de reincidencia
porcentaje_reincidencia = (cantidad_reincidentes / total_vehiculos_unicos) * 100
# Mostrar los resultados
print(f"\nTotal de vehículos únicos: {total_vehiculos_unicos}")
print(f"\nVehículos reincidentes (más de una multa): {cantidad_reincidentes}")
print(f"\nPorcentaje de vehículos reincidentes: {porcentaje_reincidencia:.2f}%")

#otros calculos

promedio_multas = np.mean(df["VALOR_MULTA"])
print(f"\nPromedio del valor de las multas: {promedio_multas:.2f}")

max_multa = np.max(df["VALOR_MULTA"])
min_multa = np.min(df["VALOR_MULTA"])
print(f"\nMulta más alta: {max_multa}")
print(f"\nMulta más baja: {min_multa}")

vehiculos_reincidentes = df["PLACA"].value_counts()
cantidad_reincidentes = (vehiculos_reincidentes > 1).sum()
print(f"\nVehículos reincidentes (más de una multa): {cantidad_reincidentes}")

#Paso 3 Analisis de datos Graficas

#Grafico 1
plt.figure(figsize=(8, 5))  # Crear una nueva figura y definir su tamaño
multas_por_año.plot(kind="bar", color="skyblue")
plt.title("Multas Registradas por Año")
plt.xlabel("Año")
plt.ylabel("Cantidad de Multas")
plt.tight_layout()
plt.show()

#Grafico 2
plt.figure()
monto_por_año.plot(kind="bar", color="green")
plt.title("Monto Total Recaudado por Año")
plt.xlabel("Año")
plt.ylabel("Monto")
plt.tight_layout()
plt.show()

#Grafico 3
sns.countplot(data=df, x="PAGADO_SI_NO", hue="PAGADO_SI_NO", palette="Set2")
plt.title("Multas Pagadas vs. No Pagadas")
plt.xlabel("¿Pagó la multa?")
plt.ylabel("Cantidad")
plt.show()

#Grafico 4
plt.figure(figsize=(6, 2))
plt.barh(["Efectividad de Cobro"], [efectividad], color="purple")
plt.xlim(0, 100)
plt.title("Efectividad en el Cobro de Multas (%)")
plt.text(efectividad + 1, 0, f"{efectividad:.2f}%", va='center')  # Etiqueta del valor 
plt.tight_layout()
plt.show()

#Grafico 5
sns.countplot(data=df, x="TIPO_VEHICULO", hue="TIPO_VEHICULO", palette="Set2", order=df["TIPO_VEHICULO"].value_counts().index)
plt.title("Multas por Tipo de Vehículo")
plt.xticks(rotation=45)
plt.ylabel("Cantidad")
plt.show()

#Grafico 6
plt.bar(mes_max_por_año["año"], mes_max_por_año["mes"])
plt.title("Mes con más multas por año")
plt.xlabel("Año")
plt.ylabel("Mes (número)")
plt.show()

#grafico 7
labels = ['Reincidentes', 'No Reincidentes']
sizes = [cantidad_reincidentes, total_vehiculos_unicos - cantidad_reincidentes]
colors = ['#ff9999','#66b3ff']

plt.figure(figsize=(6, 6))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors)
plt.title("Porcentaje de Vehículos Reincidentes")
plt.axis('equal')  # Mantiene el círculo proporcionado
plt.show()