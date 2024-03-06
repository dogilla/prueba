import boto3

# Definimos las rutas por separado por si hay que cambiarlas
bucket_name = 'bucket'
input = 'carpeta1/nombres.csv'
output = 'carpeta2/nombres_version2.csv'

# Crear una instancia de cliente de S3
s3 = boto3.client('s3')

# Descargar el archivo desde S3
response = s3.get_object(Bucket=bucket_name, Key=input)
data = response['Body'].read()

# Subir el archivo al nuevo destino
s3.put_object(Bucket=bucket_name, Key=output, Body=data)

print(f"El archivo se ha copiado exitosamente a {output}.")
